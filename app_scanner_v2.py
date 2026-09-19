import os
import json
import time
import hashlib
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
import streamlit as st
import yfinance as yf
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.trading.requests import GetAssetsRequest

st.set_page_config(page_title="Scanner Pre Market", layout="wide")

# ==========================================
# 🙈 OCULTAR BARRA SUPERIOR DE STREAMLIT (Share, GitHub, editar, menú, badges)
# ==========================================
st.markdown("""
<style>
    [data-testid="stToolbar"],
    [data-testid="stAppDeployButton"],
    [data-testid="stDecoration"],
    [data-testid="stStatusWidget"],
    [data-testid="stMainMenu"],
    .stAppDeployButton,
    #MainMenu,
    footer,
    [class*="viewerBadge"],
    [class*="_profileContainer"] {
        display: none !important;
        visibility: hidden !important;
    }
</style>
""", unsafe_allow_html=True)

ET = ZoneInfo("America/New_York")

# ==========================================
# ⚙️ PARÁMETROS DEL MOTOR
# ==========================================
INTERVALO_ESCANEO_SEGUNDOS = 10        # cada cuánto el motor recorre el mercado (una sola vez para todos los usuarios)
TAMANO_LOTE_SNAPSHOT = 500             # tickers por petición de snapshot
WORKERS_SNAPSHOT = 4                   # peticiones de snapshot en paralelo
PAUSA_MIN_ENTRE_PETICIONES = 0.33      # ~180 peticiones/min a Alpaca (límite: 200/min)

# Radar base: rango AMPLIO que el motor enriquece. Cada usuario filtra su vista dentro de este rango.
BASE_PRECIO_MIN = 1.0
BASE_PRECIO_MAX = 50.0
BASE_GAP_MIN = 3.0
BASE_GAP_MAX = 1000.0
BASE_FLOTACION_MAX = 50_000_000
MAX_ENRIQUECER = 120                   # máx. de tickers a los que se les calcula float / EMA / noticia por ciclo

MAX_FUNDAMENTALES_POR_CICLO = 40       # llamadas nuevas a yfinance .info por ciclo (el resto en el siguiente)
WORKERS_FUNDAMENTALES = 8
VIGENCIA_FUNDAMENTALES = 7 * 86400     # el float se considera válido 7 días
REINTENTO_FUNDAMENTALES = 600          # si falló, reintentar a los 10 min

TTL_TECNICO_SEGUNDOS = 30              # no recalcular EMA/MACD de un ticker más seguido que esto
VENTANA_CRUCE_EMA_MINUTOS = 15
MARGEN_PROXIMIDAD_EMA = 0.05
MINUTOS_NOTICIA_RECIENTE = 60

NOMBRE_ARCHIVO_HTML = "radar_premarket.html"
RUTA_CACHE_FUNDAMENTALES = os.path.join(os.getcwd(), "cache_fundamentales.json")
RUTA_CONFIG = os.path.join(os.getcwd(), "config_filtros.json")

VALORES_POR_DEFECTO = {
    "precio_min": 2.0,
    "precio_max": 20.0,
    "gap_min": 7.0,
    "gap_max": 500.0,
    "flotacion_max": 10_000_000,
    "vol_rel_min": 1.3,
    "intervalo_refresco": 5,
}


def cargar_config():
    """Filtros por defecto (los del dueño). Solo lectura: cada usuario ajusta su propia vista."""
    config = VALORES_POR_DEFECTO.copy()
    try:
        with open(RUTA_CONFIG, "r") as f:
            config.update(json.load(f))
    except Exception:
        pass
    return config


# ==========================================
# 🔒 CONTROL DE ACCESO POR TOKEN
# ==========================================
def obtener_tokens():
    for clave in ("tokens_autorizados", "TOKENS_AUTORIZADOS"):
        try:
            if clave in st.secrets:
                return dict(st.secrets[clave])
        except Exception:
            pass
    return {}


def verificar_token(token_usuario):
    tokens = obtener_tokens()
    if token_usuario in tokens:
        try:
            fecha_exp = datetime.strptime(str(tokens[token_usuario]), "%Y-%m-%d").date()
        except ValueError:
            return False, "FORMATO"
        if datetime.now().date() <= fecha_exp:
            return True, str(tokens[token_usuario])
        return False, "EXPIRADO"
    return False, "INVALIDO"


def pantalla_autenticacion():
    st.markdown("<style>.stApp { background-color: #0a0e1a; }</style>", unsafe_allow_html=True)
    st.markdown("""
    <div style="max-width: 460px; margin: 60px auto; background: #11151f;
                border: 1px solid #2a3348; border-radius: 12px; padding: 40px; text-align: center;">
        <h2 style="color:#FFD700; font-family:sans-serif; margin-bottom:5px;">SISTEMA PROTEGIDO</h2>
        <p style="color:#8b93a7; font-size:11px; letter-spacing:2px; margin-bottom:20px;">SCANNER PRE MARKET</p>
    </div>
    """, unsafe_allow_html=True)

    with st.form("modulo_seguridad"):
        token_ingresado = st.text_input("Introduce tu Token de Acceso", type="password")
        boton_entrar = st.form_submit_button("Validar licencia")

    if boton_entrar:
        token_limpio = token_ingresado.strip()
        es_valido, estado = verificar_token(token_limpio)
        if es_valido:
            st.session_state["token_verificado"] = token_limpio
            st.session_state["fecha_vencimiento"] = estado
            st.rerun()
        elif estado == "EXPIRADO":
            st.error("🔒 Token expirado. Renueva tu suscripción.")
        elif estado == "FORMATO":
            st.error("Error de configuración del token (la fecha debe ser AAAA-MM-DD).")
        else:
            st.error("❌ Token no válido. Acceso denegado.")
    st.stop()


if "token_verificado" not in st.session_state:
    pantalla_autenticacion()

TOKEN_ACTIVO = st.session_state["token_verificado"]
FECHA_VENCIMIENTO_LICENCIA = st.session_state["fecha_vencimiento"]
ADMIN_TOKEN = st.secrets.get("ADMIN_TOKEN", None)
ES_ADMIN = bool(ADMIN_TOKEN) and TOKEN_ACTIVO == ADMIN_TOKEN


# ==========================================
# 🧮 LÓGICA PURA (indicadores y filtros)
# ==========================================
def formatear_numero_grande(numero):
    try:
        numero = float(numero)
    except (TypeError, ValueError):
        return "N/A"
    if numero >= 1_000_000:
        return f"{numero / 1_000_000:.1f}M"
    elif numero >= 1_000:
        return f"{numero / 1_000:.0f}K"
    return f"{numero:.0f}"


def evaluar_tecnico(cierres):
    """Devuelve (cruzando_ema20, macd_positivo) a partir de una serie de cierres de 1 minuto."""
    if cierres is None or len(cierres) < 40:
        return False, False

    ema20 = cierres.ewm(span=20, adjust=False).mean()
    macd_line = cierres.ewm(span=12, adjust=False).mean() - cierres.ewm(span=26, adjust=False).mean()

    precio_act = float(cierres.iloc[-1])
    ema_act = float(ema20.iloc[-1])
    if pd.isna(ema_act) or ema_act <= 0:
        return False, False

    cerca_de_ema = precio_act > ema_act and (precio_act - ema_act) / ema_act <= MARGEN_PROXIMIDAD_EMA

    cruzo_recientemente = False
    for i in range(-VENTANA_CRUCE_EMA_MINUTOS, -1):
        if cierres.iloc[i - 1] <= ema20.iloc[i - 1] and cierres.iloc[i] > ema20.iloc[i]:
            cruzo_recientemente = True
            break

    macd_actual = macd_line.iloc[-1]
    macd_positivo = bool(not pd.isna(macd_actual) and macd_actual > 0)
    return (cerca_de_ema and cruzo_recientemente), macd_positivo


def descargar_cierres(tickers):
    """Velas de 1 minuto INCLUYENDO pre-market (prepost=True), en descargas por lotes."""
    salida = {}
    for i in range(0, len(tickers), 60):
        lote = tickers[i:i + 60]
        try:
            datos = yf.download(
                lote, period="2d", interval="1m", prepost=True,
                group_by="ticker", auto_adjust=False, progress=False, threads=True,
            )
        except Exception as e:
            print(f"⚠️ Error descargando velas: {e}")
            continue
        if datos is None or len(datos) == 0:
            continue
        for t in lote:
            try:
                if isinstance(datos.columns, pd.MultiIndex):
                    if t not in datos.columns.get_level_values(0):
                        continue
                    serie = datos[t]["Close"]
                else:
                    serie = datos["Close"]
                serie = serie.dropna()
                if len(serie) >= 40:
                    salida[t] = serie
            except Exception:
                continue
    return salida


def filtrar_resultados(filas, p):
    resultado = []
    for c in filas:
        if not (p["precio_min"] <= c["precio"] <= p["precio_max"]):
            continue
        if not (p["gap_min"] <= c["cambio_pct"] <= p["gap_max"]):
            continue
        if c["float_shares"] is None or c["float_shares"] >= p["flotacion_max"]:
            continue
        if c["volumen_relativo"] < p["vol_rel_min"]:
            continue
        if p["exigir_cruce"] and not c["cruzando_ema20"]:
            continue
        if p["exigir_macd"] and not c["macd_positivo"]:
            continue
        resultado.append(c)

    claves = {
        "Actualizado": lambda x: x["actualizado"],
        "Cambio %": lambda x: x["cambio_pct"],
        "Vol. relativo": lambda x: x["volumen_relativo"],
    }
    resultado.sort(key=claves.get(p.get("orden", "Actualizado"), claves["Actualizado"]), reverse=True)
    return resultado[: int(p["top_n"])]


# ==========================================
# ⚡️ MOTOR COMPARTIDO (un solo hilo para TODOS los usuarios)
# ==========================================
class ServicioScanner:
    def __init__(self, api_key, secret_key, tg_token, tg_chat, filtros_dueno):
        self.api_key = api_key
        self.secret_key = secret_key
        self.tg_token = tg_token
        self.tg_chat = tg_chat
        self.filtros_dueno = filtros_dueno

        self.trading = TradingClient(api_key, secret_key)
        self.data = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)

        self.encendido = True
        self.resultados = []
        self.ultima_actualizacion = None
        self.duracion_ciclo = None
        self.ultimo_error = None
        self.n_radar_base = 0
        self.universo = []
        self.universo_ts = 0.0

        self.tg_msg_id = None
        self.tg_ultimo_hash = None

        self.cache_tecnico = {}
        self.cache_fund = self._leer_cache_fundamentales()

        self._lock_ritmo = threading.Lock()
        self._ultima_peticion = 0.0

        self._hilo = threading.Thread(target=self._bucle, daemon=True)
        self._hilo.start()

    # ---------- utilidades ----------
    def _esperar_turno(self):
        with self._lock_ritmo:
            espera = self._ultima_peticion + PAUSA_MIN_ENTRE_PETICIONES - time.monotonic()
            if espera > 0:
                time.sleep(espera)
            self._ultima_peticion = time.monotonic()

    def _leer_cache_fundamentales(self):
        try:
            with open(RUTA_CACHE_FUNDAMENTALES, "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def _guardar_cache_fundamentales(self):
        try:
            with open(RUTA_CACHE_FUNDAMENTALES, "w") as f:
                json.dump(self.cache_fund, f)
        except Exception as e:
            print(f"⚠️ No se pudo guardar la caché de fundamentales: {e}")

    # ---------- universo ----------
    def _cargar_universo(self):
        try:
            solicitud = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
            activos = self.trading.get_all_assets(solicitud)
            self.universo = [
                a.symbol for a in activos
                if a.tradable
                and a.exchange in ("NASDAQ", "NYSE", "AMEX", "ARCA")
                and "." not in a.symbol
                and "-" not in a.symbol
            ]
            self.universo_ts = time.time()
            print(f"🌐 Universo cargado: {len(self.universo)} tickers.")
        except Exception as e:
            self.ultimo_error = f"Universo: {e}"
            print(f"⚠️ Error cargando universo: {e}")

    # ---------- snapshots en paralelo ----------
    def _descargar_snapshots(self):
        lotes = [self.universo[i:i + TAMANO_LOTE_SNAPSHOT] for i in range(0, len(self.universo), TAMANO_LOTE_SNAPSHOT)]

        def pedir(lote):
            self._esperar_turno()
            try:
                return self.data.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=lote)) or {}
            except Exception as e:
                self.ultimo_error = f"Snapshot: {e}"
                return {}

        snapshots = {}
        with ThreadPoolExecutor(max_workers=WORKERS_SNAPSHOT) as ex:
            for parcial in ex.map(pedir, lotes):
                snapshots.update(parcial)
        return snapshots

    # ---------- float y volumen promedio (con caché en disco) ----------
    def _asegurar_fundamentales(self, tickers):
        ahora = time.time()
        faltan = []
        for t in tickers:
            e = self.cache_fund.get(t)
            if e is None:
                faltan.append(t)
            elif e.get("float") is None and ahora - e["ts"] > REINTENTO_FUNDAMENTALES:
                faltan.append(t)
            elif ahora - e["ts"] > VIGENCIA_FUNDAMENTALES:
                faltan.append(t)
        faltan = faltan[:MAX_FUNDAMENTALES_POR_CICLO]
        if not faltan:
            return

        def pedir(t):
            try:
                info = yf.Ticker(t).info
                return t, {
                    "float": info.get("floatShares"),
                    "avgvol": info.get("averageVolume") or info.get("averageDailyVolume10Day"),
                    "ts": time.time(),
                }
            except Exception:
                return t, {"float": None, "avgvol": None, "ts": time.time()}

        with ThreadPoolExecutor(max_workers=WORKERS_FUNDAMENTALES) as ex:
            for t, entrada in ex.map(pedir, faltan):
                self.cache_fund[t] = entrada
        self._guardar_cache_fundamentales()

    # ---------- EMA20 / MACD ----------
    def _asegurar_tecnico(self, tickers):
        ahora = time.time()
        pendientes = [
            t for t in tickers
            if t not in self.cache_tecnico or ahora - self.cache_tecnico[t][0] > TTL_TECNICO_SEGUNDOS
        ]
        if not pendientes:
            return
        series = descargar_cierres(pendientes)
        for t in pendientes:
            cruzando, macd_pos = evaluar_tecnico(series.get(t))
            self.cache_tecnico[t] = (ahora, cruzando, macd_pos)

    # ---------- noticias (una sola llamada para todos) ----------
    def _noticias_recientes(self, tickers):
        if not tickers:
            return set()
        try:
            self._esperar_turno()
            desde = (datetime.now(timezone.utc) - timedelta(minutes=MINUTOS_NOTICIA_RECIENTE)).strftime("%Y-%m-%dT%H:%M:%SZ")
            respuesta = requests.get(
                "https://data.alpaca.markets/v1beta1/news",
                headers={"APCA-API-KEY-ID": self.api_key, "APCA-API-SECRET-KEY": self.secret_key},
                params={"symbols": ",".join(tickers), "start": desde, "limit": 50},
                timeout=8,
            )
            if respuesta.status_code == 200:
                con_noticia = set()
                for n in respuesta.json().get("news", []):
                    con_noticia.update(n.get("symbols", []))
                return con_noticia & set(tickers)
        except Exception:
            pass
        return set()

    # ---------- Telegram ----------
    def _enviar_telegram(self, texto_tabla):
        if not self.tg_token or not self.tg_chat:
            return
        hash_actual = hashlib.md5(texto_tabla.encode("utf-8")).hexdigest()
        if hash_actual == self.tg_ultimo_hash:
            return
        try:
            payload = {
                "chat_id": self.tg_chat,
                "text": f"⚡️ <b>SCANNER PRE MARKET</b>\n<pre>{texto_tabla}</pre>",
                "parse_mode": "HTML",
            }
            cabeceras = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            if self.tg_msg_id is None:
                url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
                r = requests.post(url, json=payload, headers=cabeceras, timeout=15)
                if r.status_code == 200:
                    self.tg_msg_id = r.json()["result"]["message_id"]
                    self.tg_ultimo_hash = hash_actual
                else:
                    print(f"❌ Telegram rechazó el mensaje: {r.status_code} - {r.text}")
            else:
                url = f"https://api.telegram.org/bot{self.tg_token}/editMessageText"
                payload["message_id"] = self.tg_msg_id
                r = requests.post(url, json=payload, headers=cabeceras, timeout=15)
                if r.status_code == 200 or "message is not modified" in r.text:
                    self.tg_ultimo_hash = hash_actual
                elif "not found" in r.text:
                    self.tg_msg_id = None  # el mensaje fue borrado: se envía uno nuevo en el próximo ciclo
                else:
                    print(f"❌ Error al editar en Telegram: {r.status_code} - {r.text}")
        except Exception as e:
            print(f"⚠️ Error de red con Telegram: {e}")

    def _escribir_html(self, texto_tabla):
        contenido = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>SCANNER PRE MARKET</title>
<meta http-equiv="refresh" content="30">
<style>body {{ background:#121212; color:#00ffcc; font-family:'Courier New',monospace; padding:20px; }}
pre {{ background:#1e1e1e; padding:25px; border-radius:8px; border:1px solid #333; color:#fff; }}</style>
</head><body><h2>SCANNER PRE MARKET</h2><pre>{texto_tabla}</pre></body></html>"""
        try:
            with open(os.path.join(os.getcwd(), NOMBRE_ARCHIVO_HTML), "w", encoding="utf-8") as f:
                f.write(contenido)
        except Exception:
            pass

    # ---------- ciclo principal ----------
    def _ciclo(self):
        inicio = time.monotonic()
        self.ultimo_error = None

        if not self.universo or time.time() - self.universo_ts > 6 * 3600:
            self._cargar_universo()
        if not self.universo:
            return

        snapshots = self._descargar_snapshots()

        base = []
        for ticker, snap in snapshots.items():
            if not snap or not snap.latest_trade or not snap.daily_bar or not snap.previous_daily_bar:
                continue
            precio = snap.latest_trade.price
            cierre_prev = snap.previous_daily_bar.close
            if not cierre_prev or cierre_prev <= 0:
                continue
            if not (BASE_PRECIO_MIN <= precio <= BASE_PRECIO_MAX):
                continue
            cambio = ((precio - cierre_prev) / cierre_prev) * 100
            if not (BASE_GAP_MIN <= cambio <= BASE_GAP_MAX):
                continue
            base.append({
                "ticker": ticker,
                "precio": precio,
                "cambio_pct": cambio,
                "volumen_dia": snap.daily_bar.volume or 0,
                "actualizado": snap.latest_trade.timestamp,
            })

        self.n_radar_base = len(base)
        base.sort(key=lambda c: c["volumen_dia"], reverse=True)
        base = base[:MAX_ENRIQUECER]

        self._asegurar_fundamentales([c["ticker"] for c in base])

        enriquecidos = []
        for c in base:
            entrada = self.cache_fund.get(c["ticker"])
            if not entrada or entrada.get("float") is None:
                continue
            avgvol = entrada.get("avgvol")
            if not avgvol or avgvol <= 0:
                continue
            if entrada["float"] >= BASE_FLOTACION_MAX:
                continue
            c["float_shares"] = entrada["float"]
            c["volumen_relativo"] = c["volumen_dia"] / avgvol
            enriquecidos.append(c)

        tickers_enr = [c["ticker"] for c in enriquecidos]
        self._asegurar_tecnico(tickers_enr)
        con_noticia = self._noticias_recientes(tickers_enr)

        for c in enriquecidos:
            _, cruzando, macd_pos = self.cache_tecnico.get(c["ticker"], (0, False, False))
            c["cruzando_ema20"] = cruzando
            c["macd_positivo"] = macd_pos
            c["tiene_noticia"] = c["ticker"] in con_noticia

        self.resultados = enriquecidos
        self.ultima_actualizacion = datetime.now(ET)
        self.duracion_ciclo = time.monotonic() - inicio

        # Telegram / HTML con los filtros del dueño (los de config_filtros.json)
        p = dict(self.filtros_dueno)
        p.update({"exigir_cruce": True, "exigir_macd": True, "top_n": 20, "orden": "Actualizado"})
        top = filtrar_resultados(enriquecidos, p)
        if top:
            tabla = f"{'TICK':<5}|{'PRE':>5}|{'CHG%':>4}|{'VOL':>5}|{'FLT':>5}\n" + "-" * 28 + "\n"
            for c in top:
                nombre = f"🔥{c['ticker']}" if c["tiene_noticia"] else c["ticker"]
                tabla += (f"{nombre:<5}|{c['precio']:>5.2f}|{c['cambio_pct']:>3.0f}%|"
                          f"{formatear_numero_grande(c['volumen_dia']):>5}|{formatear_numero_grande(c['float_shares']):>5}\n")
            self._enviar_telegram(tabla)
            self._escribir_html(tabla)

    def _bucle(self):
        while True:
            inicio = time.monotonic()
            if self.encendido:
                try:
                    self._ciclo()
                except Exception as e:
                    self.ultimo_error = f"Ciclo: {e}"
                    print(f"⚠️ Error en escaneo: {e}")
            time.sleep(max(1.0, INTERVALO_ESCANEO_SEGUNDOS - (time.monotonic() - inicio)))


@st.cache_resource
def obtener_servicio(api_key, secret_key, tg_token, tg_chat):
    print("⚙️ Iniciando el motor del scanner (una sola vez para todos los usuarios)...")
    return ServicioScanner(api_key, secret_key, tg_token, tg_chat, cargar_config())


servicio = obtener_servicio(
    st.secrets["ALPACA_API_KEY"],
    st.secrets["ALPACA_SECRET_KEY"],
    st.secrets.get("TELEGRAM_BOT_TOKEN", None),
    st.secrets.get("TELEGRAM_CHAT_ID", "-1004440734539"),
)

# ==========================================
# 🎨 ESTILO OSCURO
# ==========================================
st.markdown("""
<style>
    .stApp { background-color: #0a0e1a; color: #e6e6e6; }
    [data-testid="stHeader"], [data-testid="stSidebar"] { background-color: #0a0e1a; }
    .block-container { padding-top: 1rem; }
    .finviz-topbar { background: linear-gradient(135deg, #0d1420 0%, #131b2c 100%); padding: 18px; border-radius: 10px;
        margin-bottom: 12px; border: 1px solid #2a3348; border-bottom: 3px solid #ffd700; text-align: center; }
    .finviz-topbar h1 { color: #c9a227; font-size: 24px; margin: 0; font-family: sans-serif; font-weight: 700; }
    label, [data-testid="stWidgetLabel"] p { color: #FFD700 !important; font-weight: 700 !important;
        text-transform: uppercase; font-size: 11px !important; }
    div[data-testid="stNumberInput"] input { background-color: #1a1e27; color: #ffffff; border: 1px solid #2a3348 !important; }
</style>
""", unsafe_allow_html=True)

st.markdown(
    '<div class="finviz-topbar"><h1>🐂 SCANNER PRE MARKET LIVE 🐻</h1>'
    '<div style="color:#8b93a7; font-size:11px; letter-spacing:2px;">RADAR EN TIEMPO REAL</div></div>',
    unsafe_allow_html=True,
)
st.markdown(
    f'<div style="text-align:right; color:#8b93a7; font-size:11px; margin-bottom:8px;">'
    f'🔐 LICENCIA ACTIVA · vence {FECHA_VENCIMIENTO_LICENCIA}</div>',
    unsafe_allow_html=True,
)

# ==========================================
# 📊 FILTROS (cada usuario ajusta su propia vista)
# ==========================================
cfg = cargar_config()

with st.container(border=True):
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    with c1:
        PRECIO_MIN = st.number_input("Precio mín. ($)", value=float(cfg["precio_min"]), step=0.5, key="f_pmin")
    with c2:
        PRECIO_MAX = st.number_input("Precio máx. ($)", value=float(cfg["precio_max"]), step=0.5, key="f_pmax")
    with c3:
        GAP_MIN = st.number_input("Gap mín. (%)", value=float(cfg["gap_min"]), step=1.0, key="f_gmin")
    with c4:
        GAP_MAX = st.number_input("Gap máx. (%)", value=float(cfg["gap_max"]), step=10.0, key="f_gmax")
    with c5:
        FLOT_MAX = st.number_input("Flotación máx.", value=int(cfg["flotacion_max"]), step=1_000_000, key="f_flt")
    with c6:
        VOLREL_MIN = st.number_input("Vol. relativo mín.", value=float(cfg["vol_rel_min"]), step=0.1, key="f_vr")
    with c7:
        REFRESCO = st.number_input("Refresco pantalla (seg)", value=int(cfg["intervalo_refresco"]), min_value=1, step=1, key="f_ref")

    d1, d2, d3, d4, d5 = st.columns(5)
    with d1:
        EXIGIR_CRUCE = st.toggle("Exigir cruce EMA20", value=True, key="f_cruce")
    with d2:
        EXIGIR_MACD = st.toggle("Exigir MACD positivo", value=True, key="f_macd")
    with d3:
        ORDEN = st.selectbox("Ordenar por", ["Actualizado", "Cambio %", "Vol. relativo"], key="f_orden")
    with d4:
        TOP_N = st.number_input("Top N", value=20, min_value=1, max_value=100, key="f_top")
    with d5:
        AUTO_ON = st.toggle("Auto-refresh", value=True, key="f_auto")

params = {
    "precio_min": PRECIO_MIN, "precio_max": PRECIO_MAX,
    "gap_min": GAP_MIN, "gap_max": GAP_MAX,
    "flotacion_max": FLOT_MAX, "vol_rel_min": VOLREL_MIN,
    "exigir_cruce": EXIGIR_CRUCE, "exigir_macd": EXIGIR_MACD,
    "orden": ORDEN, "top_n": TOP_N,
}

# ==========================================
# 🟢🔴 ESTADO DEL MOTOR (solo el administrador puede encender/apagar)
# ==========================================
col_estado, col_bot = st.columns([3, 1])
with col_estado:
    st.markdown("### 🟢 Scanner ENCENDIDO" if servicio.encendido else "### 🔴 Scanner APAGADO")
with col_bot:
    if ES_ADMIN:
        servicio.encendido = st.toggle("Encender / Apagar", value=servicio.encendido, key="toggle_motor")

# ==========================================
# 🖥️ TABLA DE RESULTADOS (se refresca sola sin recargar la página)
# ==========================================
def color_cambio(val):
    try:
        v = float(val)
    except (TypeError, ValueError):
        return ""
    return f"color: {'#2ecc71' if v >= 0 else '#e74c3c'}; font-weight: 700"


@st.fragment(run_every=(f"{int(REFRESCO)}s" if AUTO_ON else None))
def panel_resultados():
    filas = filtrar_resultados(list(servicio.resultados), params)

    if servicio.ultima_actualizacion:
        detalle = (f"Última actualización: {servicio.ultima_actualizacion.strftime('%H:%M:%S')} ET"
                   f" · ciclo {servicio.duracion_ciclo:.1f}s"
                   f" · {len(servicio.universo)} tickers vigilados"
                   f" · {servicio.n_radar_base} en el radar base"
                   f" (precio ${BASE_PRECIO_MIN:.0f}-${BASE_PRECIO_MAX:.0f}, gap ≥ {BASE_GAP_MIN:.0f}%, float ≤ {formatear_numero_grande(BASE_FLOTACION_MAX)})")
        st.caption(detalle)
    else:
        st.caption("Esperando el primer escaneo (la primera vez puede tardar un minuto)...")

    if servicio.ultimo_error:
        st.warning(f"Aviso del motor: {servicio.ultimo_error}")

    st.markdown(f"**{len(filas)} resultados** · el motor escanea cada {INTERVALO_ESCANEO_SEGUNDOS}s")

    if not filas:
        st.info("Sin candidatos que cumplan los filtros en este momento.")
        return

    df = pd.DataFrame([
        {
            "No.": i + 1,
            "Ticker": c["ticker"],
            "Precio": round(c["precio"], 2),
            "Cambio %": round(c["cambio_pct"], 1),
            "Volumen": formatear_numero_grande(c["volumen_dia"]),
            "Flotación": formatear_numero_grande(c["float_shares"]),
            "Vol. Relativo": round(c["volumen_relativo"], 2),
            "EMA20": "✅" if c["cruzando_ema20"] else "",
            "MACD+": "✅" if c["macd_positivo"] else "",
            "Noticia": "🔥" if c["tiene_noticia"] else "",
            "Actualizado (ET)": c["actualizado"].astimezone(ET).strftime("%H:%M:%S") if hasattr(c["actualizado"], "astimezone") else str(c["actualizado"]),
        }
        for i, c in enumerate(filas)
    ])

    styled = (
        df.style
        .map(color_cambio, subset=["Cambio %"])
        .set_properties(**{"background-color": "#12151c", "color": "#e6e6e6", "border-color": "#2a2e39"})
        .set_table_styles([{"selector": "th", "props": [("background-color", "#0e1117"), ("color", "#00ffcc"), ("font-weight", "bold")]}])
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


panel_resultados()
