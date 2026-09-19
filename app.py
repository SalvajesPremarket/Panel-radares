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
    "gap_min": 5.0,
    "gap_max": 500.0,
    "flotacion_max": 15_000_000,
    "vol_rel_min": 1.5,
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
        p.update({"exigir_cruce": True, "exigir_macd": True, "top_n": 10, "orden": "Actualizado"})
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
    .finviz-topbar { position: relative; background: linear-gradient(135deg, #0d1420 0%, #131b2c 100%); padding: 18px 100px;
        border-radius: 10px; margin-bottom: 12px; border: 1px solid #2a3348; border-bottom: 3px solid #ffd700; text-align: center; }
    .finviz-topbar h1 { color: #c9a227; font-size: 24px; margin: 0; font-family: sans-serif; font-weight: 700; }
    .topbar-figura { position: absolute; top: 50%; transform: translateY(-50%); width: 84px; height: 84px;
        border-radius: 50%; background: #0a0e1a; box-shadow: 0 0 0 2px #c9a227, 0 4px 12px rgba(0,0,0,0.5);
        display: flex; align-items: center; justify-content: center; overflow: hidden; }
    .topbar-figura img { width: 100%; height: 100%; object-fit: cover; object-position: center; }
    .topbar-figura.toro { left: 20px; }
    .topbar-figura.oso { right: 20px; }
    label, [data-testid="stWidgetLabel"] p { color: #FFD700 !important; font-weight: 700 !important;
        text-transform: uppercase; font-size: 11px !important; }
    div[data-testid="stNumberInput"] input { background-color: #1a1e27; color: #ffffff; border: 1px solid #2a3348 !important; }
</style>
""", unsafe_allow_html=True)

IMG_TORO_B64 = "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"
IMG_OSO_B64 = "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"

st.markdown(
    f'<div class="finviz-topbar">'
    f'<div class="topbar-figura toro"><img src="data:image/png;base64,{IMG_TORO_B64}"></div>'
    f'<h1>SCANNER PRE MARKET LIVE</h1>'
    f'<div style="color:#8b93a7; font-size:11px; letter-spacing:2px;">RADAR EN TIEMPO REAL</div>'
    f'<div class="topbar-figura oso"><img src="data:image/png;base64,{IMG_OSO_B64}"></div>'
    f'</div>',
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
        TOP_N = st.number_input("Top N", value=10, min_value=1, max_value=100, key="f_top")
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
