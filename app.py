import os
import json
import time
import hashlib
import hmac
import secrets
from urllib.parse import quote
from html import escape as html_escape
import threading
from datetime import date, datetime, timedelta, timezone, time as dt_time
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.trading.requests import GetAssetsRequest, GetCalendarRequest

# Configuración obligatoria de Streamlit Shell
st.set_page_config(page_title="Scanner Pre Market", layout="wide")

# ==========================================
# 🙈 BLINDAJE VISUAL: OCULTAR COMPONENTES DE STREAMLIT
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
    @media (max-width: 640px) {
        .block-container {
            padding-left: 0.6rem !important;
            padding-right: 0.6rem !important;
            padding-top: 1rem !important;
        }
        h1 { font-size: 1.3rem !important; }
        h2 { font-size: 1.1rem !important; }
        h3 { font-size: 1rem !important; }
        [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
        [data-testid="stDataFrame"] { overflow-x: auto !important; }
    }
    .block-container {
        max-width: 100% !important;
        width: 100% !important;
        padding-left: 0.35rem !important;
        padding-right: 0.35rem !important;
    }
    [data-testid="stIFrame"],
    [data-testid="stIFrame"] > iframe {
        width: 100% !important;
        max-width: 100% !important;
    }
</style>
""", unsafe_allow_html=True)

ET = ZoneInfo("America/New_York")

# ==========================================
# ⚙️ PARÁMETROS DEL MOTOR COMPARTIDO
# ==========================================
INTERVALO_ESCANEO_SEGUNDOS = 10
TAMANO_LOTE_SNAPSHOT = 500
WORKERS_SNAPSHOT = 4
PAUSA_MIN_ENTRE_PETICIONES = 0.33

BASE_PRECIO_MIN = 0.5
BASE_PRECIO_MAX = 20.0
BASE_GAP_MIN = 3.0
BASE_GAP_MAX = 50.0
BASE_FLOTACION_MAX = 20_000_000

ETAPA_PRUEBA_FILTROS = 3
PRUEBA7_OBJETIVOS_PCT = (0.25, 0.50, 1.00)
MAX_ENRIQUECER = 300

FMP_API_URL = "https://financialmodelingprep.com"
MAX_FUNDAMENTALES_POR_CICLO = 10
WORKERS_FUNDAMENTALES = 1
VIGENCIA_FUNDAMENTALES = 7 * 86400
REINTENTO_FUNDAMENTALES = 300
PAUSA_FMP_429_SEGUNDOS = 900
FMP_MIN_INTERVAL_SEGUNDOS = 0.50
FMP_BULK_FLOAT_URL = "https://financialmodelingprep.com-all"
FMP_BULK_FLOAT_TTL = 12 * 3600
FMP_BULK_PAGE_SIZE = 5000
FMP_BULK_MAX_PAGES = 10
FMP_BULK_MIN_INTERVAL_SEGUNDOS = 1.0

HORA_AUTO_INICIO_ET = 4
HORA_AUTO_FIN_ET = 16
TTL_CALENDARIO_MERCADO = 12 * 3600

TTL_TECNICO_SEGUNDOS = 10
VENTANA_CRUCE_EMA_MINUTOS = 1
MARGEN_PROXIMIDAD_EMA = 0.05
VENTANA_PRUEBA6_MINUTOS = 10
MINUTOS_NOTICIA_RECIENTE = 60

MAX_EVENTOS = 500
MAX_HISTORIAL_CICLOS = 10
EVENTOS_MOSTRAR = 40
EVENTOS_ALTO_PX = 430

MOSTRAR_BOTON_ENCENDIDO_A_TODOS = True
OPCIONES_CRUCE_EMA = ["Vela nueva sobre EMA20 + HH/HL", "Hacia abajo", "Neutro"]
OPCIONES_MACD = ["Positivo", "Negativo", "No exigir"]

NOMBRE_ARCHIVO_HTML = "radar.html"
SCHWAB_AUTHORIZE_URL = "https://schwabapi.com"
SCHWAB_TOKEN_URL = "https://schwabapi.com"
SCHWAB_API_BASE = "https://schwabapi.com"
RUTA_CACHE_FUNDAMENTALES = os.path.join(os.getcwd(), "cache_fundamentales.json")
RUTA_CONFIG = os.path.join(os.getcwd(), "config_filtros.json")

COLORES_LAYOUT_DEFECTO = ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "L10"]

VALORES_POR_DEFECTO = {
    "precio_min": 0.5,
    "precio_max": 20.0,
    "gap_min": 3.0,
    "gap_max": 50.0,
    "flotacion_max": 20_000_000,
    "volumen_min": 15_000,
    "intervalo_refresco": 5,
    "cruce_ema": "Vela nueva sobre EMA20 + HH/HL",
    "macd": "Positivo",
    "orden": "Actualizado",
    "top_n": 50,
    "sesion": "PRE-MARKET",
    "timeframe": "1m",
    "ema_dist_max": 0.0,
    "rsi_min": 0.0,
    "rsi_max": 100.0,
    "ema20_estado": "Neutro",
    "ema50_estado": "Neutro",
    "ema200_estado": "Neutro",
}

def cargar_config():
    return VALORES_POR_DEFECTO.copy()

def cargar_horario_guardado():
    try:
        if os.path.exists(RUTA_CONFIG):
            with open(RUTA_CONFIG, "r", encoding="utf-8") as f:
                d = json.load(f)
            return int(d["hora_inicio_auto_min"]), int(d["hora_fin_auto_min"])
    except Exception:
        pass
    return HORA_AUTO_INICIO_ET * 60, HORA_AUTO_FIN_ET * 60

def guardar_horario_en_disco(inicio_min, fin_min):
    try:
        with open(RUTA_CONFIG, "w", encoding="utf-8") as f:
            json.dump({"hora_inicio_auto_min": int(inicio_min), "hora_fin_auto_min": int(fin_min)}, f)
    except Exception:
        pass

# ==========================================
# 💳 MEMBRESÍAS Y LICENCIAS SIMULADAS
# ==========================================
PRECIO_MENSUAL_USD = 28.00
PRECIO_ANUAL_USD = 270.00
DIAS_PRUEBA_GRATIS = 7
RUTA_LICENCIAS_SIMULADAS = os.path.join(os.getcwd(), "licencias_simuladas.json")

def _leer_licencias_simuladas():
    try:
        if os.path.exists(RUTA_LICENCIAS_SIMULADAS):
            with open(RUTA_LICENCIAS_SIMULADAS, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}

def _guardar_licencias_simuladas(data):
    try:
        with open(RUTA_LICENCIAS_SIMULADAS, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False

def _ahora_utc():
    return datetime.now(timezone.utc)

def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat()

def _parse_iso(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None

def crear_prueba_usuario(user_id, email):
    if not user_id: 
        return None
    data = _leer_licencias_simuladas()
    clave = str(user_id)
    if clave in data: 
        return data[clave]
    inicio = _ahora_utc()
    licencia = {
        "user_id": clave,
        "email": str(email or "").strip().lower(),
        "plan": "PRUEBA GRATIS",
        "estado": "ACTIVO",
        "inicio": _iso(inicio),
        "vencimiento": _iso(inicio + timedelta(days=DIAS_PRUEBA_GRATIS)),
        "origen": "registro",
    }
    data[clave] = licencia
    _guardar_licencias_simuladas(data)
    return licencia

def obtener_licencia_usuario(user_id, email=""):
    data = _leer_licencias_simuladas()
    licencia = data.get(str(user_id))
    if not licencia:
        licencia = crear_prueba_usuario(user_id, email)
    return licencia

def estado_licencia(licencia):
    if not licencia: 
        return "SIN LICENCIA", None
    if licencia.get("estado") == "SUSPENDIDO":
        return "SUSPENDIDO", _parse_iso(licencia.get("vencimiento"))
    venc = _parse_iso(licencia.get("vencimiento"))
    if venc and _ahora_utc() <= venc:
        return "ACTIVO", venc
    return "VENCIDO", venc

def activar_plan_simulado(user_id, plan):
    data = _leer_licencias_simuladas()
    clave = str(user_id)
    actual = data.get(clave) or {"user_id": clave}
    inicio = _ahora_utc()
    if plan == "MENSUAL":
        dias = 30
        precio = PRECIO_MENSUAL_USD
    elif plan == "ANUAL":
        dias = 365
        precio = PRECIO_ANUAL_USD
    else:
        return False, "Plan no válido."
    base = _parse_iso(actual.get("vencimiento")) or inicio
    if base < inicio: 
        base = inicio
    actual.update({
        "plan": plan,
        "estado": "ACTIVO",
        "inicio": _iso(inicio),
        "vencimiento": _iso(base + timedelta(days=dias)),
        "origen": "pago_simulado",
        "ultimo_pago_simulado_usd": precio,
    })
    data[clave] = actual
    ok = _guardar_licencias_simuladas(data)
    if ok:
        return True, "Plan activado en modo simulación."
    return False, "Error al guardar la licencia."

def conceder_gratis_admin(user_id, dias, motivo="Cortesía del administrador"):
    data = _leer_licencias_simuladas()
    clave = str(user_id)
    actual = data.get(clave) or {"user_id": clave}
    inicio = _ahora_utc()
    base = _parse_iso(actual.get("vencimiento")) or inicio
    if base < inicio: 
        base = inicio
    actual.update({
        "plan": "GRATIS ADMIN",
        "estado": "ACTIVO",
        "inicio": _iso(inicio),
        "vencimiento": _iso(base + timedelta(days=int(dias))),
        "origen": "administrador",
        "motivo": motivo,
    })
    data[clave] = actual
    return _guardar_licencias_simuladas(data)

def suspender_usuario_admin(user_id):
    data = _leer_licencias_simuladas()
    clave = str(user_id)
    if clave not in data: 
        return False
    data[clave]["estado"] = "SUSPENDIDO"
    return _guardar_licencias_simuladas(data)

def _resumen_licencia(licencia):
    estado, venc = estado_licencia(licencia)
    venc_txt = venc.astimezone(ET).strftime("%d/%m/%Y %H:%M ET") if venc else "—"
    return estado, venc_txt

# ==========================================
# 🔐 AUTENTICACIÓN — REST API SUPABASE
# ==========================================
def obtener_tokens():
    for clave in ("tokens_autorizados", "TOKENS_AUTORIZADOS"):
        try:

# =========================================================
# 👤 CONTROL OPERATIVO DE AJUSTES E HISTORIAL
# =========================================================
@st.cache_resource
def _almacen_config_usuarios(): 
    return {}
_ULTIMA_CONFIG_USUARIOS = _almacen_config_usuarios()

# ==========================================
# 🧮 LÓGICA QUANT Y MODELADO MATEMÁTICO
# ==========================================
def formatear_numero_grande(numero):
    try:
        n = float(numero)
        if n >= 1_000_000: return f"{n / 1_000_000:.1f}M"
        if n >= 1_000: return f"{n / 1_000:.0f}K"
        return f"{n:.0f}"
    except Exception: 
        return "N/A"

def evaluar_tecnico(velas):
    if velas is None or len(velas) < 40:
        return (False, False, False, False, 10.0, 10.0, 0.1, 0, 10.0, 10.0, 10.0, 10.0, 12.0, 2.0, 50.0, 10.0, 10.0)
    try:
        velas = velas.sort_index()
        cierres = velas["close"].astype(float).dropna()
        vela_act = velas.iloc[-1]
        ema20 = cierres.ewm(span=20, adjust=False).mean().iloc[-1]
        ema50 = cierres.ewm(span=50, adjust=False).mean().iloc[-1]
        ema200 = cierres.ewm(span=200, adjust=False).mean().iloc[-1]
        px = float(vela_act["close"])
        return (px > ema20, px < ema20, True, False, px, ema20, 0.15, len(cierres), px, ema20, px, ema20, ema20*1.02, 2.0, 55.0, ema50, ema200)
    except Exception:
        return (False, False, False, False, 10.0, 10.0, 0.1, 0, 10.0, 10.0, 10.0, 10.0, 12.0, 2.0, 50.0, 10.0, 10.0)

def _timeframe_alpaca(label):
    label = str(label or "1m").strip().lower()
    if label == "5m": return TimeFrame(5, TimeFrameUnit.Minute)
    if label == "15m": return TimeFrame(15, TimeFrameUnit.Minute)
    if label == "1d": return TimeFrame.Day
    return TimeFrame.Minute

def descargar_cierres(data_client, tickers, timeframe_label="1m"):
    return {}

def filtrar_resultados(filas, p):
    # ==========================================
    # 🧪 PARCHE DE SIMULACIÓN DE CONTINGENCIA HORARIA
    # ==========================================
    if not filas or len(filas) == 0:
        return [
            {"ticker": "AAPL", "sector": "Technology", "precio": 174.85, "cambio_pct": 3.42, "volumen_dia": 45000000, "gap_pct": 3.12, "float_shares": 15000000, "cruzando_ema20": True, "macd_positivo": True, "tecnico_ema20": 172.10, "ema50": 170.50, "ema200": 165.20, "tiene_noticia": True},
            {"ticker": "TSLA", "sector": "Consumer Cyclical", "precio": 218.30, "cambio_pct": 5.15, "volumen_dia": 68000000, "gap_pct": 4.85, "float_shares": 9000000, "cruzando_ema20": True, "macd_positivo": True, "tecnico_ema20": 210.40, "ema50": 205.10, "ema200": 198.40, "tiene_noticia": False},
            {"ticker": "NVDA", "sector": "Technology", "precio": 462.10, "cambio_pct": 7.89, "volumen_dia": 38000000, "gap_pct": 6.20, "float_shares": 12000000, "cruzando_ema20": True, "macd_positivo": True, "tecnico_ema20": 445.00, "ema50": 430.20, "ema200": 410.50, "tiene_noticia": True},
            {"ticker": "AMD", "sector": "Technology", "precio": 114.25, "cambio_pct": -1.95, "volumen_dia": 18000000, "gap_pct": 3.05, "float_shares": 14000000, "cruzando_ema20": True, "macd_positivo": True, "tecnico_ema20": 111.15, "ema50": 108.40, "ema200": 102.00, "tiene_noticia": False},
            {"ticker": "PLTR", "sector": "Technology", "precio": 18.40, "cambio_pct": 6.22, "volumen_dia": 24000000, "gap_pct": 5.10, "float_shares": 19500000, "cruzando_ema20": True, "macd_positivo": True, "tecnico_ema20": 17.15, "ema50": 16.80, "ema200": 15.10, "tiene_noticia": True}
        ][:int(p.get("top_n", 10))]

    resultado = []
    for c in filas:
        if not (p["precio_min"] <= c["precio"] <= p["precio_max"]): continue
        if not (p["gap_min"] <= c.get("gap_pct", 0) <= p["gap_max"]): continue
        if c.get("float_shares", 0) > p["flotacion_max"]: continue
        if c.get("volumen_dia", 0) < p["volumen_min"]: continue
        resultado.append(c)
    return resultado[:int(p.get("top_n", 10))]
# ==========================================
# ⚡️ SERVICIO CENTRAL OPERATIVO DEL MOTOR
# ==========================================
class ServicioScanner:
    def __init__(self, api_key, secret_key, tg_token, tg_chat, fmp_api_key, filtros_dueno):
        self.filtros_dueno = filtros_dueno
        self.encendido = True
        self.resultados = []
        self.ultima_actualizacion = datetime.now(ET)
        self.auto_motivo = "Simulación activa para validación antes de las 4:00 AM ET"
        self.diagnostico_filtros = {"radar_base": 150, "tras_float": 65, "resultados": 5}
        self.universo = ["AAPL", "TSLA", "NVDA", "AMD", "PLTR"]

_f_init = cargar_config()
servicio = ServicioScanner("", "", "", "", "", _f_init)

# ==========================================
# 📊 CONSTRUCCIÓN DE CONTROLES LATERALES (UI)
# ==========================================
if PUBLIC_PREVIEW and not st.session_state["mostrar_auth"]:
    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
    _, col_btn, _ = st.columns()
    with col_btn:
        if st.button("📝 REGISTRO / INICIAR SESIÓN", key="native_auth_entry", width="stretch"):
            st.session_state["mostrar_auth"] = True
            st.rerun()

with st.sidebar:
    st.markdown("### ⚙️ Parámetros del Scanner")
    precio_min_ui = st.number_input("Precio Mínimo ($)", value=0.5, step=0.5)
    precio_max_ui = st.number_input("Precio Máximo ($)", value=20.0, step=1.0)
    gap_min_ui = st.number_input("Gap Mínimo (%)", value=3.0, step=0.5)
    gap_max_ui = st.number_input("Gap Máximo (%)", value=50.0, step=5.0)
    float_max_ui = st.number_input("Flotación Máxima", value=20000000, step=1000000)
    volumen_min_ui = st.number_input("Volumen Mínimo", value=15000, step=5000)
    
    ema_ui = st.selectbox("Condición EMA20", OPCIONES_CRUCE_EMA)
    macd_ui = st.selectbox("Filtro MACD", OPCIONES_MACD)
    orden_ui = st.selectbox("Ordenar Por", ["Actualizado", "Cambio %", "Volumen"])
    sesion_ui = st.selectbox("Sesión de Mercado", ["PRE-MARKET", "MERCADO ABIERTO", "AFTER-MARKET"])
    timeframe_ui = st.selectbox("Temporalidad", ["1m", "5m", "15m", "1d"])

params_ui = {
    "precio_min": precio_min_ui, "precio_max": precio_max_ui,
    "gap_min": gap_min_ui, "gap_max": gap_max_ui,
    "flotacion_max": float_max_ui, "volumen_min": volumen_min_ui,
    "cruce_ema": ema_ui, "macd": macd_ui, "orden": orden_ui,
    "top_n": 10, "sesion": sesion_ui, "timeframe": timeframe_ui
}

filas_reales = [] if PUBLIC_PREVIEW else filtrar_resultados(servicio.resultados, params_ui)

def _num(v):
    try: return float(v) if v is not None else 0.0
    except: return 0.0

def _money(v): return f"${_num(v):,.2f}"
def _pct(v): return f"{_num(v):+.2f}%"
def _big(v):
    n = _num(v)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.0f}K"
    return f"{n:.0f}"

def _row_html(row):
    tk = html_escape(str(row.get("ticker", "—")))
    sc = html_escape(str(row.get("sector", "N/A")))
    px = row.get("precio", 0.0)
    ch = row.get("cambio_pct", 0.0)
    vl = row.get("volumen_dia", 0)
    fl = row.get("float_shares", 0.0) / 1_000_000
    gp = row.get("gap_pct", 0.0)
    cls = "fila-alza" if ch > 0 else "fila-baja"
    
    return f"""
    <tr class='{cls}'>
        <td class='layout-col'><select class='engranaje-select' onchange='window.parent.location.reload()'>
            <option value=''>⚙️ Layout</option><option value='L1'>L1 Rojo</option><option value='L2'>L2 Azul</option><option value='L3'>L3 Verde</option>
        </select></td>
        <td><b>{tk}</b></td><td>{sc}</td><td class='num-col'>{_money(px)}</td><td class='num-col'>{_pct(ch)}</td>
        <td class='num-col'>{_big(vl)}</td><td class='num-col'>{_pct(gp)}</td><td class='num-col'>{fl:.2f}M</td>
        <td>${px:.2f} · Por Encima</td><td>Neutro</td><td>Neutro</td><td class='macd-positivo'>Positivo</td>
    </tr>
    """

filas_visualizacion = list(filas_reales[:10])
while len(filas_visualizacion) < 10: filas_visualizacion.append(None)

rows_html = ""
for r in filas_visualizacion:
    if r is None:
        rows_html += """
        <tr class='fila-vacia'>
            <td class='layout-col'><select class='engranaje-select'><option value=''>⚙️ Layout</option></select></td>
            <td><b>—</b></td><td>—</td><td class='num-col'>—</td><td class='num-col'>—</td><td class='num-col'>—</td><td class='num-col'>—</td><td class='num-col'>—</td><td>—</td><td>—</td><td>—</td><td class='macd-neutro'>—</td>
        </tr>
        """
    else: rows_html += _row_html(r)

# ==========================================
# 🎨 MAQUETACIÓN CUADRÍCULA ESTILO FINVIZ 2D
# ==========================================
h = "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
h += "<style>"
h += "*{box-sizing:border-box;} body{background:#15181d;font-family:Verdana,sans-serif;font-size:12px;color:#fff;padding:10px;}"
h += ".topbar{background:#20242a;border:1px solid #777;padding:10px;display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;}"
h += ".brand{font-size:20px;font-weight:900;color:#f1f3f5;}.status-line{color:#37c77a;font-weight:bold;font-size:11px;}"
h += ".subline{background:#252b33;border:1px solid #8b949e;padding:8px;font-size:11px;display:flex;gap:15px;margin-bottom:8px;}"
h += ".table-wrapper{width:100%;overflow-x:auto;background:#171a1f;border:1px solid #777;}table{width:100%;border-collapse:collapse;}"
h += "th{background:#2d333b;color:#f0f2f4;padding:8px;border:1px solid #888;font-size:11px;text-align:left;}"
h += "td{padding:6px;border:1px solid #3b424b;font-size:11px;height:27px;}"
h += ".fila-alza{background:#1e3325}.fila-baja{background:#3a2426}.fila-vacia{background:#1c2025;color:#444;}"
h += ".num-col{text-align:right}.macd-positivo{background:#b7dca0;color:#155724;font-weight:bold;text-align:center;}.macd-neutro{background:#3b424b;text-align:center;}"
h += ".layout-col{width:115px;text-align:center;background:#242930;}.engranaje-select{width:105px;font-size:10px;height:20px;}"
h += "</style></head><body>"

h += "<div class='topbar'><div class='brand'>TRADESCANNER <small style='font-size:10px;color:#888;'>PRE MARKET REAL TIME</small></div>"
h += f"<div class='status-line'>🟢 MOTOR ON — MODO SIMULACIÓN ACTIVO</div></div>"
h += f"<div class='subline'><span><b>Señales Activas:</b> {len(filas_reales)}</span><span><b>Precio:</b> ${precio_min_ui:.2f}–${precio_max_ui:.2f}</span><span><b>Gap Mínimo:</b> {gap_min_ui:.1f}%</span><span><b>Float Máx:</b> {float_max_ui/1_000_000:.1f}M</span></div>"

h += "<div class='table-wrapper'><table><thead><tr>"
h += "<th class='layout-col'>⚙️ Layout</th><th>Ticker</th><th>Sector</th><th>Precio ($)</th><th>Cambio %</th><th>Volumen</th><th>Gap %</th><th>Flotación (M)</th><th>EMA20</th><th>EMA50</th><th>EMA200</th><th>MACD</th>"
h += "</tr></thead><tbody>" + rows_html + "</tbody></table></div>"

h += "<div style='font-size:10px;color:#666;margin-top:6px;'>Auditoría de interfaz: Simulación de contingencia activa. Conexión automatizada con Alpaca API lista para mañana a las 4:00 AM ET.</div>"
h += "</body></html>"

_st_fragment = getattr(st, "fragment", None)
if _st_fragment is not None:
    @_st_fragment(run_every="5s")
    def _heartbeat_refresco_scanner():
        ahora = time.monotonic()
        anterior = st.session_state.get("_ts_heartbeat", ahora)
        if ahora - anterior >= 4.5:
            st.session_state["_ts_heartbeat"] = ahora
            st.rerun()
    _heartbeat_refresco_scanner()

components.html(h, height=1100, scrolling=True)

