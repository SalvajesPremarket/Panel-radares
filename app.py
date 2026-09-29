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

st.set_page_config(page_title="Scanner Pre Market", layout="wide")

# ==========================================
# 🙈 OCULTAR BARRA SUPERIOR DE STREAMLIT
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
# ⚙️ PARÁMETROS DEL MOTOR
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
HORA_MERCADO_INICIO_ET = 9
MINUTO_MERCADO_INICIO_ET = 30
HORA_MERCADO_FIN_ET = 16
HORA_AFTER_FIN_ET = 20
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
        with open(RUTA_CONFIG, "r", encoding="utf-8") as f:
            d = json.load(f)
        return int(d["hora_inicio_auto_min"]), int(d["hora_fin_auto_min"])
    except Exception:
        return HORA_AUTO_INICIO_ET * 60, HORA_AUTO_FIN_ET * 60

def guardar_horario_en_disco(inicio_min, fin_min):
    try:
        with open(RUTA_CONFIG, "w", encoding="utf-8") as f:
            json.dump({"hora_inicio_auto_min": int(inicio_min), "hora_fin_auto_min": int(fin_min)}, f)
    except Exception:
        pass

# ==========================================
# 💳 MEMBRESÍAS Y COBRO SIMULADO
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
    if not user_id: return None
    data = _leer_licencias_simuladas()
    clave = str(user_id)
    if clave in data: return data[clave]
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
    if not licencia: return "SIN LICENCIA", None
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
    if base < inicio: base = inicio
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
    return ok, "Plan activado en modo simulación."

def conceder_gratis_admin(user_id, dias, motivo="Cortesía del administrador"):
    data = _leer_licencias_simuladas()
    clave = str(user_id)
    actual = data.get(clave) or {"user_id": clave}
    inicio = _ahora_utc()
    base = _parse_iso(actual.get("vencimiento")) or inicio
    if base < inicio: base = inicio
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
    if clave not in data: return False
    data[clave]["estado"] = "SUSPENDIDO"
    return _guardar_licencias_simuladas(data)

def _resumen_licencia(licencia):
    estado, venc = estado_licencia(licencia)
    venc_txt = venc.astimezone(ET).strftime("%d/%m/%Y %H:%M ET") if venc else "—"
    return estado, venc_txt

# ==========================================
# 🔐 AUTENTICACIÓN — ADMIN + SUPABASE AUTH REST
# ==========================================
def verificar_token(token_usuario):
    admin_token = str(st.secrets.get("ADMIN_TOKEN", "")).strip()
    if admin_token and token_usuario == admin_token:
        return True, "2099-01-01"
    tokens = obtener_tokens()
    if token_usuario in tokens:
        try:
# =========================================================
# 👤 PERSISTENCIA CONFIGURACIÓN POR USUARIO
# =========================================================
@st.cache_resource
def _almacen_ultima_configuracion_usuarios():
    return {}

_ULTIMA_CONFIG_USUARIOS = _almacen_ultima_configuracion_usuarios()
_RUTA_ULTIMA_CONFIG_USUARIOS = os.path.join(os.getcwd(), "ultima_config_usuarios.json")
try:
    if os.path.exists(_RUTA_ULTIMA_CONFIG_USUARIOS):
        with open(_RUTA_ULTIMA_CONFIG_USUARIOS, "r", encoding="utf-8") as _f_cfg:
            _disk_cfg = json.load(_f_cfg)
            if isinstance(_disk_cfg, dict):
                _ULTIMA_CONFIG_USUARIOS.update(_disk_cfg)
except Exception:
    pass

_CONFIG_USUARIO_KEYS = (
    "f_price_min", "f_price_max", "f_gap_min", "f_gap_max",
    "f_float_max", "f_vol", "f_ema", "f_mac", "f_order",
    "market_session", "timeframe", "ema_dist_max",
    "rsi_min", "rsi_max", "ema20_estado", "ema50_estado",
    "ema200_estado", "c_active", "c_start", "c_end",
    "c_lang", "c_wnd", "c_broker", "c_url", "refresh_sec",
)

def _clave_configuracion_activa():
    try:
        email = str(st.session_state.get("usuario_auth", {}).get("email", "")).strip().lower()
        if email: return email
        token = str(st.session_state.get("token_verificado", "")).strip()
        if token: return "admin:" + hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]
    except Exception: pass
    return ""

def _email_usuario_activo():
    return _clave_configuracion_activa()

def _restaurar_ultima_configuracion_servidor():
    if PUBLIC_PREVIEW: return False
    email = _email_usuario_activo()
    if not email: return False
    guardada = _ULTIMA_CONFIG_USUARIOS.get(email)
    if not isinstance(guardada, dict) or not guardada: return False
    cambio = False
    for clave in _CONFIG_USUARIO_KEYS:
        if clave in guardada and str(st.query_params.get(clave, "")) == "":
            st.query_params[clave] = str(guardada[clave])
            cambio = True
    return cambio

def _guardar_ultima_configuracion_servidor():
    if PUBLIC_PREVIEW: return
    email = _email_usuario_activo()
    if not email: return
    estado = {}
    for clave in _CONFIG_USUARIO_KEYS:
        valor = st.query_params.get(clave, None)
        if valor is not None and str(valor) != "":
            estado[clave] = str(valor)
    if estado:
        estado["_saved_at"] = datetime.now(timezone.utc).isoformat()
        _ULTIMA_CONFIG_USUARIOS[email] = estado
        try:
            with open(_RUTA_ULTIMA_CONFIG_USUARIOS, "w", encoding="utf-8") as _f_cfg:
                json.dump(_ULTIMA_CONFIG_USUARIOS, _f_cfg, ensure_ascii=False, indent=2)
        except Exception: pass

if _restaurar_ultima_configuracion_servidor():
    st.rerun()

ADMIN_TOKEN = st.secrets.get("ADMIN_TOKEN", None)
_ADMIN_TOKENS_RAW = st.secrets.get("ADMIN_TOKENS", "")
if isinstance(_ADMIN_TOKENS_RAW, (list, tuple, set)):
    ADMIN_TOKENS = {str(x).strip() for x in _ADMIN_TOKENS_RAW if str(x).strip()}
else:
    ADMIN_TOKENS = {x.strip() for x in str(_ADMIN_TOKENS_RAW).split(",") if x.strip()}
if ADMIN_TOKEN: ADMIN_TOKENS.add(str(ADMIN_TOKEN).strip())

ES_ADMIN = (st.session_state.get("tipo_acceso") == "admin" and str(st.session_state.get("token_verificado")) in ADMIN_TOKENS)

# ==========================================
# 💳 CONTROL DE LICENCIA / PAYWALL
# ==========================================
LICENCIA_ACTUAL = None
ESTADO_LICENCIA = "ADMIN" if ES_ADMIN else "SIN LICENCIA"
VENCIMIENTO_LICENCIA_DT = None
if not ES_ADMIN and not PUBLIC_PREVIEW:
    _u = st.session_state.get("usuario_auth", {})
    LICENCIA_ACTUAL = obtener_licencia_usuario(_u.get("user_id", ""), _u.get("email", ""))
    ESTADO_LICENCIA, VENCIMIENTO_LICENCIA_DT = estado_licencia(LICENCIA_ACTUAL)

    if ESTADO_LICENCIA != "ACTIVO":
        st.markdown("""
        <style>
        .paywall {max-width:850px;margin:55px auto;padding:30px;border:1px solid #334155;border-radius:18px;background:#0d1118;text-align:center;}
        .paywall h1{color:#d4af37;margin-bottom:8px;}
        .paywall p{color:#aeb7c5;}
        </style>
        <div class="paywall">
          <h1>🔒 Tu acceso requiere una membresía activa</h1>
          <p>La prueba gratuita terminó o la cuenta todavía no tiene una licencia asignada.</p>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("### Elige un plan — MODO PRUEBA SIMULADO")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### 💳 Mensual — $28 USD")
            if st.button("ACTIVAR PLAN MENSUAL (SIMULADO)", width="stretch"):
                ok, msg = activar_plan_simulado(_u.get("user_id", ""), "MENSUAL")
                if ok: st.success("✅ Membresía mensual activada."); st.rerun()
        with c2:
            st.markdown("#### 💳 Anual — $270 USD")
            if st.button("ACTIVAR PLAN ANUAL (SIMULADO)", width="stretch"):
                ok, msg = activar_plan_simulado(_u.get("user_id", ""), "ANUAL")
                if ok: st.success("✅ Membresía anual activada."); st.rerun()
        st.stop()

# ==========================================
# 👤 PANEL LATERAL DE SESIÓN NATIVA
# ==========================================
with st.sidebar:
    st.markdown("### 👤 Estado de Sesión")
    if PUBLIC_PREVIEW:
        st.info("👀 Modo Explorador")
        if st.button("📝 REGISTRO / INICIAR SESIÓN", key="sidebar_auth_public", width="stretch"):
            st.session_state["mostrar_auth"] = True
            st.rerun()
    elif ES_ADMIN: st.success("👑 Administrador Master")
    else: st.info(st.session_state.get("usuario_auth", {}).get("email", "Usuario"))

    if not PUBLIC_PREVIEW and st.button("🚪 CERRAR SESIÓN DEL SCANNER", key="cerrar_sesion_global", width="stretch"):
        cerrar_sesion()
        st.rerun()

    if ES_ADMIN:
        st.markdown("---")
        st.markdown("### 👑 Administración de Licencias")
        licencias = _read_licencias_simuladas = _leer_licencias_simuladas()
        st.metric("Usuarios Totales", len(licencias))
        for uid, lic in list(licencias.items())[:10]:
            est, v_tx = _resumen_licencia(lic)
            st.markdown(f"**{lic.get('email','Usuario')}** ({est})")
            if st.button("🎁 +30 Días Gratis", key=f"g_{uid}"):
                conceder_gratis_admin(uid, 30); st.rerun()

# ==========================================
# ⚙️ OAUTH CHARLES SCHWAB
# ==========================================
def _schwab_secret(nombre, default=""):
    try: return str(st.secrets.get(nombre, default) or default).strip()
    except: return str(default or "").strip()

def _schwab_client_id(): return _schwab_secret("SCHWAB_CLIENT_ID")
def _schwab_client_secret(): return _schwab_secret("SCHWAB_CLIENT_SECRET")
def _schwab_redirect_uri(): return _schwab_secret("SCHWAB_REDIRECT_URI")

def _schwab_exchange_code(code):
    cid, sec, red = _schwab_client_id(), _schwab_client_secret(), _schwab_redirect_uri()
    try:
        r = requests.post(SCHWAB_TOKEN_URL, data={"grant_type": "authorization_code", "code": code, "redirect_uri": red}, auth=(cid, sec), timeout=15)
        if r.status_code == 200:
            st.session_state["schwab_token"] = r.json()
            return True, "Conectado."
        return False, f"Error Schwab: {r.text}"
    except Exception as e: return False, str(e)

def _schwab_access_token(): return (st.session_state.get("schwab_token") or {}).get("access_token", "")
def _schwab_authorize_url():
    cid, red = _schwab_client_id(), _schwab_redirect_uri()
    if not cid or not red: return ""
    return f"{SCHWAB_AUTHORIZE_URL}?client_id={quote(cid)}&redirect_uri={quote(red)}&response_type=code"

def _schwab_send_layout_bridge(ticker, layout_color, bridge_url):
    if not bridge_url: return False, "Falta URL del puente."
    try:
        r = requests.post(bridge_url, json={"broker": "Charles Schwab", "ticker": str(ticker), "layout_color": str(layout_color), "timestamp": time.time()}, timeout=5)
        return (True, "Enviado.") if r.ok else (False, f"HTTP {r.status_code}")
    except Exception as e: return False, str(e)

# ==========================================
# 🧮 LÓGICA PURA Y FILTROS QUANT DEL MOTOR
# ==========================================
def formatear_numero_grande(numero):
    try:
        n = float(numero)
        if n >= 1_000_000: return f"{n / 1_000_000:.1f}M"
        if n >= 1_000: return f"{n / 1_000:.0f}K"
        return f"{n:.0f}"
    except: return "N/A"

def evaluar_tecnico(velas):
    if velas is None or len(velas) < 220:
        return (False, False, False, False, None, None, None, 0, None, None, None, None, None, None, None, None, None)
    try:
        velas = velas.sort_index()
        cierres = velas["close"].astype(float).dropna()
        if len(cierres) < 220: return (False, False, False, False, None, None, None, 0, None, None, None, None, None, None, None, None, None)
        
        vela_prev, vela_act = velas.iloc[-2], velas.iloc[-1]
        ema20 = cierres.ewm(span=20, adjust=False).mean()
        ema50 = cierres.ewm(span=50, adjust=False).mean().iloc[-1]
        ema200 = cierres.ewm(span=200, adjust=False).mean().iloc[-1]
        macd_line = cierres.ewm(span=12, adjust=False).mean() - cierres.ewm(span=26, adjust=False).mean()
        
        px_act, ema_act = float(vela_act["close"]), float(ema20.iloc[-1])
        open_act, low_act, low_prev = float(vela_act["open"]), float(vela_act["low"]), float(vela_prev["low"])
        
        cruz_arriba = bool(open_act > float(ema20.iloc[-2]) and low_act > low_prev)
        macd_pos = bool(macd_line.iloc[-1] > 0)
        
        return (cruz_arriba, False, macd_pos, not macd_pos, px_act, ema_act, float(macd_line.iloc[-1]), len(cierres), float(vela_prev["close"]), float(ema20.iloc[-2]), px_act, ema_act, px_act*1.02, 2.0, 50.0, ema50, ema200)
    except:
        return (False, False, False, False, None, None, None, 0, None, None, None, None, None, None, None, None, None)
# ==========================================
# 🎨 CAPTURA Y PROCESAMIENTO DE PARÁMETROS DE INTERFAZ
# ==========================================
def _qtxt(clave, defecto):
    val = st.query_params.get(clave, defecto)
    return str(val[0] if isinstance(val, list) else val).strip()

precio_min_ui = float(params_ui.get("precio_min", 0.5))
precio_max_ui = float(params_ui.get("precio_max", 20.0))
gap_min_ui = float(params_ui.get("gap_min", 3.0))
gap_max_ui = float(params_ui.get("gap_max", 50.0))
float_max_ui = float(params_ui.get("flotacion_max", 20_000_000))
volumen_min_ui = int(params_ui.get("volumen_min", 15_000))
ema_ui = str(params_ui.get("cruce_ema", "Vela nueva sobre EMA20 + HH/HL"))
macd_ui = str(params_ui.get("macd", "Positivo"))
orden_ui = str(params_ui.get("orden", "Actualizado"))
sesion_ui = str(params_ui.get("sesion", "PRE-MARKET"))
timeframe_ui = str(params_ui.get("timeframe", "1m"))

# Sincronizar parámetros con el motor compartido
try:
    servicio.filtros_dueno.update(params_ui)
    servicio.filtros_dueno["ema50_estado"] = "Neutro"
    servicio.filtros_dueno["ema200_estado"] = "Neutro"
except Exception:
    pass

_guardar_ultima_configuracion_servidor()

# Si es modo visitante, la carátula se renderiza estéticamente pero vacía de señales reales
filas_reales = [] if PUBLIC_PREVIEW else filtrar_resultados(list(servicio.resultados), params_ui)

def _num(v, default=0.0):
    try: return float(v) if v is not None and v != "" else default
    except: return default

def _entero(v, default=0):
    try: return int(float(v)) if v is not None and v != "" else default
    except: return default

def _safe_text(v, default=""):
    return html_escape(str(v if v is not None else default))

def _money(v): return f"${_num(v):,.2f}"
def _pct(v): return f"{_num(v):+.2f}%"
def _big(v):
    n = _num(v)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.0f}K"
    return f"{n:.0f}"

def _row_html(row):
    ticker = _safe_text(row.get("ticker", ""))
    sector = _safe_text(row.get("sector", "N/A"))
    precio = _num(row.get("precio"))
    cambio = _num(row.get("cambio_pct"))
    volumen = _entero(row.get("volumen_dia"))
    flotacion = _num(row.get("float_shares")) / 1_000_000 if row.get("float_shares") else 0.0
    ema_ok = bool(row.get("cruzando_ema20"))
    mac_pos = bool(row.get("macd_positivo"))
    noticia = bool(row.get("tiene_noticia"))
    fila = "fila-alza" if cambio > 0 else ("fila-baja" if cambio < 0 else "")
    
    ema20_val = row.get("tecnico_ema20", 0.0)
    ema50_val = row.get("ema50", 0.0)
    ema200_val = row.get("ema200", 0.0)
    
    ema_txt = f"${ema20_val:.2f} · Por Encima" if ema_ok else f"${ema20_val:.2f} · Neutro"
    ema50_txt = f"${ema50_val:.2f} · Neutro"
    ema200_txt = f"${ema200_val:.2f} · Neutro"
    mac_txt = "Positivo" if mac_pos else "Neutro"
    mac_cls = "macd-positivo" if mac_pos else "macd-neutro"
    news = " 🔥" if noticia else ""
    
    return (
        f"<tr class='{fila}'>"
        f"<td class='layout-col'><select class='engranaje-select' onchange='window.parent.location.reload()'>"
        f"<option value=''>⚙️ Layout</option>"
        f"<option value='L1'>L1 Rojo</option><option value='L2'>L2 Azul</option>"
        f"<option value='L3'>L3 Verde</option><option value='L4'>L4 Amarillo</option>"
        f"<option value='L5'>L5 Morado</option><option value='L6'>L6 Naranja</option>"
        f"<option value='L7'>L7 Blanco</option><option value='L8'>L8 Negro</option>"
        f"</select></td>"
        f"<td><b>{ticker}</b>{news}</td>"
        f"<td>{sector}</td>"
        f"<td class='num-col'>{_money(precio)}</td>"
        f"<td class='num-col'>{_pct(cambio)}</td>"
        f"<td class='num-col'>{_big(volumen)}</td>"
        f"<td class='num-col'>{_pct(row.get('gap_pct'))}</td>"
        f"<td class='num-col'>{flotacion:.2f}M</td>"
        f"<td>{_safe_text(ema_txt)}</td>"
        f"<td>{_safe_text(ema50_txt)}</td>"
        f"<td>{_safe_text(ema200_txt)}</td>"
        f"<td class='{mac_cls}'>{mac_txt}</td></tr>"
    )

# Mantener de forma permanente las 10 líneas de la carátula estilo Finviz
filas_visualizacion = list(filas_reales[:10])
while len(filas_visualizacion) < 10:
    filas_visualizacion.append(None)

rows_html = ""
for item in filas_visualizacion:
    if item is None:
        rows_html += (
            "<tr class='fila-vacia'>"
            "<td class='layout-col'><select class='engranaje-select'>"
            "<option value=''>⚙️ Layout</option>"
            "</select></td>"
            "<td><b>—</b></td><td>—</td><td class='num-col'>—</td>"
            "<td class='num-col'>—</td><td class='num-col'>—</td><td class='num-col'>—</td>"
            "<td class='num-col'>—</td><td>—</td><td>—</td><td>—</td><td class='macd-neutro'>—</td></tr>"
        )
    else:
        rows_html += _row_html(item)

_estado_txt = "ON" if servicio.encendido else "OFF"
_refresh_raw = str(st.query_params.get("refresh_sec", "180"))
try: refresh_sec = max(5, int(float(_refresh_raw)))
except: refresh_sec = 180
if PUBLIC_PREVIEW: refresh_sec = 180

# ==========================================
# 🎨 INYECTAR FRON-END HTML + COMPONENTES CSS
# ==========================================
h = "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
h += "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
h += "<style>"
h += "*{box-sizing:border-box;}"
h += "html,body{margin:0;padding:0;width:100%;background:#15181d;font-family:Verdana,Arial,sans-serif;font-size:12px;color:#fff;overflow-x:hidden;}"
h += ".main-container{width:100%;padding:6px;}"
h += ".topbar{background:#20242a;border:1px solid #777;padding:10px;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center;}"
h += ".brand{font-size:22px;font-weight:900;letter-spacing:.3px;color:#f1f3f5;}.brand small{font-size:10px;color:#8f98a3;}"
h += ".subline{background:#252b33;border:1px solid #8b949e;padding:7px 9px;margin-bottom:6px;font-size:11px;font-weight:700;display:flex;gap:16px;flex-wrap:wrap;}"
h += ".result-title{background:#2d333b;color:#f0f2f4;border:1px solid #777;border-bottom:0;padding:5px 8px;font-size:11px;font-weight:900;}"
h += ".table-wrapper{width:100%;overflow-x:auto;background:#171a1f;border:1px solid #777;}table{width:100%;min-width:930px;border-collapse:collapse;}"
h += "th{background:#2d333b;color:#f0f2f4;font-weight:bold;padding:7px;border:1px solid #888;font-size:10px;text-align:left;}"
h += "td{padding:5px 7px;border:1px solid #3b424b;font-size:11px;color:#dce1e6;white-space:nowrap;height:27px;}"
h += ".fila-alza{background:#1e3325}.fila-baja{background:#3a2426}.fila-vacia{background:#1c2025;color:#666;}"
h += ".num-col{text-align:right}.macd-positivo{background:#b7dca0;color:#155724;font-weight:bold;text-align:center}.macd-neutro{background:#3b424b;text-align:center;}"
h += ".layout-col{width:120px;text-align:center;background:#242930;}.engranaje-select{width:112px;font-size:9px;height:21px;}"
h += ".footer-note{margin-top:4px;font-size:9px;color:#7f8995;display:flex;justify-content:space-between;}"
h += "</style></head><body><div class='main-container'>"

h += f"<div class='topbar'><div class='brand'>TRADESCANNER <small>PRE MARKET · REAL TIME</small></div><div style='color:#37c77a; font-weight:bold;'>🟢 MOTOR ON — MODO AUDITORÍA</div></div>"
h += f"<div class='subline'><span><b>Señales:</b> {len(filas_reales)}</span><span><b>Precio:</b> ${precio_min_ui:.2f}–${precio_max_ui:.2f}</span><span><b>Gap:</b> {gap_min_ui:.1f}%–{gap_max_ui:.1f}%</span><span><b>Float:</b> ≤ {float_max_ui/1_000_000:.1f}M</span><span><b>Vol:</b> ≥ {_big(volumen_min_ui)}</span><span><b>EMA20:</b> {_safe_text(ema_ui)}</span><span><b>MACD:</b> {_safe_text(macd_ui)}</span></div>"

h += "<div class='result-title'>RESULTADOS · VISUALIZACIÓN · 10 LÍNEAS PERMANENTES</div>"
h += "<div class='table-wrapper'><table><thead><tr>"
h += "<th class='layout-col'>⚙️ Layout</th><th>Ticker</th><th>Sector</th><th>Precio ($)</th><th>Cambio %</th><th>Volumen</th><th>Gap %</th><th>Flotación (M)</th><th>EMA20 ({timeframe_ui})</th><th>EMA50</th><th>EMA200</th><th>MACD</th>"
h += "</tr></thead><tbody>" + rows_html + "</tbody></table></div>"

_ultima_scan_txt = servicio.ultima_actualizacion.strftime("%H:%M:%S ET") if servicio.ultima_actualizacion else "aún no ejecutado"
_universo_txt = str(len(servicio.universo))
h += f"<div class='footer-note'><span>Motor Real · Técnico: {timeframe_ui.upper()} · Último escaneo: {_safe_text(_ultima_scan_txt)} · Universo: {_universo_txt}</span><span>Estado: {_safe_text(_estado_txt)} · {refresh_sec}s refresco</span></div>"
h += "</div></body></html>"

# ==========================================
# 🔄 REFRESCO NATIVO AUTOMÁTICO DE INTERFAZ
# ==========================================
_st_fragment = getattr(st, "fragment", None)
if _st_fragment is not None:
    @_st_fragment(run_every=f"{refresh_sec}s")
    def _heartbeat_refresco_scanner():
        ahora = time.monotonic()
        anterior = st.session_state.get("_ts_heartbeat", ahora)
        if ahora - anterior >= max(1, refresh_sec - 0.5):
            st.session_state["_ts_heartbeat"] = ahora
            st.rerun()
        else:
            st.session_state.setdefault("_ts_heartbeat", ahora)
    _heartbeat_refresco_scanner()

# Botón nativo de salida transparente fuera del Iframe
if not PUBLIC_PREVIEW:
    _salir_col1, _salir_col2 = st.columns([0.90, 0.10])
    with _salir_col2:
        if st.button("SALIR", key="ts_native_logout", help="Cerrar sesión"):
            cerrar_sesion()
            st.query_params.clear()
            st.rerun()

# Renderizado final encapsulado protegido dentro del Iframe extendido
components.html(h, height=1100, scrolling=True)

