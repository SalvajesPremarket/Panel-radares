import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import os
import json
import time
import hashlib
import hmac
import secrets
from urllib.parse import quote
from html import escape as html_escape
import threading
import traceback
from datetime import date, datetime, timedelta, timezone, time as dt_time
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
import streamlit as st
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.trading.requests import GetAssetsRequest, GetCalendarRequest
from BotTradeScanner.integracion.live_motor_bridge import MotorVelasBridge

st.set_page_config(page_title="Scanner Pre Market", layout="wide")

# Precio y gap viven DENTRO del cuadro gris (iframe). Los controles nativos de afuera quedan apagados.
_USAR_FILTROS_NATIVOS = False

# ------------------------------------------------------------------
# Canal fiable cuadro gris -> Python.
# Antes los cambios del cuadro gris viajaban por la URL del navegador (history /
# location), que en muchos despliegues no llega al servidor y por eso todo volvia
# al valor anterior en el refresh. Ahora el cuadro gris vive en un componente
# bidireccional: cada cambio se devuelve a Python directamente (sin URL).
# ------------------------------------------------------------------
_TS_COMP_HTML = r'''<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
html,body{margin:0;padding:0;width:100%;height:100%;background:#15181d;overflow:hidden}
#wrap{position:relative;width:100%;height:100vh;min-height:680px;background:#15181d;overflow:hidden}
iframe{position:absolute;left:0;top:0;width:100%;height:100%;border:0;background:#15181d}
</style></head>
<body><div id="wrap"></div>
<script>
(function(){
  var wrap=document.getElementById('wrap');
  var current=null,pending=null,lastHtml=null,height=0,deferSince=0,timer=null;
  function post(type,data){var m={isStreamlitMessage:true,type:type};for(var k in data){m[k]=data[k];}window.parent.postMessage(m,'*');}
  function setHeight(h){if(!h||h===height)return;height=h;wrap.style.height=h+'px';post('streamlit:setFrameHeight',{height:h});}
  function viewportHeight(){
    try{
      var h=Number(window.top.innerHeight)||0;
      if(h>300)return Math.max(680,Math.min(1400,h-12));
    }catch(e){}
    return 900;
  }
  function fitViewport(){setHeight(viewportHeight());}
  function busy(f){try{return !!(f.contentWindow&&f.contentWindow._tsDirty);}catch(e){return false;}}
  function isOurs(w){return !!w&&((current&&current.contentWindow===w)||(pending&&pending.contentWindow===w));}
  function show(f){
    // Conservado por compatibilidad con mensajes tsReady antiguos.
    if(!f)return;
    f.style.visibility='visible';
    if(f===current)pending=null;
  }
  function run(){
    if(!lastHtml)return;
    // ESTABLE: no desmontar/recrear iframes. El scanner ya no usa refresh
    // dentro de st.fragment; cada cambio de configuración llega por el
    // componente y puede actualizar el mismo iframe sin pantalla blanca.
    if(!current){
      var f=document.createElement('iframe');
      f.__html=lastHtml;
      try{f.setAttribute('allow','loopback-network; local-network; local-network-access');}catch(e){}
      current=f;
      wrap.appendChild(f);
    }
    if(current.__html===lastHtml){
      current.style.visibility='visible';
      return;
    }
    current.__html=lastHtml;
    current.style.visibility='visible';
    try{current.srcdoc=lastHtml;}catch(e){current.src='data:text/html;charset=utf-8,'+encodeURIComponent(lastHtml);}
  }
  function schedule(){if(timer)return;timer=setTimeout(function(){timer=null;run();},0);}
  window.addEventListener('message',function(ev){
    var d=ev.data;if(!d)return;
    if(d.type==='streamlit:render'){
      var a=d.args||{};
      if(a.alto)setHeight(parseInt(a.alto,10));
      if(typeof a.html==='string'&&a.html!==lastHtml){lastHtml=a.html;schedule();}
      return;
    }
    if(!isOurs(ev.source))return;
    if(d.tsNav){
      post('streamlit:setComponentValue',{value:{id:String(Date.now())+'-'+Math.random().toString(36).slice(2),q:String(d.q||'')},dataType:'json'});
      return;
    }
    if(d.tsReady&&pending&&ev.source===pending.contentWindow){show(pending);}
  });
  post('streamlit:componentReady',{apiVersion:1});
  setHeight(viewportHeight());
  setTimeout(fitViewport,300);
  window.addEventListener('resize',function(){setTimeout(fitViewport,80);});
  window.addEventListener('orientationchange',function(){setTimeout(fitViewport,180);});
})();
</script></body></html>
'''
_TS_COMP_OK = False
_ts_scanner_ui = None
try:
    import streamlit.components.v1 as _stc
    _TS_COMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ts_scanner_component")
    os.makedirs(_TS_COMP_DIR, exist_ok=True)
    _TS_COMP_FILE = os.path.join(_TS_COMP_DIR, "index.html")
    _actual = ""
    if os.path.exists(_TS_COMP_FILE):
        with open(_TS_COMP_FILE, "r", encoding="utf-8") as _f_c:
            _actual = _f_c.read()
    if _actual != _TS_COMP_HTML:
        with open(_TS_COMP_FILE, "w", encoding="utf-8") as _f_c:
            _f_c.write(_TS_COMP_HTML)
    _ts_scanner_ui = _stc.declare_component("ts_scanner_ui", path=_TS_COMP_DIR)
    _TS_COMP_OK = True
except Exception as _e_comp:
    print(f"⚠️ Componente del scanner no disponible, se usa st.iframe: {_e_comp}")
    _TS_COMP_OK = False

# ESTABILIZACIÓN WEB: el scanner no usa el componente V1 bidireccional.
# Ese componente añade un iframe contenedor + el iframe del scanner y, al
# recibir nuevos argumentos, puede desmontarse/recrearse durante un rerun.
# Para evitar la pantalla blanca mantenemos un único iframe HTML nativo.
_TS_USE_COMPONENT = False
_TS_COMP_OK = bool(_TS_COMP_OK and _TS_USE_COMPONENT)

# Sin "flash" en el refresh automático: Streamlit atenúa (opacity) los elementos
# mientras se recalcula y el iframe de la tabla parpadea al recargarse. Se deja todo
# opaco, sin transición, y el iframe con el mismo color de fondo que su contenido.
st.markdown("""
<style>
    [data-stale="true"], [data-stale="true"] * {
        opacity: 1 !important;
        transition: none !important;
        filter: none !important;
    }
    .stApp [data-testid="stAppViewContainer"], .stApp [data-testid="stMain"],
    .stApp [data-testid="stElementContainer"], .stApp .element-container {
        transition: none !important;
        animation: none !important;
    }
    iframe, [data-testid="stCustomComponentV1"], [data-testid="stIFrame"] {
        background: #15181d !important;
        transition: none !important;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 🙈 OCULTAR BARRA SUPERIOR DE STREAMLIT (Share, GitHub, editar, menú, badges)
# 📱 + AJUSTES RESPONSIVOS para que se vea bien en celular
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

    /* --- Responsivo: pantallas de celular (ancho <= 640px) --- */
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
        /* la tabla de resultados no se recorta: permite scroll horizontal */
        [data-testid="stDataFrame"] { overflow-x: auto !important; }
    }

    /* --- La carátula del scanner debe ocupar todo el ancho disponible --- */
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
# 📊 INSTRUMENTACIÓN FASE 1 — CONSUMO REAL
# Solo mide; no modifica la lógica del scanner.
# ==========================================
METRICAS_FASE1_VERSION = 1

# ==========================================
# ⚙️ PARÁMETROS DEL MOTOR
# ==========================================
INTERVALO_ESCANEO_SEGUNDOS = 10        # cada cuánto el motor recorre el mercado (una sola vez para todos los usuarios)
TAMANO_LOTE_SNAPSHOT = 500             # tickers por petición de snapshot
WORKERS_SNAPSHOT = 4                   # peticiones de snapshot en paralelo
PAUSA_MIN_ENTRE_PETICIONES = 0.33      # ~180 peticiones/min a Alpaca (límite: 200/min)

# Radar base: rango AMPLIO que el motor enriquece. Cada usuario filtra su vista dentro de este rango.
BASE_PRECIO_MIN = 0.5
BASE_PRECIO_MAX = 20.0
BASE_GAP_MIN = 3.0
BASE_GAP_MAX = 50.0
BASE_FLOTACION_MAX = 20_000_000

# 🧪 ETAPA DE DEPURACIÓN DE FILTROS
# 1 = solo precio + EMA20 + MACD. Telegram queda APAGADO.
# Luego podremos pasar a 2, 3, 4... agregando un filtro por vez.
ETAPA_PRUEBA_FILTROS = 3

# PRUEBA 7: medir alcanzabilidad de objetivos sobre la misma señal.
PRUEBA7_OBJETIVOS_PCT = (0.25, 0.50, 1.00)

MAX_ENRIQUECER = 300                   # Muestra técnica amplia, manteniendo ciclos rápidos.

# Float: FMP es la fuente principal; volumen y velas técnicas se obtienen con Alpaca.
FMP_API_URL = "https://financialmodelingprep.com/stable/shares-float"
MAX_FUNDAMENTALES_POR_CICLO = 10       # Respaldo individual; la fuente preferida es el bulk.
WORKERS_FUNDAMENTALES = 1               # Serializado con lock para respetar el ritmo de FMP.
VIGENCIA_FUNDAMENTALES = 7 * 86400
REINTENTO_FUNDAMENTALES = 300
PAUSA_FMP_429_SEGUNDOS = 900            # tras HTTP 429, pausa FMP durante 15 min
FMP_MIN_INTERVAL_SEGUNDOS = 0.50        # Ritmo rápido de respaldo; el lock evita ráfagas concurrentes.
FMP_BULK_FLOAT_URL = "https://financialmodelingprep.com/stable/shares-float-all"
FMP_BULK_FLOAT_TTL = 12 * 3600           # FMP actualiza All Shares Float diariamente; refrescamos como máximo 2 veces/día
FMP_BULK_PAGE_SIZE = 5000
FMP_BULK_MAX_PAGES = 10                 # El universo de acciones de EE.UU. cabe normalmente en pocas páginas.
FMP_BULK_MIN_INTERVAL_SEGUNDOS = 1.0

# Horario automático: 04:00–16:00 ET, solo días de mercado según Alpaca.
HORA_AUTO_INICIO_ET = 4
HORA_AUTO_FIN_ET = 20
HORA_MERCADO_INICIO_ET = 9
MINUTO_MERCADO_INICIO_ET = 30
HORA_MERCADO_FIN_ET = 16
HORA_AFTER_FIN_ET = 20
TTL_CALENDARIO_MERCADO = 12 * 3600

TTL_TECNICO_SEGUNDOS = 10              # no recalcular EMA/MACD de un ticker más seguido que esto
MAX_TIMEFRAMES_ACTIVOS = 4        # temporalidades que el motor calcula a la vez (las más recientes)
VIGENCIA_TIMEFRAME_ACTIVO = 1800  # una temporalidad sigue activa 30 min después de que alguien la pidió
VENTANA_CRUCE_EMA_MINUTOS = 1
MARGEN_PROXIMIDAD_EMA = 0.05
# PRUEBA 6: ventana fija de observación posterior a la detección.
# Es diagnóstico únicamente; no modifica ningún filtro ni resultado.
VENTANA_PRUEBA6_MINUTOS = 10
MINUTOS_NOTICIA_RECIENTE = 60

# --- Cuadro "Eventos en vivo" (parte de abajo de la interfaz) ---
MAX_EVENTOS = 500                      # eventos que guarda el motor en memoria
MAX_HISTORIAL_CICLOS = 10               # ciclos recientes conservados para depuración
EVENTOS_MOSTRAR = 40                   # filas visibles en el cuadro
EVENTOS_ALTO_PX = 430                  # alto del cuadro (con scroll)

# --- Botón encender/apagar del scanner ---
MOSTRAR_BOTON_ENCENDIDO_A_TODOS = True  # True: lo ve cualquier usuario con licencia. False: solo el administrador

# --- Opciones de los filtros técnicos (la primera es la que viene por defecto) ---
OPCIONES_CRUCE_EMA = ["Vela nueva sobre EMA20 + HH/HL", "Hacia abajo", "Neutro"]
OPCIONES_MACD = ["Positivo", "Negativo", "No exigir"]

NOMBRE_ARCHIVO_HTML = "radar.html"

# Charles Schwab: OAuth 2.0. Las credenciales sensibles deben ir en
# Streamlit Secrets (SCHWAB_CLIENT_ID / SCHWAB_CLIENT_SECRET /
# SCHWAB_REDIRECT_URI). Nunca se escriben en el HTML ni en localStorage.
SCHWAB_AUTHORIZE_URL = "https://api.schwabapi.com/v1/oauth/authorize"
SCHWAB_TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
SCHWAB_API_BASE = "https://api.schwabapi.com"
RUTA_CACHE_FUNDAMENTALES = os.path.join(os.getcwd(), "cache_fundamentales.json")
RUTA_CONFIG = os.path.join(os.getcwd(), "config_filtros.json")

VALORES_POR_DEFECTO = {
    "precio_min": 0.5,
    "precio_max": 20.0,
    "gap_min": 3.0,
    "gap_max": 50.0,
    "flotacion_max": 20_000_000,
    "volumen_min": 15_000,
    "intervalo_refresco": 5,
    # Valores técnicos usados por el motor compartido/diagnóstico.
    # Antes faltaban aquí y filtrar_resultados() podía lanzar KeyError
    # con self.filtros_dueno, abortando el ciclo antes de publicar el diagnóstico.
    "cruce_ema": "Hacia arriba",
    "macd": "Positivo",
    "orden": "Actualizado",
    "top_n": 10,
    "sesion": "TODO EL MERCADO",
    "timeframe": "1m",
    "ema_dist_max": 0.0,
    "rsi_min": 0.0,
    "rsi_max": 100.0,
    # Pestañas EMA20/50/200: estado (arriba/abajo) + condición de entrada.
    "ema20_estado": "Neutro", "ema50_estado": "Neutro", "ema200_estado": "Neutro",
    "ema20_cond": "Naciendo", "ema50_cond": "Ninguna", "ema200_cond": "Ninguna",
    "ema20_dist": 0.5, "ema50_dist": 0.5, "ema200_dist": 0.5,
    # Detector de swing: zona inferior -> cruce EMA20 -> primer toque EMA50/EMA200.
    "swing_activo": False,
    "swing_origen": "Bollinger inferior + debajo de EMA20",
    "swing_objetivo": "EMA50 o EMA200",
    "swing_ventana": 10,
    "swing_tolerancia": 1.0,
    "swing_origen_tolerancia": 1.0,
    "swing_multitimeframe": False,
    "swing_tfs": "1d,1w,1mo",
    # Filtros opcionales: el usuario decide cuáles activar.
    "gap_activo": False,
    "flotacion_activa": False,
    "volumen_activo": False,
    "ema20_activa": False,
}


def cargar_config():
    """Filtros personales por defecto; no se usan para controlar el motor compartido."""
    return VALORES_POR_DEFECTO.copy()


def cargar_config_motor_compartido():
    """Pool técnico común: amplio, sin filtros personales de ningún usuario."""
    d = VALORES_POR_DEFECTO.copy()
    d.update({
        "precio_min": BASE_PRECIO_MIN, "precio_max": BASE_PRECIO_MAX,
        "gap_min": BASE_GAP_MIN, "gap_max": BASE_GAP_MAX,
        "flotacion_max": BASE_FLOTACION_MAX, "volumen_min": 0,
        "macd": "No exigir",
        "ema20_estado": "Neutro", "ema50_estado": "Neutro", "ema200_estado": "Neutro",
        "ema20_cond": "Ninguna", "ema50_cond": "Ninguna", "ema200_cond": "Ninguna",
        "ema20_dist": 0.0, "ema50_dist": 0.0, "ema200_dist": 0.0,
        "gap_activo": False, "flotacion_activa": False, "volumen_activo": False,
        "ema20_activa": False, "f_gap_on": "OFF", "f_float_on": "OFF",
        "f_vol_on": "OFF", "ema20_on": "OFF",
        "rsi_min": 0.0, "rsi_max": 100.0, "swing_activo": False, "timeframe": "1m",
    })
    return d


def cargar_horario_guardado():
    """Horario automático guardado en disco (sobrevive a reinicios de la app).
    Si no hay nada guardado, usa los valores por defecto del código."""
    try:
        with open(RUTA_CONFIG, "r", encoding="utf-8") as f:
            d = json.load(f)
        return int(d["hora_inicio_auto_min"]), int(d["hora_fin_auto_min"])
    except Exception:
        return HORA_AUTO_INICIO_ET * 60, HORA_AUTO_FIN_ET * 60


def guardar_horario_en_disco(inicio_min, fin_min):
    """Guarda el horario automático en disco para que sobreviva a reinicios de la app."""
    try:
        with open(RUTA_CONFIG, "r", encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        d = {}
    try:
        d["hora_inicio_auto_min"] = int(inicio_min)
        d["hora_fin_auto_min"] = int(fin_min)
        with open(RUTA_CONFIG, "w", encoding="utf-8") as f:
            json.dump(d, f)
    except Exception:
        pass


def cargar_estado_motor_guardado():
    """Estado ON/OFF del motor central administrado por ADMIN."""
    try:
        with open(RUTA_CONFIG, "r", encoding="utf-8") as f:
            d = json.load(f)
        return bool(d.get("motor_central_encendido", True))
    except Exception:
        return True


def guardar_estado_motor_en_disco(encendido):
    """Guarda el ON/OFF central para que no se pierda al reiniciar la app."""
    try:
        with open(RUTA_CONFIG, "r", encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        d = {}
    try:
        d["motor_central_encendido"] = bool(encendido)
        with open(RUTA_CONFIG, "w", encoding="utf-8") as f:
            json.dump(d, f)
    except Exception:
        pass


# ==========================================
# 💳 MEMBRESÍAS Y COBRO SIMULADO (MODO PRUEBA)
# ==========================================
# Estos precios son únicamente de prueba. No hay cobro real ni tarjeta.
PRECIO_MENSUAL_USD = 28.00
PRECIO_ANUAL_USD = 270.00
PRECIO_MENSUAL_ROBOT_USD = 38.00
PRECIO_ANUAL_ROBOT_USD = 370.00
DIAS_PRUEBA_GRATIS = 30
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
    """Crea una prueba de 1 mes una sola vez por usuario."""
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
    """Activa una suscripción simulada; NO procesa dinero real."""
    data = _leer_licencias_simuladas()
    clave = str(user_id)
    actual = data.get(clave) or {"user_id": clave}
    inicio = _ahora_utc()
    if plan in {"MENSUAL", "MENSUAL_SCANNER"}:
        dias = 30
        precio = PRECIO_MENSUAL_USD
        nombre_plan = "SCANNER MENSUAL"
        incluye_robot = False
    elif plan in {"ANUAL", "ANUAL_SCANNER"}:
        dias = 365
        precio = PRECIO_ANUAL_USD
        nombre_plan = "SCANNER ANUAL"
        incluye_robot = False
    elif plan == "MENSUAL_ROBOT":
        dias = 30
        precio = PRECIO_MENSUAL_ROBOT_USD
        nombre_plan = "SCANNER + ROBOT MENSUAL"
        incluye_robot = True
    elif plan == "ANUAL_ROBOT":
        dias = 365
        precio = PRECIO_ANUAL_ROBOT_USD
        nombre_plan = "SCANNER + ROBOT ANUAL"
        incluye_robot = True
    else:
        return False, "Plan no válido."
    # En simulación, cada activación extiende desde hoy o desde el vencimiento vigente.
    base = _parse_iso(actual.get("vencimiento")) or inicio
    if base < inicio:
        base = inicio
    actual.update({
        "plan": nombre_plan,
        "incluye_robot": incluye_robot,
        "estado": "ACTIVO",
        "inicio": _iso(inicio),
        "vencimiento": _iso(base + timedelta(days=dias)),
        "origen": "pago_simulado",
        "ultimo_pago_simulado_usd": precio,
    })
    data[clave] = actual
    ok = _guardar_licencias_simuladas(data)
    return ok, ("Plan activado en modo simulación." if ok else "No se pudo guardar la licencia simulada.")

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
    if venc:
        venc_txt = venc.astimezone(ET).strftime("%d/%m/%Y %H:%M ET")
    else:
        venc_txt = "—"
    return estado, venc_txt

# ==========================================
# 🔐 AUTENTICACIÓN — ADMIN + USUARIOS
# ==========================================
# ADMIN:
#   - Mantiene el acceso actual mediante ADMIN_TOKEN.
# USUARIOS:
#   - Se registran con email + contraseña.
#   - Inician/cerran sesión desde la propia aplicación.
#   - Sus credenciales son gestionadas por Supabase Auth.
#
# Secrets necesarios para el registro/login de usuarios:
#   SUPABASE_URL
#   SUPABASE_ANON_KEY
#
# El scanner, las API keys y los secrets del servidor NO se entregan
# al usuario desde este módulo.

def obtener_tokens():
    """Tokens/licencias antiguas del sistema. Se conservan para compatibilidad."""
    for clave in ("tokens_autorizados", "TOKENS_AUTORIZADOS"):
        try:
            if clave in st.secrets:
                return dict(st.secrets[clave])
        except Exception:
            pass
    return {}


def verificar_token(token_usuario):
    """Valida el acceso administrativo usando ADMIN_TOKEN, ADMIN_TOKENS o tokens legacy."""
    token_usuario = str(token_usuario or "").strip()
    if not token_usuario:
        return False, "INVALIDO"

    # Aceptar tanto ADMIN_TOKEN como ADMIN_TOKENS para que la pantalla de
    # administrador use exactamente la misma fuente de secretos que el resto
    # del control de acceso.
    candidatos = []
    try:
        admin_token = str(st.secrets.get("ADMIN_TOKEN", "")).strip()
        if admin_token:
            candidatos.append((admin_token, "2099-01-01"))
    except Exception:
        pass
    try:
        raw = st.secrets.get("ADMIN_TOKENS", "")
        valores = raw if isinstance(raw, (list, tuple, set)) else str(raw).split(",")
        for valor in valores:
            valor = str(valor).strip()
            if valor:
                candidatos.append((valor, "2099-01-01"))
    except Exception:
        pass

    vistos = set()
    for token, vencimiento in candidatos:
        if token in vistos:
            continue
        vistos.add(token)
        if token_usuario == token:
            return True, vencimiento

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


def _supabase_config():
    """Obtiene la URL y la anon key de Supabase desde Streamlit Secrets."""
    url = str(st.secrets.get("SUPABASE_URL", "")).strip().rstrip("/")
    key = str(st.secrets.get("SUPABASE_ANON_KEY", "")).strip()
    return url, key


def supabase_auth_request(endpoint, payload):
    """
    Llama directamente a Supabase Auth REST API usando requests.
    No requiere instalar el paquete supabase.
    """
    url, key = _supabase_config()
    if not url or not key:
        return None, "Faltan SUPABASE_URL y/o SUPABASE_ANON_KEY en Streamlit Secrets."

    try:
        respuesta = requests.post(
            f"{url}/auth/v1/{endpoint}",
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=20,
        )

        try:
            data = respuesta.json()
        except Exception:
            data = {}

        if respuesta.ok:
            return data, None

        mensaje = (
            data.get("msg")
            or data.get("message")
            or data.get("error_description")
            or data.get("error")
        )
        if not mensaje:
            try:
                detalle = respuesta.text.strip()
            except Exception:
                detalle = ""
            mensaje = detalle or "No se pudo completar la operación."
        codigo = str(data.get("code", "")).strip() if isinstance(data, dict) else ""
        sufijo = f" [{codigo}]" if codigo else ""
        return None, f"{mensaje}{sufijo} (HTTP {respuesta.status_code})"

    except Exception as e:
        return None, f"Error de conexión con el servicio de autenticación: {e}"


def registrar_usuario(email, password):
    """Crea una cuenta de usuario mediante Supabase Auth."""
    email = str(email).strip().lower()

    if not email or "@" not in email:
        return None, "Introduce un correo electrónico válido."

    if len(password) < 8:
        return None, "La contraseña debe tener al menos 8 caracteres."

    # No forzamos una URL fija de Streamlit. Si Supabase requiere
    # confirmación por correo, utiliza la Site URL / Redirect URLs configurada
    # en Supabase. Así un cambio de dominio no rompe el registro.
    data, error = supabase_auth_request(
        "signup",
        {
            "email": email,
            "password": password,
        },
    )

    if error:
        return None, error

    return data, None


def iniciar_sesion_usuario(email, password):
    """Inicia sesión con email y contraseña mediante Supabase Auth."""
    email = str(email).strip().lower()

    if not email or not password:
        return None, "Introduce tu correo y contraseña."

    data, error = supabase_auth_request(
        "token?grant_type=password",
        {
            "email": email,
            "password": password,
        },
    )

    if error:
        return None, error

    return data, None


def solicitar_recuperacion(email):
    """Solicita un código OTP de recuperación por correo.

    No usamos el enlace de un solo uso de Supabase porque algunos clientes
    de correo/seguridad pueden abrirlo automáticamente y consumirlo antes
    de que el usuario lo pulse.
    """
    email = str(email).strip().lower()

    if not email or "@" not in email:
        return None, "Introduce un correo electrónico válido."

    data, error = supabase_auth_request(
        "recover",
        {"email": email},
    )

    if error:
        return None, error

    return data, None


def verificar_codigo_recuperacion(email, codigo):
    """Verifica el OTP de recuperación y obtiene una sesión temporal."""
    email = str(email).strip().lower()
    codigo = "".join(str(codigo).split())

    if not email or "@" not in email:
        return None, "Introduce un correo electrónico válido."

    # Supabase usa OTP numérico para este flujo. Aceptamos 6-8 dígitos para
    # mantener compatibilidad con las variantes de plantilla documentadas.
    if not codigo.isdigit() or len(codigo) not in (6, 8):
        return None, "El código debe tener 6 u 8 dígitos."

    data, error = supabase_auth_request(
        "verify",
        {
            "email": email,
            "token": codigo,
            "type": "recovery",
        },
    )

    if error:
        return None, error

    if not data or not data.get("access_token"):
        return None, "Supabase no devolvió una sesión válida de recuperación."

    return data, None


def actualizar_password_recuperacion(access_token, nueva_password):
    """Cambia la contraseña usando la sesión temporal obtenida con el OTP."""
    if not access_token:
        return None, "La sesión de recuperación no es válida. Solicita un nuevo código."

    if len(str(nueva_password)) < 8:
        return None, "La contraseña debe tener al menos 8 caracteres."

    url, key = _supabase_config()
    if not url or not key:
        return None, "Faltan SUPABASE_URL y/o SUPABASE_ANON_KEY en Streamlit Secrets."

    try:
        respuesta = requests.put(
            f"{url}/auth/v1/user",
            headers={
                "apikey": key,
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json={"password": str(nueva_password)},
            timeout=20,
        )

        try:
            data = respuesta.json()
        except Exception:
            data = {}

        if respuesta.ok:
            return data, None

        mensaje = (
            data.get("msg")
            or data.get("message")
            or data.get("error_description")
            or data.get("error")
            or "No se pudo cambiar la contraseña."
        )
        return None, str(mensaje)

    except Exception as e:
        return None, f"Error de conexión con el servicio de autenticación: {e}"


def limpiar_recuperacion():
    """Elimina cualquier sesión temporal de recuperación."""
    for clave in (
        "recovery_email",
        "recovery_access_token",
        "recovery_codigo_verificado",
    ):
        st.session_state.pop(clave, None)

# Sesiones persistentes para que un refresh/navegación de la carátula no obligue
# al usuario a volver a escribir sus credenciales. El identificador que viaja
# en la URL es aleatorio y no contiene la contraseña ni el token de admin.
# La información sensible permanece únicamente en memoria del servidor.
@st.cache_resource
def _almacen_sesiones_persistentes():
    return {}

_PERSISTENT_AUTH_SESSIONS = _almacen_sesiones_persistentes()

def _admin_tokens_para_sesion():
    """Obtiene los tokens admin configurados sin depender de variables definidas más abajo."""
    candidatos = []
    try:
        t = str(st.secrets.get("ADMIN_TOKEN", "")).strip()
        if t:
            candidatos.append((t, "2099-01-01"))
    except Exception:
        pass
    try:
        raw = st.secrets.get("ADMIN_TOKENS", "")
        vals = raw if isinstance(raw, (list, tuple, set)) else str(raw).split(",")
        for x in vals:
            x = str(x).strip()
            if x:
                candidatos.append((x, "2099-01-01"))
    except Exception:
        pass
    try:
        legacy = obtener_tokens()
        for token, venc in legacy.items():
            token = str(token).strip()
            if token:
                candidatos.append((token, str(venc)))
    except Exception:
        pass
    vistos = set()
    return [(t, v) for t, v in candidatos if not (t in vistos or vistos.add(t))]


def _crear_ticket_admin(token, fecha_vencimiento="2099-01-01"):
    """Crea un ticket opaco firmado; nunca coloca el token admin en la URL."""
    ts = str(int(time.time()))
    secreto = f"{token}|{ts}|TradeScannerAdminSession".encode("utf-8")
    firma = hmac.new(token.encode("utf-8"), secreto, hashlib.sha256).hexdigest()
    return f"adm.{ts}.{firma}"


def _validar_ticket_admin(ticket):
    try:
        partes = str(ticket).split(".")
        if len(partes) != 3 or partes[0] != "adm":
            return None
        ts = int(partes[1])
        if abs(time.time() - ts) > 60 * 60 * 24 * 30:
            return None
        firma_recibida = partes[2]
        for token, venc in _admin_tokens_para_sesion():
            esperado = hmac.new(
                token.encode("utf-8"),
                f"{token}|{ts}|TradeScannerAdminSession".encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            if hmac.compare_digest(firma_recibida, esperado):
                if venc != "2099-01-01":
                    try:
                        if datetime.now().date() > datetime.strptime(venc, "%Y-%m-%d").date():
                            return None
                    except Exception:
                        return None
                return token, venc
    except Exception:
        return None
    return None


def _crear_sesion_persistente(tipo, datos):
    if tipo == "admin":
        token = str((datos or {}).get("token", "")).strip()
        venc = str((datos or {}).get("fecha_vencimiento", "2099-01-01"))
        sid = _crear_ticket_admin(token, venc) if token else secrets.token_urlsafe(32)
    else:
        sid = secrets.token_urlsafe(32)
    _PERSISTENT_AUTH_SESSIONS[sid] = {"tipo": tipo, "datos": dict(datos or {})}
    try:
        _email = str((datos or {}).get("email", "")).strip().lower()
        if tipo == "usuario" and _email:
            # Este helper se define antes del bloque de configuración de usuario.
            # Nunca debe fallar el login/registro por una referencia adelantada.
            _cfg_store = globals().get("_ULTIMA_CONFIG_USUARIOS", {})
            _cfg = _cfg_store.get(_email) if isinstance(_cfg_store, dict) else None
            if isinstance(_cfg, dict) and _cfg:
                _PERSISTENT_AUTH_SESSIONS[sid]["config"] = dict(_cfg)
    except Exception:
        pass
    return sid


def _restaurar_sesion_persistente():
    try:
        sid = str(st.query_params.get("auth_session", "")).strip()
        if not sid:
            return False
        ses = _PERSISTENT_AUTH_SESSIONS.get(sid)
        if not ses and sid.startswith("adm."):
            validado = _validar_ticket_admin(sid)
            if validado:
                token, venc = validado
                ses = {"tipo": "admin", "datos": {"token": token, "fecha_vencimiento": venc}}
                _PERSISTENT_AUTH_SESSIONS[sid] = ses
        if not ses:
            return False
        tipo = ses.get("tipo")
        datos = ses.get("datos", {})
        if tipo == "admin":
            token = str(datos.get("token", ""))
            if not token:
                return False
            st.session_state["token_verificado"] = token
            st.session_state["fecha_vencimiento"] = datos.get("fecha_vencimiento", "2099-01-01")
            st.session_state["tipo_acceso"] = "admin"
            return True
        if tipo == "usuario":
            st.session_state["usuario_auth"] = dict(datos)
            st.session_state["tipo_acceso"] = "usuario"
            return bool(st.session_state["usuario_auth"].get("email") or st.session_state["usuario_auth"].get("user_id"))
    except Exception:
        return False
    return False


def cerrar_sesion():
    """Limpia la sesión local y la sesión persistente del navegador."""
    try:
        sid = str(st.query_params.get("auth_session", "")).strip()
        if sid:
            _PERSISTENT_AUTH_SESSIONS.pop(sid, None)
        st.query_params.pop("auth_session", None)
    except Exception:
        pass
    for clave in (
        "usuario_auth",
        "token_verificado",
        "fecha_vencimiento",
        "tipo_acceso",
        # No dejar credenciales/configuración del broker en una sesión
        # que pueda ser reutilizada por otro usuario.
        "bk_api_key",
        "bk_api_secret",
        "bk_nombre",
        "bk_puente",
        "bk_cargado",
        "_bk_guardado",
        "bk_colores",
        "usar_api_broker_dashboard",
    ):
        st.session_state.pop(clave, None)
    # COLORES_LAYOUT_DEFECTO no está definido en este archivo; se usa un rango seguro.
    for _i in range(len(globals().get("COLORES_LAYOUT_DEFECTO", range(20)))):
        st.session_state.pop(f"bk_wh_{_i}", None)


def _guardar_usuario_auth(data, tipo="usuario"):
    """Guarda únicamente los datos necesarios para la sesión actual."""
    usuario = data.get("user") or {}

    # En algunos flujos de Supabase el user puede no venir completo,
    # pero sí viene el access_token.
    st.session_state["usuario_auth"] = {
        "user_id": usuario.get("id", ""),
        "email": usuario.get("email", ""),
        "access_token": data.get("access_token", ""),
        "refresh_token": data.get("refresh_token", ""),
    }
    st.session_state["tipo_acceso"] = tipo


def pantalla_autenticacion():
    """
    Pantalla inicial:
      1) Iniciar sesión
      2) Registrarse
      3) Acceso administrador
    """
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at 50% 0%, rgba(212,175,55,.10), transparent 35%),
                #030303 !important;
        }
        .auth-card {
            position: relative;
            box-sizing: border-box;
            width: min(520px, calc(100% - 24px));
            max-width: 520px;
            margin: 24px auto 20px auto;
            background: #0d1118;
            border: 1px solid #2a3348;
            border-radius: 16px;
            padding: 24px 18px 22px 18px;
            box-shadow: 0 18px 50px rgba(0,0,0,.35);
            overflow: hidden;
            isolation: isolate;
        }
        .auth-title {
            box-sizing: border-box;
            width: 100%;
            color: #d4af37;
            font-family: sans-serif;
            font-weight: 800;
            font-size: clamp(18px, 5vw, 26px);
            line-height: 1.2;
            text-align: center;
            margin: 0 auto 6px auto;
            padding: 0;
            overflow-wrap: anywhere;
            word-break: break-word;
        }
        .auth-subtitle {
            box-sizing: border-box;
            width: 100%;
            color: #8e96a3;
            text-align: center;
            font-size: 11px;
            letter-spacing: 2px;
            margin-bottom: 14px;
        }
        .auth-offer {
            box-sizing: border-box;
            width: 100%;
            margin: 8px auto 0 auto;
            padding: 11px 10px 10px 10px;
            border: 1px solid rgba(212,175,55,.55);
            border-radius: 10px;
            background: linear-gradient(180deg, rgba(212,175,55,.10), rgba(212,175,55,.035));
            text-align: center;
            color: #f3f3f3;
        }
        .auth-offer-title {
            color: #f2d675;
            font-size: 13px;
            font-weight: 800;
            letter-spacing: .8px;
            margin-bottom: 7px;
        }
        .auth-offer-line {
            font-size: 12px;
            line-height: 1.55;
            color: #d9dee7;
        }
        .auth-offer-free { color: #37c77a; font-weight: 800; }
        .auth-offer-price { color: #f2d675; font-weight: 800; }
        @media (max-width: 640px) {
            .auth-card {
                width: calc(100% - 18px);
                margin-top: 14px;
                padding: 20px 12px 18px 12px;
                border-radius: 14px;
            }
            .auth-title {
                font-size: 19px;
                line-height: 1.18;
            }
            .auth-subtitle {
                font-size: 10px;
                margin-bottom: 10px;
            }
            .auth-offer {
                padding: 10px 7px 9px 7px;
            }
            .auth-offer-title {
                font-size: 12px;
            }
            .auth-offer-line {
                font-size: 11px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div style="box-sizing:border-box;width:min(520px,calc(100% - 18px));max-width:520px;margin:14px auto 20px auto;padding:18px 14px 14px;background:#0d1118;border:1px solid #2a3348;border-radius:16px;box-shadow:0 18px 50px rgba(0,0,0,.35);overflow:hidden;text-align:center;">
            <div style="box-sizing:border-box;width:100%;margin:0;padding:0 2px;color:#d4af37;font-family:Arial,sans-serif;font-weight:800;font-size:clamp(18px,5vw,26px);line-height:1.2;text-align:center;overflow-wrap:anywhere;word-break:break-word;">TRADE SCANNER INSTITUTIONAL</div>
            <div style="width:100%;margin:5px 0 11px;color:#8e96a3;font-family:Arial,sans-serif;font-size:10px;letter-spacing:2px;text-align:center;">SCANNER</div>
            <div style="box-sizing:border-box;width:100%;margin:0;padding:10px 8px 9px;border:1px solid rgba(212,175,55,.55);border-radius:10px;background:linear-gradient(180deg,rgba(212,175,55,.10),rgba(212,175,55,.035));text-align:center;color:#f3f3f3;">
                <div style="color:#f2d675;font-family:Arial,sans-serif;font-size:12px;font-weight:800;letter-spacing:.8px;margin-bottom:6px;">🎁 OFERTA DE LANZAMIENTO</div>
                <div style="font-family:Arial,sans-serif;font-size:11px;line-height:1.65;color:#d9dee7;">Prueba <span style="color:#37c77a;font-weight:800;">1 MES GRATIS</span></div>
                <div style="font-family:Arial,sans-serif;font-size:11px;line-height:1.65;color:#d9dee7;margin-top:3px;">Solo Scanner: <span style="color:#f2d675;font-weight:800;">$28/mes</span> · <span style="color:#f2d675;font-weight:800;">$270/año</span></div>
                <div style="font-family:Arial,sans-serif;font-size:11px;line-height:1.65;color:#d9dee7;">Scanner + Robot: <span style="color:#f2d675;font-weight:800;">$38/mes</span> · <span style="color:#f2d675;font-weight:800;">$370/año</span></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Diagnóstico visible de configuración: no muestra la clave, solo confirma
    # si Streamlit recibió los dos secretos necesarios para Supabase.
    _sb_url, _sb_key = _supabase_config()
    if not _sb_url or not _sb_key:
        st.error(
            "⚠️ El registro/login no está configurado todavía en este despliegue: "
            "faltan SUPABASE_URL y/o SUPABASE_ANON_KEY en Streamlit Secrets."
        )
    else:
        st.caption("🔐 Autenticación de usuarios: Supabase configurado")

    # Una sola ventana para todos.
    # El acceso de administrador está dentro de la misma pantalla y
    # requiere el token secreto configurado en Streamlit Secrets.
    # No se utiliza una segunda URL ni un parámetro especial de administrador.
    st.button(
        "← Volver al scanner",
        key="ts_volver_auth",
        on_click=lambda: st.session_state.update(mostrar_auth=False),
    )
    tab_login, tab_registro, tab_admin = st.tabs(
        ["🔐 Iniciar sesión", "📝 Registrarse", "👑 Administrador"]
    )

    with tab_login:
        st.markdown("### Acceso de usuario")
        with st.form("form_login_usuario"):
            email = st.text_input(
                "Correo electrónico",
                placeholder="tu@email.com",
                key="login_email",
            )
            password = st.text_input(
                "Contraseña",
                type="password",
                key="login_password",
            )
            entrar = st.form_submit_button(
                "🚀 INICIAR SESIÓN",
                width="stretch",
            )

        if entrar:
            data, error = iniciar_sesion_usuario(email, password)
            if error:
                st.error(f"❌ {error}")
            else:
                cerrar_sesion()  # evita que una sesión admin previa siga mandando sobre el usuario
                _guardar_usuario_auth(data, tipo="usuario")
                _u = data.get("user") or {}
                crear_prueba_usuario(_u.get("id", ""), _u.get("email", email))
                _sid = _crear_sesion_persistente("usuario", st.session_state["usuario_auth"])
                st.query_params["auth_session"] = _sid
                st.session_state["mostrar_auth"] = False
                st.query_params.pop("auth", None)
                st.rerun()

        with st.expander("🔑 ¿Olvidaste tu contraseña?", expanded=bool(st.session_state.get("recovery_email"))):
            st.caption("Te enviaremos un código de recuperación por correo. No necesitas abrir ningún enlace.")

            email_recuperacion = st.text_input(
                "Correo de tu cuenta",
                value=st.session_state.get("recovery_email", st.session_state.get("login_email", "")),
                placeholder="tu@email.com",
                key="recovery_email_ui",
            )
            st.session_state["recovery_email"] = str(email_recuperacion).strip().lower()

            if not st.session_state.get("recovery_codigo_verificado"):
                with st.form("form_recuperar_password"):
                    enviar_recuperacion = st.form_submit_button(
                        "📩 ENVIAR CÓDIGO DE RECUPERACIÓN",
                        width="stretch",
                    )

                if enviar_recuperacion:
                    _, error_recuperacion = solicitar_recuperacion(email_recuperacion)
                    if error_recuperacion:
                        st.error(f"❌ {error_recuperacion}")
                    else:
                        st.success(
                            "✅ Si el correo está registrado, recibirás un código. "
                            "Revisa también la carpeta de spam."
                        )
                        st.session_state["recovery_codigo_enviado"] = True

                if st.session_state.get("recovery_codigo_enviado"):
                    with st.form("form_verificar_codigo_recuperacion"):
                        codigo_recuperacion = st.text_input(
                            "Código recibido por correo",
                            placeholder="Ej.: 123456",
                            max_chars=8,
                            key="recovery_otp",
                        )
                        verificar_codigo = st.form_submit_button(
                            "🔐 VERIFICAR CÓDIGO",
                            width="stretch",
                        )

                    if verificar_codigo:
                        data_recuperacion, error_verificacion = verificar_codigo_recuperacion(
                            email_recuperacion,
                            codigo_recuperacion,
                        )
                        if error_verificacion:
                            st.error(f"❌ {error_verificacion}")
                        else:
                            st.session_state["recovery_access_token"] = data_recuperacion.get("access_token", "")
                            st.session_state["recovery_codigo_verificado"] = True
                            st.success("✅ Código verificado. Ahora puedes crear una contraseña nueva.")
                            st.rerun()

            if st.session_state.get("recovery_codigo_verificado"):
                st.info("🔓 Identidad verificada. Crea tu nueva contraseña.")
                with st.form("form_nueva_password_recuperacion"):
                    nueva_password_recuperacion = st.text_input(
                        "Nueva contraseña",
                        type="password",
                        key="recovery_new_password",
                    )
                    repetir_password_recuperacion = st.text_input(
                        "Repetir nueva contraseña",
                        type="password",
                        key="recovery_new_password_2",
                    )
                    cambiar_password = st.form_submit_button(
                        "💾 CAMBIAR CONTRASEÑA",
                        width="stretch",
                    )

                if cambiar_password:
                    if nueva_password_recuperacion != repetir_password_recuperacion:
                        st.error("❌ Las contraseñas no coinciden.")
                    else:
                        _, error_password = actualizar_password_recuperacion(
                            st.session_state.get("recovery_access_token", ""),
                            nueva_password_recuperacion,
                        )
                        if error_password:
                            st.error(f"❌ {error_password}")
                        else:
                            limpiar_recuperacion()
                            st.success("✅ Contraseña cambiada correctamente. Ya puedes iniciar sesión con tu nueva contraseña.")
                            st.rerun()

    with tab_registro:
        st.markdown("### Crear cuenta")
        st.caption("Crea tu acceso personal al scanner.")

        with st.form("form_registro_usuario"):
            nuevo_email = st.text_input(
                "Correo electrónico",
                placeholder="tu@email.com",
                key="registro_email",
            )
            nueva_password = st.text_input(
                "Contraseña",
                type="password",
                key="registro_password",
            )
            repetir_password = st.text_input(
                "Repetir contraseña",
                type="password",
                key="registro_password_2",
            )
            registrar = st.form_submit_button(
                "📝 CREAR CUENTA",
                width="stretch",
            )

        if registrar:
            if nueva_password != repetir_password:
                st.error("❌ Las contraseñas no coinciden.")
            else:
                data, error = registrar_usuario(nuevo_email, nueva_password)

                if error:
                    st.error(f"❌ {error}")
                else:
                    # Si Supabase devuelve access_token, la sesión puede
                    # iniciarse inmediatamente. Si no, normalmente significa
                    # que está activada la confirmación por correo.
                    if data and data.get("access_token"):
                        cerrar_sesion()
                        _guardar_usuario_auth(data, tipo="usuario")
                        _u = data.get("user") or {}
                        crear_prueba_usuario(_u.get("id", ""), _u.get("email", nuevo_email))
                        _sid = _crear_sesion_persistente("usuario", st.session_state["usuario_auth"])
                        st.query_params["auth_session"] = _sid
                        st.session_state["mostrar_auth"] = False
                        st.query_params.pop("auth", None)
                        st.success("✅ Cuenta creada. Tu prueba gratuita de 1 mes está activa.")
                        st.rerun()
                    else:
                        st.success(
                            "✅ Cuenta creada. Revisa tu correo para confirmar la cuenta. "
                            "Al iniciar sesión se activará tu prueba gratuita de 1 mes."
                        )

    with tab_admin:
            st.markdown("### Acceso del administrador")
            st.caption("Este acceso conserva el sistema de token del propietario.")

            with st.form("form_admin_token"):
                token_ingresado = st.text_input(
                    "Token de administrador",
                    type="password",
                    key="admin_token_login",
                )
                entrar_admin = st.form_submit_button(
                    "👑 VALIDAR ACCESO",
                    width="stretch",
                )

            if entrar_admin:
                token_limpio = token_ingresado.strip()
                es_valido, estado = verificar_token(token_limpio)

                if es_valido:
                    cerrar_sesion()
                    st.session_state["token_verificado"] = token_limpio
                    st.session_state["fecha_vencimiento"] = estado
                    st.session_state["tipo_acceso"] = "admin"
                    # Salir de la pantalla de autenticación antes del rerun.
                    # Si no se limpia este estado, el rerun vuelve a mostrar
                    # el formulario y parece que "VALIDAR ACCESO" no funciona.
                    st.session_state["mostrar_auth"] = False
                    _sid = _crear_sesion_persistente("admin", {"token": token_limpio, "fecha_vencimiento": estado})
                    st.query_params["auth_session"] = _sid
                    st.query_params.pop("auth", None)
                    st.rerun()
                elif estado == "EXPIRADO":
                    st.error("🔒 Token expirado.")
                elif estado == "FORMATO":
                    st.error("❌ Error de configuración del token.")
                else:
                    st.error("❌ Token no válido. Acceso denegado.")

    st.stop()


# =========================================================
# 🌐 MODO PÚBLICO / AUTENTICACIÓN
# =========================================================
# auth_session solo es respaldo de recarga completa; durante un rerun normal
# la identidad permanece en st.session_state.
def _ts_aplicar_evento_ui():
    """Recibe lo que hizo el usuario en el cuadro gris (filtros, EMAs, idioma, refresh...)
    y lo escribe en los parametros de la sesion ANTES de calcular nada."""
    try:
        ev = st.session_state.get("ts_scanner_ui")
        if not isinstance(ev, dict):
            return
        eid = str(ev.get("id", ""))
        if not eid or eid == st.session_state.get("_ts_evt_visto"):
            return
        st.session_state["_ts_evt_visto"] = eid
        from urllib.parse import parse_qsl
        pares = dict(parse_qsl(str(ev.get("q", "")), keep_blank_values=True))
        pares.pop("_ts", None)
        for _k, _v in pares.items():
            if _k == "auth_session" and not _v:
                continue
            if str(st.query_params.get(_k, "")) != _v:
                st.query_params[_k] = _v
        if "c_active" in pares:
            # Solo se consume como orden de motor después de verificar ES_ADMIN.
            st.session_state["_admin_motor_evento"] = str(pares.get("c_active", ""))
        # Una accion del usuario siempre gana a un auto-refresh que coincida en el tiempo.
        st.session_state["_ts_rerun_auto"] = False
    except Exception as _e_ev:
        print(f"⚠️ No se pudo aplicar el evento del cuadro gris: {_e_ev}")


_ts_aplicar_evento_ui()

PUBLIC_PREVIEW = (
    "token_verificado" not in st.session_state
    and "usuario_auth" not in st.session_state
)

# Visitantes: solo lectura. No aceptamos configuración personal enviada por URL.
if PUBLIC_PREVIEW:
    # Limpieza atómica de filtros de usuario en la primera carga pública.
    # Eliminar cada parámetro con .pop() por separado puede producir una
    # cascada de actualizaciones de URL durante un refresh completo.
    _PUBLIC_QUERY_KEYS = {
        "c_active","c_start","c_end","c_lang","c_wnd","c_broker","c_url","refresh_sec",
        "f_price_min","f_price_max","f_gap_min","f_gap_max","f_float_max","f_vol",
        "f_ema","f_mac","f_order","timeframe","technical_timeframe","ema_dist_max",
        "rsi_min","rsi_max","ema20_estado","ema50_estado","ema200_estado",
        "ema20_cond","ema50_cond","ema200_cond","ema20_dist","ema50_dist","ema200_dist",
        "f_gap_on","f_float_on","f_vol_on","ema20_on","swing_activo","swing_origen",
        "swing_objetivo","swing_ventana","swing_tolerancia","swing_origen_tolerancia",
        "swing_multitimeframe","swing_tfs",
    }
    try:
        _qp_actual_publico = dict(st.query_params)
        _qp_limpio_publico = {
            _k: _v for _k, _v in _qp_actual_publico.items()
            if _k not in _PUBLIC_QUERY_KEYS
        }
        if len(_qp_limpio_publico) != len(_qp_actual_publico):
            try:
                st.query_params.from_dict(_qp_limpio_publico)
            except Exception:
                st.query_params.clear()
                st.query_params.update(_qp_limpio_publico)
    except Exception:
        pass
AUTH_REQUESTED = str(st.query_params.get("auth", "0")).lower() in ("1", "true", "yes")
LOGOUT_REQUESTED = str(st.query_params.get("logout", "0")).lower() in ("1", "true", "yes")

# Estado nativo de Streamlit: no depende de iframe, target, window.open ni
# navegación del navegador.
if "mostrar_auth" not in st.session_state:
    st.session_state["mostrar_auth"] = False

if LOGOUT_REQUESTED:
    cerrar_sesion()
    st.session_state["mostrar_auth"] = False
    try:
        st.query_params.clear()
    except Exception:
        pass
    st.rerun()

# Si llega ?auth=1 desde una versión anterior, se convierte una sola vez al
# estado nativo y se elimina el parámetro.
if AUTH_REQUESTED:
    st.session_state["mostrar_auth"] = True
    try:
        st.query_params.pop("auth", None)
    except Exception:
        pass

# Si hay un pedido explícito de autenticación, NO restauramos una sesión vieja
# primero. Esto garantiza que REGISTRO/LOGIN siempre sea accesible.
# Cuando el usuario pide explícitamente REGISTRO / LOGIN, la pantalla de
# autenticación debe abrirse incluso si Streamlit restauró una sesión anterior.
# Esto evita que la restauración automática bloquee el botón de acceso.
if st.session_state.get("mostrar_auth"):
    pantalla_autenticacion()
    st.stop()

# Restauración normal de sesión solamente cuando no se está mostrando Auth.
if "token_verificado" not in st.session_state and "usuario_auth" not in st.session_state:
    _restaurar_sesion_persistente()
    PUBLIC_PREVIEW = (
        "token_verificado" not in st.session_state
        and "usuario_auth" not in st.session_state
    )

# =========================================================
# IDENTIDAD ACTIVA
# =========================================================
if "token_verificado" in st.session_state:
    TOKEN_ACTIVO = st.session_state["token_verificado"]
    FECHA_VENCIMIENTO_LICENCIA = st.session_state.get(
        "fecha_vencimiento", "2099-01-01"
    )
    TIPO_ACCESO = "admin"
else:
    _usuario_actual = st.session_state.get("usuario_auth", {})
    TOKEN_ACTIVO = (
        _usuario_actual.get("user_id")
        or _usuario_actual.get("email")
        or "usuario"
    )
    FECHA_VENCIMIENTO_LICENCIA = "2099-01-01"
    TIPO_ACCESO = "usuario"

USUARIO_AUTENTICADO = not PUBLIC_PREVIEW

# ------------------------------------------------------------------
# Persistencia real de la última configuración por usuario.
# La carátula vive dentro de un iframe y el localStorage del iframe no
# es una base fiable para recuperar la configuración después de un
# refresh/login. Por eso la última configuración también se conserva
# en memoria del servidor, asociada al correo del usuario.
# ------------------------------------------------------------------
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
    "f_gap_on", "f_float_on", "f_vol_on", "ema20_on",
    "ema20_cond", "ema50_cond", "ema200_cond",
    "ema20_dist", "ema50_dist", "ema200_dist",
    "swing_activo", "swing_origen", "swing_objetivo", "swing_ventana",
    "swing_tolerancia", "swing_origen_tolerancia", "swing_multitimeframe", "swing_tfs",
)

def _clave_configuracion_activa():
    """Clave estable y no sensible para persistir configuración."""
    try:
        email = str(st.session_state.get("usuario_auth", {}).get("email", "")).strip().lower()
        if email:
            return email
        token = str(st.session_state.get("token_verificado", "")).strip()
        if token:
            return "admin:" + hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]
    except Exception:
        pass
    return ""

def _email_usuario_activo():
    return _clave_configuracion_activa()

def _restaurar_ultima_configuracion_servidor():
    if not USUARIO_AUTENTICADO:
        return False
    email = _email_usuario_activo()
    if not email:
        return False
    guardada = _ULTIMA_CONFIG_USUARIOS.get(email)
    try:
        _sid_cfg = str(st.query_params.get("auth_session", "")).strip()
        _ses_cfg = _PERSISTENT_AUTH_SESSIONS.get(_sid_cfg, {}) if _sid_cfg else {}
        _cfg_sid = _ses_cfg.get("config") if isinstance(_ses_cfg, dict) else None
        if isinstance(_cfg_sid, dict) and _cfg_sid:
            # La sesión persistente y el almacenamiento por usuario pueden
            # tener versiones distintas. Elegimos la más reciente por
            # _saved_at para no resucitar una configuración antigua.
            _cfg_usr = guardada if isinstance(guardada, dict) else None
            try:
                _ts_usr = datetime.fromisoformat(str(_cfg_usr.get("_saved_at", "")).replace("Z", "+00:00")) if _cfg_usr else datetime.min.replace(tzinfo=timezone.utc)
            except Exception:
                _ts_usr = datetime.min.replace(tzinfo=timezone.utc)
            try:
                _ts_sid = datetime.fromisoformat(str(_cfg_sid.get("_saved_at", "")).replace("Z", "+00:00"))
            except Exception:
                _ts_sid = datetime.min.replace(tzinfo=timezone.utc)
            guardada = _cfg_sid if _ts_sid >= _ts_usr else _cfg_usr
            if isinstance(guardada, dict):
                _ULTIMA_CONFIG_USUARIOS[email] = dict(guardada)
    except Exception:
        pass
    if not isinstance(guardada, dict) or not guardada:
        return False
    # Preparar toda la restauración antes de tocar st.query_params.
    # Una sola actualización evita una cascada de cambios de URL durante
    # el arranque de un refresh completo.
    _restaurados = {}
    for clave in _CONFIG_USUARIO_KEYS:
        if clave in guardada and str(st.query_params.get(clave, "")) == "":
            if clave == "market_session":
                _restaurados[clave] = "TODO EL MERCADO"
            elif clave == "c_start":
                _restaurados[clave] = "04:00"
            elif clave == "c_end":
                _restaurados[clave] = "20:00"
            else:
                _restaurados[clave] = str(guardada[clave])
    if _restaurados:
        try:
            st.query_params.update(_restaurados)
        except Exception:
            # Compatibilidad con versiones donde update no esté disponible.
            for _k, _v in _restaurados.items():
                st.query_params[_k] = _v
        return True
    return False

def _guardar_ultima_configuracion_servidor():
    if not USUARIO_AUTENTICADO:
        return
    email = _email_usuario_activo()
    if not email:
        return
    estado = {}
    for clave in _CONFIG_USUARIO_KEYS:
        valor = st.query_params.get(clave, None)
        if valor is not None and str(valor) != "":
            estado[clave] = str(valor)
    if estado:
        estado["_saved_at"] = datetime.now(timezone.utc).isoformat()
        _ULTIMA_CONFIG_USUARIOS[email] = estado
        # También la asociamos a la sesión persistente actual. Así una
        # navegación/refresh conserva exactamente el último estado aunque
        # todavía no haya vuelto a Supabase.
        try:
            _sid_cfg = str(st.query_params.get("auth_session", "")).strip()
            if _sid_cfg and _sid_cfg in _PERSISTENT_AUTH_SESSIONS:
                _PERSISTENT_AUTH_SESSIONS[_sid_cfg]["config"] = dict(estado)
        except Exception:
            pass
        try:
            with open(_RUTA_ULTIMA_CONFIG_USUARIOS, "w", encoding="utf-8") as _f_cfg:
                json.dump(_ULTIMA_CONFIG_USUARIOS, _f_cfg, ensure_ascii=False, indent=2)
        except Exception as _e_cfg:
            print(f"⚠️ No se pudo persistir la configuración del usuario: {_e_cfg}")

# Al volver a entrar con la misma cuenta, recuperar la última configuración
# antes de construir la interfaz. Así el refresh tampoco vuelve a 3 minutos.
# La configuración restaurada ya quedó escrita en st.query_params y puede
# ser consumida por este mismo ciclo. No forzar un segundo rerun durante
# el arranque: en un refresh completo ese rerun intermedio puede dejar la
# página sin contenido mientras Streamlit reconstruye la sesión.
_restaurar_ultima_configuracion_servidor()


# Solo los tokens configurados como ADMIN pueden ser administradores.
ADMIN_TOKEN = st.secrets.get("ADMIN_TOKEN", None)
_ADMIN_TOKENS_RAW = st.secrets.get("ADMIN_TOKENS", "")

if isinstance(_ADMIN_TOKENS_RAW, (list, tuple, set)):
    ADMIN_TOKENS = {
        str(x).strip()
        for x in _ADMIN_TOKENS_RAW
        if str(x).strip()
    }
else:
    ADMIN_TOKENS = {
        x.strip()
        for x in str(_ADMIN_TOKENS_RAW).split(",")
        if x.strip()
    }

if ADMIN_TOKEN:
    ADMIN_TOKENS.add(str(ADMIN_TOKEN).strip())

ES_ADMIN = (
    TIPO_ACCESO == "admin"
    and bool(TOKEN_ACTIVO)
    and TOKEN_ACTIVO in ADMIN_TOKENS
)

# ==========================================
# 💳 CONTROL DE LICENCIA DEL USUARIO
# ==========================================
LICENCIA_ACTUAL = None
ESTADO_LICENCIA = "ADMIN" if ES_ADMIN else "SIN LICENCIA"
VENCIMIENTO_LICENCIA_DT = None
if not ES_ADMIN and USUARIO_AUTENTICADO:
    _u = st.session_state.get("usuario_auth", {})
    LICENCIA_ACTUAL = obtener_licencia_usuario(
        _u.get("user_id", ""), _u.get("email", "")
    )
    ESTADO_LICENCIA, VENCIMIENTO_LICENCIA_DT = estado_licencia(LICENCIA_ACTUAL)

    if ESTADO_LICENCIA != "ACTIVO":
        st.markdown(
            """
            <style>
            .paywall {max-width:850px;margin:55px auto;padding:30px;border:1px solid #334155;border-radius:18px;background:#0d1118;text-align:center;}
            .paywall h1{color:#d4af37;margin-bottom:8px;}
            .paywall p{color:#aeb7c5;}
            </style>
            <div class="paywall">
              <h1>🔒 Tu acceso requiere una membresía</h1>
              <p>La prueba gratuita terminó o la cuenta todavía no tiene una licencia activa.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("### Elige tu plan — COBRO SIMULADO")
        st.caption("Tu primer mes es gratis. En esta versión de prueba no se realiza ningún cargo real ni se solicita tarjeta.")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### 🟦 Solo Scanner")
            st.markdown("**$28/mes** · **$270/año**")
            if st.button("ACTIVAR SCANNER MENSUAL", width="stretch"):
                ok, msg = activar_plan_simulado(_u.get("user_id", ""), "MENSUAL_SCANNER")
                if ok:
                    st.success("✅ Plan Scanner mensual simulado activado.")
                    st.rerun()
                else:
                    st.error(msg)
            if st.button("ACTIVAR SCANNER ANUAL", width="stretch"):
                ok, msg = activar_plan_simulado(_u.get("user_id", ""), "ANUAL_SCANNER")
                if ok:
                    st.success("✅ Plan Scanner anual simulado activado.")
                    st.rerun()
                else:
                    st.error(msg)
        with c2:
            st.markdown("#### 🟧 Scanner + 🤖 Robot")
            st.markdown("**$38/mes** · **$370/año**")
            if st.button("ACTIVAR SCANNER + ROBOT MENSUAL", width="stretch"):
                ok, msg = activar_plan_simulado(_u.get("user_id", ""), "MENSUAL_ROBOT")
                if ok:
                    st.success("✅ Plan Scanner + Robot mensual simulado activado.")
                    st.rerun()
                else:
                    st.error(msg)
            if st.button("ACTIVAR SCANNER + ROBOT ANUAL", width="stretch"):
                ok, msg = activar_plan_simulado(_u.get("user_id", ""), "ANUAL_ROBOT")
                if ok:
                    st.success("✅ Plan Scanner + Robot anual simulado activado.")
                    st.rerun()
                else:
                    st.error(msg)
        st.info("El administrador también podrá concederte acceso gratuito durante el período de prueba o como cortesía.")
        st.stop()


# Barra discreta de sesión.
with st.sidebar:
    st.markdown("### 👤 Sesión")

    if PUBLIC_PREVIEW:
        st.info("👀 Visitante")
        st.caption("Puedes explorar la interfaz sin registrarte. Usa REGISTRO / INICIAR SESIÓN dentro del scanner.")
    elif ES_ADMIN:
        st.success("Administrador")
    else:
        _email_ui = st.session_state.get("usuario_auth", {}).get(
            "email", "Usuario"
        )
        st.info(_email_ui)

    if not PUBLIC_PREVIEW and st.button(
        "🚪 CERRAR SESIÓN",
        key="cerrar_sesion_global",
        width="stretch",
    ):
        cerrar_sesion()
        st.rerun()

    if ES_ADMIN:
        st.markdown("---")
        st.markdown("### 👑 Administración")
        st.caption("Modo de prueba: licencias y cobros simulados")
        licencias = _leer_licencias_simuladas()
        st.metric("Usuarios registrados", len(licencias))
        if licencias:
            activos = sum(1 for x in licencias.values() if estado_licencia(x)[0] == "ACTIVO")
            vencidos = sum(1 for x in licencias.values() if estado_licencia(x)[0] == "VENCIDO")
            st.write(f"Activos: **{activos}** · Vencidos: **{vencidos}**")
            for uid, lic in list(licencias.items())[:25]:
                estado, venc_txt = _resumen_licencia(lic)
                st.markdown(f"**{lic.get('email','Usuario')}**  ")
                st.caption(f"{lic.get('plan','—')} · {estado} · vence {venc_txt}")
                if st.button("🎁 +30 días", key=f"grant_{uid}", width="stretch"):
                    if conceder_gratis_admin(uid, 30):
                        st.success("30 días gratuitos concedidos.")
                        st.rerun()
                if st.button("⛔ Suspender", key=f"suspend_{uid}", width="stretch"):
                    if suspender_usuario_admin(uid):
                        st.warning("Usuario suspendido.")
                        st.rerun()


# ==========================================
# 🔐 CHARLES SCHWAB / OAUTH 2.0
# ==========================================
def _schwab_secret(nombre, default=""):
    try:
        return str(st.secrets.get(nombre, default) or default).strip()
    except Exception:
        return str(default or "").strip()


def _schwab_client_id():
    return str(st.session_state.get("schwab_client_id") or _schwab_secret("SCHWAB_CLIENT_ID"))


def _schwab_client_secret():
    return str(st.session_state.get("schwab_client_secret") or _schwab_secret("SCHWAB_CLIENT_SECRET"))


def _schwab_redirect_uri():
    return str(st.session_state.get("schwab_redirect_uri") or _schwab_secret("SCHWAB_REDIRECT_URI", "")).strip()


def _schwab_exchange_code(code):
    cid = _schwab_client_id()
    secret = _schwab_client_secret()
    redirect = _schwab_redirect_uri()
    if not cid or not secret or not redirect or not code:
        return False, "Faltan SCHWAB_CLIENT_ID, SCHWAB_CLIENT_SECRET, SCHWAB_REDIRECT_URI o code."
    try:
        r = requests.post(
            SCHWAB_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect,
            },
            auth=(cid, secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        if r.status_code != 200:
            return False, f"Schwab token HTTP {r.status_code}: {r.text[:300]}"
        tok = r.json()
        st.session_state["schwab_token"] = tok
        st.session_state["schwab_connected"] = True
        return True, "Charles Schwab conectado correctamente."
    except Exception as e:
        return False, f"Error OAuth Schwab: {e}"


def _schwab_access_token():
    tok = st.session_state.get("schwab_token")
    if not isinstance(tok, dict):
        return ""
    return str(tok.get("access_token") or "")


def _schwab_authorize_url():
    cid = _schwab_client_id()
    redirect = _schwab_redirect_uri()
    if not cid or not redirect:
        return ""
    return (SCHWAB_AUTHORIZE_URL + "?client_id=" + quote(cid, safe="") +
            "&redirect_uri=" + quote(redirect, safe="") + "&response_type=code")


def _schwab_callback():
    try:
        code = str(st.query_params.get("code", "")).strip()
        if not code:
            return
        ok, msg = _schwab_exchange_code(code)
        st.session_state["schwab_status"] = msg
        # No conservar el authorization code en la URL.
        q = dict(st.query_params)
        q.pop("code", None)
        q.pop("state", None)
        if q:
            st.query_params.clear()
            for k, v in q.items():
                st.query_params[k] = v
        else:
            st.query_params.clear()
        st.rerun()
    except Exception as e:
        st.session_state["schwab_status"] = f"Error procesando callback Schwab: {e}"


def _schwab_send_layout_bridge(ticker, layout_color, bridge_url):
    """Envía el ticker al puente local configurado por el usuario.
    El API oficial de Schwab se usa para autorización; el concepto de
    'layout' de thinkorswim no es un endpoint oficial documentado de la API.
    Por eso el puente existente sigue siendo el mecanismo de layout.
    """
    if not bridge_url:
        return False, "No hay PUENTE DE LAYOUT configurado."
    try:
        r = requests.post(
            bridge_url,
            json={
                "broker": "Charles Schwab",
                "ticker": str(ticker),
                "layout_color": str(layout_color),
                "schwab_connected": bool(_schwab_access_token()),
                "timestamp": time.time(),
            },
            timeout=5,
        )
        if 200 <= r.status_code < 300:
            return True, "Ticker enviado al puente de layout."
        return False, f"Puente HTTP {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"No se pudo contactar el puente: {e}"

_schwab_callback()

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


def _big(v):
    """Formato compacto (K/M) para volumen y float. Nivel de módulo: lo usa el motor
    (mensaje de Telegram) y antes solo existía dentro de _render_scanner, así que
    _ciclo lanzaba NameError al armar la tabla cada vez que había resultados."""
    try:
        n = float(v)
    except (TypeError, ValueError):
        n = 0.0
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.0f}K"
    return f"{n:.0f}"


def evaluar_tecnico(velas):
    """Calcula EMA20/MACD/Bollinger/RSI sobre la temporalidad seleccionada.

    Señal EMA20 solicitada:
      1) la vela actual NACE (abre) por encima de la EMA20 de la vela anterior;
      2) su mínimo es mayor que el mínimo de la vela anterior.
      El HIGH de la vela actual NO se usa porque la vela todavía está formándose.

    La EMA20 se calcula sobre cierres. Para evitar que la EMA "se mueva"
    durante la vela que nace, se compara el OPEN actual contra la EMA20
    calculada hasta la vela anterior.
    """
    if velas is None or len(velas) < 40:
        return (False, False, False, False, None, None, None, 0,
                None, None, None, None, None, None, None, None, None)

    try:
        velas = velas.sort_index()
        cierres = velas["close"].astype(float).dropna()
        if len(cierres) < 40:
            return (False, False, False, False, None, None, None, 0,
                    None, None, None, None, None, None, None, None, None)

        # Aseguramos que OHLC y cierres correspondan a las últimas dos velas.
        if not all(col in velas.columns for col in ("open", "high", "low", "close")):
            return (False, False, False, False, None, None, None, 0,
                    None, None, None, None, None, None, None, None, None)

        vela_prev = velas.iloc[-2]
        vela_act = velas.iloc[-1]
        ema20 = cierres.ewm(span=20, adjust=False).mean()
        ema50 = cierres.ewm(span=50, adjust=False).mean()
        ema200 = cierres.ewm(span=200, adjust=False).mean()
        macd_line = cierres.ewm(span=12, adjust=False).mean() - cierres.ewm(span=26, adjust=False).mean()

        # RSI(14) de Wilder sobre la misma temporalidad seleccionada.