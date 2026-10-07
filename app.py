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
from TradeScanner.data_engine import AlpacaMarketStream
try:
    from BotTradeScanner.integracion.live_motor_bridge import MotorVelasBridge
    _MOTOR_VELAS_IMPORT_ERROR = None
except Exception as _e_motor_import:
    # Si el módulo no se encuentra, el scanner igual debe abrir (sin motor de velas).
    MotorVelasBridge = None
    _MOTOR_VELAS_IMPORT_ERROR = f"{type(_e_motor_import).__name__}: {_e_motor_import}"
    print(f"⚠️ No se pudo importar MotorVelasBridge: {_MOTOR_VELAS_IMPORT_ERROR}")

st.set_page_config(page_title="Scanner Pre Market", layout="wide")

# Precio y gap viven DENTRO del cuadro gris (iframe). Los controles nativos de afuera quedan apagados.
_USAR_FILTROS_NATIVOS = True

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
      try{f.setAttribute('allow','loopback-network; local-network; local-network-access');}catch(e){}
      current=f;
      wrap.appendChild(f);
      try{f.srcdoc=lastHtml;}catch(e){f.src='data:text/html;charset=utf-8,'+encodeURIComponent(lastHtml);}
      f.__html=lastHtml;
      f.style.visibility='visible';
      return;
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
    if(d.tsNav){
      post('streamlit:setComponentValue',{value:{id:String(Date.now())+'-'+Math.random().toString(36).slice(2),q:String(d.q||'')},dataType:'json'});
      return;
    }
    if(!isOurs(ev.source))return;
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
_TS_USE_COMPONENT = True
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
        [data-testid="stTabs"] { position: relative !important; z-index: 100000 !important; }
        [data-testid="stTabs"] [role="tab"] { position: relative !important; z-index: 100001 !important; pointer-events: auto !important; cursor: pointer !important; }
        [data-testid="stForm"] button { position: relative !important; z-index: 100002 !important; pointer-events: auto !important; }
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
    st.markdown(
        f'<a href="/" target="_top" '
        'style="display:inline-block;padding:6px 10px;border:1px solid #555;border-radius:4px;'
        'color:#e5e9ee;text-decoration:none;background:#20252c;font-size:11px;">← Volver al scanner</a>',
        unsafe_allow_html=True,
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

    return


def _ts_auth_href(modo="abrir"):
    """Construye una navegación de la misma página sin usar callbacks de Streamlit.
    Se usa para aislar Auth del rerun que desmonta el iframe del scanner.
    """
    try:
        from urllib.parse import urlencode
        pares = {}
        for _k, _v in st.query_params.items():
            if isinstance(_v, list):
                if _v:
                    pares[str(_k)] = str(_v[0])
            elif _v is not None:
                pares[str(_k)] = str(_v)
        pares.pop("logout", None)
        if modo == "abrir":
            pares["auth"] = "1"
        else:
            pares.pop("auth", None)
        _qs = urlencode(pares)
        return "?" + _qs if _qs else "/"
    except Exception:
        return "/?auth=1" if modo == "abrir" else "/"


# =========================================================
# 🌐 MODO PÚBLICO / AUTENTICACIÓN
# =========================================================
# auth_session solo es respaldo de recarga completa; durante un rerun normal
# la identidad permanece en st.session_state.

def _ts_diag_add(etapa, detalle="", estado=None):
    """Registro temporal de la ruta UI -> Python -> estado."""
    try:
        log = st.session_state.setdefault("_ts_diag_log", [])
        from datetime import datetime
        fila = {"hora": datetime.now().strftime("%H:%M:%S.%f")[:-3],
                "etapa": str(etapa), "detalle": str(detalle)}
        if isinstance(estado, dict):
            fila["estado"] = {k: str(v) for k, v in estado.items()}
        log.append(fila)
        if len(log) > 80:
            del log[:-80]
    except Exception:
        pass


def _ts_diag_snapshot():
    """Copia compacta de los valores que pueden rebotar."""
    try:
        claves = ("f_price_min", "f_price_max", "f_gap_min", "f_gap_max",
                  "f_float_max", "f_vol", "f_mac", "f_order", "timeframe")
        qp = {}
        for k in claves:
            v = st.query_params.get(k, "")
            if isinstance(v, list):
                v = v[0] if v else ""
            qp[k] = str(v)
        canon = st.session_state.get("_ts_query_elegida")
        if not isinstance(canon, dict):
            canon = {}
        return {"query": qp, "canon": {k: str(canon.get(k, "")) for k in claves}}
    except Exception:
        return {}



def _ts_aplicar_evento_ui():
    """Recibe lo que hizo el usuario en el cuadro gris y lo convierte
    inmediatamente en el estado canónico de esta sesión.
    
    El evento del componente es la fuente de verdad. No basta con escribir
    st.query_params: el sincronizador que corre después también puede tener
    una copia anterior en _ts_query_elegida. Por eso ambos se actualizan
    atómicamente aquí, antes de construir el scanner.
    """
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
        _diag_raw = pares.pop("_ts_diag", "")
        if _diag_raw:
            try:
                _diag_browser = json.loads(_diag_raw)
                _ts_diag_add(
                    "NAVEGADOR → PYTHON",
                    "pushConfig recibió los valores del DOM",
                    {"dom": _diag_browser.get("fields", {}), "url_q": str(ev.get("q", ""))[-900:]}
                )
            except Exception as _de:
                _ts_diag_add("NAVEGADOR → PYTHON", f"diagnóstico JS inválido: {_de}")
        pares.pop("_ts", None)

        # La configuración que viene del iframe pasa a ser canónica
        # inmediatamente. Esto evita que un valor viejo de Session State
        # pueda ganar en el mismo rerun.
        _canon = st.session_state.get("_ts_query_elegida")
        if not isinstance(_canon, dict):
            _canon = {}

        _permitidas = set(_CONFIG_USUARIO_KEYS) if "_CONFIG_USUARIO_KEYS" in globals() else set()
        for _k, _v in pares.items():
            if _k == "auth_session" and not _v:
                continue
            if _k in _permitidas or _k in ("_u", "technical_timeframe", "robot"):
                _canon[_k] = str(_v)

        st.session_state["_ts_query_elegida"] = _canon

        # Aplicar el mismo estado a query_params en una sola operación.
        _qp_evento = {}
        for _k, _v in _canon.items():
            if _k == "auth_session" and not _v:
                continue
            if str(st.query_params.get(_k, "")) != str(_v):
                _qp_evento[_k] = str(_v)
        if _qp_evento:
            try:
                st.query_params.update(_qp_evento)
            except Exception:
                for _k, _v in _qp_evento.items():
                    st.query_params[_k] = _v

        _ts_diag_add(
            "PYTHON: EVENTO APLICADO",
            "La configuración recibida quedó escrita en Session State y query_params",
            _ts_diag_snapshot()
        )

        if "c_active" in pares:
            # Solo se consume como orden de motor después de verificar ES_ADMIN.
            st.session_state["_admin_motor_evento"] = str(pares.get("c_active", ""))

        # Marcar el _u recibido como ya visto: este evento ya fue consumido.
        try:
            _u_evento = int(float(str(pares.get("_u", "0") or "0")))
        except Exception:
            _u_evento = 0
        try:
            _u_prev = int(st.session_state.get("_ts_u_visto", 0) or 0)
        except Exception:
            _u_prev = 0
        st.session_state["_ts_u_visto"] = max(_u_prev, _u_evento)

        # Una acción del usuario siempre gana a un auto-refresh que coincida en el tiempo.
        st.session_state["_ts_rerun_auto"] = False
    except Exception as _e_ev:
        print(f"⚠️ No se pudo aplicar el evento del cuadro gris: {_e_ev}")


# Restaurar la identidad persistente ANTES de decidir si debemos mostrar Auth.
# En un F5 Streamlit crea un st.session_state nuevo; auth_session es la clave
# para recuperar al usuario antes de entrar al flujo público.
if "token_verificado" not in st.session_state and "usuario_auth" not in st.session_state:
    _restaurar_sesion_persistente()

# Estado nativo de autenticación.
# IMPORTANTE: la autenticación se resuelve ANTES de construir el scanner/robot.
# Este era el flujo estable de la versión anterior al montaje del robot y evita
# intentar insertar Auth dentro del mismo árbol que contiene el iframe del scanner.
if "mostrar_auth" not in st.session_state:
    st.session_state["mostrar_auth"] = False

# El cierre de sesión se procesa fuera del callback del botón.
if st.session_state.pop("_ts_logout_requested", False):
    cerrar_sesion()
    st.session_state.pop("_ts_query_elegida", None)
    st.session_state.pop("_ts_u_visto", None)
    st.session_state["mostrar_auth"] = False
    try:
        st.query_params.clear()
    except Exception:
        pass

AUTH_REQUESTED = str(st.query_params.get("auth", "0")).lower() in ("1", "true", "yes")
LOGOUT_REQUESTED = str(st.query_params.get("logout", "0")).lower() in ("1", "true", "yes")

if LOGOUT_REQUESTED:
    cerrar_sesion()
    st.session_state["mostrar_auth"] = False
    try:
        st.query_params.clear()
    except Exception:
        pass
    st.rerun()

if AUTH_REQUESTED:
    st.session_state["mostrar_auth"] = True
    try:
        st.query_params.pop("auth", None)
    except Exception:
        pass

# Si Auth está abierta, no procesamos eventos del iframe, sincronización,
# motor visual ni robot. Solo dibujamos la autenticación y terminamos el run.
if st.session_state.get("mostrar_auth"):
    pantalla_autenticacion()
    st.stop()

# Desde aquí comienza el flujo normal de la aplicación.
_ts_aplicar_evento_ui()

PUBLIC_PREVIEW = (
    "token_verificado" not in st.session_state
    and "usuario_auth" not in st.session_state
)

# Visitantes: solo lectura. No aceptamos configuración personal enviada por URL.
if PUBLIC_PREVIEW:
    # Limpieza atómica de filtros de usuario en la primera carga pública.
    # Evita una cascada de actualizaciones de URL durante un refresh completo.
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
        delta = cierres.diff()
        ganancias = delta.clip(lower=0)
        perdidas = -delta.clip(upper=0)
        avg_gain = ganancias.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
        avg_loss = perdidas.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
        rs = avg_gain / avg_loss.replace(0, pd.NA)
        rsi_series = 100 - (100 / (1 + rs))
        rsi_val = rsi_series.iloc[-1]
        if pd.isna(rsi_val):
            rsi_val = 100.0 if avg_loss.iloc[-1] == 0 and avg_gain.iloc[-1] > 0 else (50.0 if avg_loss.iloc[-1] == 0 else None)
        else:
            rsi_val = float(rsi_val)

        precio_act = float(vela_act["close"])
        precio_prev = float(vela_prev["close"])
        ema_act = float(ema20.iloc[-1])
        ema_prev = float(ema20.iloc[-2])
        # EMA50/EMA200 son informativas: solo se reportan si hay velas suficientes.
        ema50_act = float(ema50.iloc[-1]) if (len(cierres) >= 50 and not pd.isna(ema50.iloc[-1])) else None
        ema200_act = float(ema200.iloc[-1]) if (len(cierres) >= 200 and not pd.isna(ema200.iloc[-1])) else None
        macd_actual = macd_line.iloc[-1]
        macd_val = float(macd_actual) if not pd.isna(macd_actual) else None

        bb_mid = cierres.rolling(20).mean()
        bb_std = cierres.rolling(20).std()
        bb_upper = bb_mid.iloc[-1] + 2 * bb_std.iloc[-1]
        bb_upper_val = float(bb_upper) if not pd.isna(bb_upper) else None

        open_act = float(vela_act["open"])
        high_act = float(vela_act["high"])
        low_act = float(vela_act["low"])
        high_prev = float(vela_prev["high"])
        low_prev = float(vela_prev["low"])

        if pd.isna(ema_act) or ema_act <= 0 or pd.isna(ema_prev) or ema_prev <= 0:
            return (False, False, False, False, precio_act, None, macd_val, len(cierres),
                    precio_prev, ema_prev, precio_act, ema_act, bb_upper_val, None, None, ema50_act, ema200_act)

        # EMA20 NUEVA: vela naciendo por encima + mínimo superior. El HIGH actual NO participa.
        estructura_alcista = bool(
            open_act > ema_prev and
            low_act > low_prev
        )

        # La señal de bajada conserva una lógica simétrica para no romper
        # el selector existente de la interfaz.
        estructura_bajista = bool(
            open_act < ema_prev and
            high_act < high_prev
        )

        cruzo_arriba = estructura_alcista
        cruzo_abajo = estructura_bajista
        macd_positivo = bool(macd_val is not None and macd_val > 0)
        macd_negativo = bool(macd_val is not None and macd_val < 0)
        bb_dist_pct = ((bb_upper_val - precio_act) / precio_act * 100.0) if bb_upper_val is not None and precio_act > 0 else None

        return (cruzo_arriba, cruzo_abajo, macd_positivo, macd_negativo,
                precio_act, ema_act, macd_val, len(cierres), precio_prev, ema_prev,
                precio_act, ema_act, bb_upper_val, bb_dist_pct, rsi_val, ema50_act, ema200_act)
    except Exception as e:
        print(f"⚠️ Error evaluando EMA20/velas: {e}")
        return (False, False, False, False, None, None, None, 0,
                None, None, None, None, None, None, None, None, None)


OPCIONES_COND_EMA = ("Ninguna", "Naciendo", "Distancia", "Naciendo o distancia", "Pullback a la baja", "Pullback a la alta")


def evaluar_swing(velas):
    """Detecta zona inferior -> cruce EMA20 -> continuación -> primer toque."""
    out = {"swing_cross_ago": 999, "swing_origin_bb_dist_pct": 999.0,
           "swing_origin_below_ema20": False, "swing_continua_alcista": False,
           "swing_ema50_touch_dist_current": 999.0, "swing_ema50_touch_dist_previous_min": 999.0,
           "swing_ema200_touch_dist_current": 999.0, "swing_ema200_touch_dist_previous_min": 999.0}
    try:
        if velas is None or len(velas) < 200:
            return out
        v=velas.sort_index()
        if not all(c in v.columns for c in ("open","high","low","close")):
            return out
        close=v["close"].astype(float).reset_index(drop=True)
        e20=close.ewm(span=20,adjust=False).mean()
        e50=close.ewm(span=50,adjust=False).mean()
        e200=close.ewm(span=200,adjust=False).mean()
        mid=close.rolling(20).mean(); std=close.rolling(20).std(); lower=mid-2*std
        last=len(close)-1; cross=None
        for i in range(max(1,last-30),last+1):
            if close.iloc[i-1] < e20.iloc[i-1] and close.iloc[i] >= e20.iloc[i]:
                cross=i
        if cross is None:
            return out
        out["swing_cross_ago"]=last-cross
        bb_dist=999.0; below=False
        for j in range(max(0,cross-12),cross+1):
            if float(close.iloc[j]) < float(e20.iloc[j]): below=True
            b=lower.iloc[j]
            if not pd.isna(b) and float(b)>0:
                lo=float(v.iloc[j]["low"]); d=0.0 if lo<=float(b) else max(0.0,(lo-float(b))/float(b)*100.0)
                bb_dist=min(bb_dist,d)
        out["swing_origin_bb_dist_pct"]=bb_dist
        out["swing_origin_below_ema20"]=below
        post=close.iloc[cross:last+1]
        out["swing_continua_alcista"]=bool(len(post)>=2 and close.iloc[last]>e20.iloc[last] and float(post.max())>float(close.iloc[cross]))
        def rdist(j,ema):
            e=float(ema.iloc[j])
            if e<=0:return 999.0
            lo=float(v.iloc[j]["low"]); hi=float(v.iloc[j]["high"])
            if lo<=e<=hi:return 0.0
            return min(abs(lo-e),abs(hi-e))/e*100.0
        for n,ema in ((50,e50),(200,e200)):
            cur=rdist(last,ema); prev=999.0
            for j in range(cross+1,last): prev=min(prev,rdist(j,ema))
            out["swing_ema%d_touch_dist_current"%n]=cur
            out["swing_ema%d_touch_dist_previous_min"%n]=prev
    except Exception as e:
        print("⚠️ Error evaluando Swing EMA20/50/200:",e)
    return out


def evaluar_ema_condiciones(velas):
    """Datos extra por EMA (20/50/200) para las pestañas de filtros.

    Por cada EMA N devuelve:
      emaN_dist_pct   -> distancia absoluta (%) entre el precio actual y la EMA N
      emaN_nace_arriba -> la vela actual NACE sobre la EMA N (misma definición
                          que ya usa la EMA20: open actual > EMA N de la vela
                          anterior y mínimo actual > mínimo anterior)
      emaN_nace_abajo  -> lo simétrico hacia abajo
    Si no hay velas suficientes para esa EMA, no devuelve sus claves.
    """
    salida = {}
    try:
        if velas is None or len(velas) < 40:
            return salida
        velas = velas.sort_index()
        if not all(col in velas.columns for col in ("open", "high", "low", "close")):
            return salida
        cierres = velas["close"].astype(float).dropna()
        vela_prev = velas.iloc[-2]
        vela_act = velas.iloc[-1]
        open_act = float(vela_act["open"])
        high_act = float(vela_act["high"])
        low_act = float(vela_act["low"])
        high_prev = float(vela_prev["high"])
        low_prev = float(vela_prev["low"])
        precio = float(vela_act["close"])
        for n in (20, 50, 200):
            if len(cierres) < n:
                continue
            serie = cierres.ewm(span=n, adjust=False).mean()
            ema_act = float(serie.iloc[-1])
            ema_prev = float(serie.iloc[-2])
            if pd.isna(ema_act) or pd.isna(ema_prev) or ema_act <= 0 or ema_prev <= 0:
                continue
            salida[f"ema{n}_dist_pct"] = abs(precio - ema_act) / ema_act * 100.0
            salida[f"ema{n}_nace_arriba"] = bool(open_act > ema_prev and low_act > low_prev)
            salida[f"ema{n}_nace_abajo"] = bool(open_act < ema_prev and high_act < high_prev)
            # Pullback a la baja: vela BAJISTA que retrocede hacia la EMA desde arriba y todavia cierra sobre ella.
            # Se guarda cuanto separa el MINIMO de la vela de la EMA (negativo = la mecha la perfora).
            if precio > ema_act and precio < open_act:
                salida[f"ema{n}_pb_baja_dist"] = (low_act - ema_act) / ema_act * 100.0
            # Pullback a la alta: vela ALCISTA que retrocede hacia la EMA desde abajo y todavia cierra bajo ella.
            if precio < ema_act and precio > open_act:
                salida[f"ema{n}_pb_alta_dist"] = (ema_act - high_act) / ema_act * 100.0
    except Exception as e:
        print(f"⚠️ Error evaluando condiciones EMA: {e}")
    return salida


def cumple_macd(c, p):
    """MACD según el selector: Positivo (por defecto), Negativo o No exigir."""
    modo = p.get("macd", "Positivo")
    if modo == "No exigir":
        return True
    if modo == "Negativo":
        return bool(c.get("macd_negativo", False))
    return bool(c.get("macd_positivo", False))


def cumple_condiciones_ema(c, p):
    """Aplica las pestañas EMA20 / EMA50 / EMA200.

    Para cada EMA:
      1) Estado: "Por encima" exige precio > EMA; "Por debajo" exige precio < EMA;
         "Neutro" no exige nada.
      2) Condición de entrada (además del estado):
           Ninguna              -> nada más
           Naciendo             -> primera vela naciendo sobre (o bajo) la EMA
           Distancia            -> precio a <= X% de la EMA
           Naciendo o distancia -> cualquiera de las dos
    Por defecto EMA20 = "Naciendo" (la regla original del scanner) y
    EMA50/EMA200 = "Ninguna". Si un dato no se puede calcular, la condición falla.
    """
    swing_activo = _filtro_activo(p, "swing_activo", False)
    if swing_activo:
        for _n in (20, 50, 200):
            _pedido = p.get("ema%d_estado" % _n, "Neutro")
            if _pedido in ("Por encima", "Por debajo") and c.get("ema%d_estado" % _n, "Neutro") != _pedido:
                return False
        try: _ventana=max(1,min(30,int(float(p.get("swing_ventana",10)))))
        except Exception: _ventana=10
        try: _tol=max(0.0,min(10.0,float(p.get("swing_tolerancia",1.0))))
        except Exception: _tol=1.0
        try: _otol=max(0.0,min(10.0,float(p.get("swing_origen_tolerancia",1.0))))
        except Exception: _otol=1.0
        if int(c.get("swing_cross_ago",999)) > _ventana:return False
        _origen=str(p.get("swing_origen","Bollinger inferior + debajo de EMA20"))
        _bb=float(c.get("swing_origin_bb_dist_pct",999.0)) <= _otol
        _below=bool(c.get("swing_origin_below_ema20",False))
        if _origen=="Bollinger inferior" and not _bb:return False
        if _origen=="Debajo de EMA20" and not _below:return False
        if _origen=="Bollinger inferior + debajo de EMA20" and not (_bb and _below):return False
        if not bool(c.get("swing_continua_alcista",False)):return False
        _obj=str(p.get("swing_objetivo","EMA50 o EMA200"))
        _ok50=_obj in ("EMA50","EMA50 o EMA200") and float(c.get("swing_ema50_touch_dist_current",999.0))<=_tol and float(c.get("swing_ema50_touch_dist_previous_min",999.0))>_tol
        _ok200=_obj in ("EMA200","EMA50 o EMA200") and float(c.get("swing_ema200_touch_dist_current",999.0))<=_tol and float(c.get("swing_ema200_touch_dist_previous_min",999.0))>_tol
        if not (_ok50 or _ok200):return False
        c["swing_objetivo_detectado"]="EMA50" if _ok50 and not _ok200 else ("EMA200" if _ok200 and not _ok50 else "EMA50 + EMA200")
        return True

    for n in (20, 50, 200):
        pedido = p.get(f"ema{n}_estado", "Neutro")
        cond = p.get(f"ema{n}_cond", "Naciendo" if n == 20 else "Ninguna")
        if n == 20 and not _filtro_activo(p, "ema20_activa", _filtro_activo(p, "ema20_on", False)):
            # Con el interruptor en OFF solo se ignora la EMA20 si esta en su valor por defecto.
            # Si el usuario eligio ARRIBA/ABAJO o una condicion distinta, SI se aplica.
            if pedido not in ("Por encima", "Por debajo") and cond in ("Naciendo", "Ninguna"):
                continue
        if pedido in ("Por encima", "Por debajo") and c.get(f"ema{n}_estado", "Neutro") != pedido:
            return False
        if cond in ("Pullback a la baja", "Pullback a la alta"):
            try:
                limite_pb = float(p.get(f"ema{n}_dist", 0.5))
            except Exception:
                limite_pb = 0.5
            dist_pb = c.get(f"ema{n}_pb_baja_dist" if cond == "Pullback a la baja" else f"ema{n}_pb_alta_dist")
            if dist_pb is None or float(dist_pb) > limite_pb:
                return False
            continue
        if cond not in ("Naciendo", "Distancia", "Naciendo o distancia"):
            continue
        abajo = (pedido == "Por debajo")
        if n == 20:
            nace = bool(c.get("cruzando_ema20_abajo") if abajo else c.get("cruzando_ema20"))
        else:
            nace = bool(c.get(f"ema{n}_nace_abajo" if abajo else f"ema{n}_nace_arriba"))
        try:
            limite = float(p.get(f"ema{n}_dist", 0.5))
        except Exception:
            limite = 0.5
        dist = c.get(f"ema{n}_dist_pct")
        cerca = dist is not None and float(dist) <= limite
        if cond == "Naciendo":
            ok = nace
        elif cond == "Distancia":
            ok = cerca
        else:
            ok = nace or cerca
        if not ok:
            return False
    return True


def _timeframe_alpaca(label):
    """Convierte la selección de interfaz a un TimeFrame de Alpaca."""
    label = str(label or "1m").strip().lower()
    if label.endswith("m"):
        n = int(label[:-1])
        if n == 1:
            return TimeFrame.Minute
        return TimeFrame(n, TimeFrameUnit.Minute)
    if label.endswith("h"):
        n = int(label[:-1])
        if n == 1:
            return TimeFrame.Hour
        return TimeFrame(n, TimeFrameUnit.Hour)
    if label == "1d":
        return TimeFrame.Day
    if label == "1w":
        return TimeFrame.Week
    if label == "1mo":
        return TimeFrame.Month
    return TimeFrame.Minute


def _ttl_tecnico(label):
    """Cada cuánto recalcular EMA/MACD de un ticker: las velas largas cambian más lento."""
    label = str(label or "1m").strip().lower()
    try:
        if label.endswith("mo"):
            seg = 86400
        elif label.endswith("m"):
            seg = int(label[:-1]) * 60
        elif label.endswith("h"):
            seg = int(label[:-1]) * 3600
        else:
            seg = 86400
    except Exception:
        seg = 60
    return max(TTL_TECNICO_SEGUNDOS, min(120, seg / 6))


def _inicio_historial(timeframe_label):
    """Inicio de la ventana de velas según la temporalidad elegida.

    Objetivo: ~200 velas de historial en TODAS las temporalidades (EMA20/MACD
    necesitan ~40 y las EMA50/200 informativas se calculan si hay suficientes).
    Antes las temporalidades en minutos usaban siempre 3 días, por lo que 30m
    (y a veces 15m) se quedaban sin velas suficientes y nunca daban señal.
    Se cuenta en sesiones extendidas de 16 h (04:00-20:00 ET = 960 min).
    """
    label = str(timeframe_label or "1m").strip().lower()
    minutos = None
    try:
        if label.endswith("mo"):
            minutos = None
        elif label.endswith("m"):
            minutos = int(label[:-1])
        elif label.endswith("h"):
            minutos = int(label[:-1]) * 60
    except Exception:
        minutos = None

    if minutos:
        # sesiones hábiles hacia atrás (mínimo 1 = la sesión anterior completa)
        sesiones = max(1, min(25, -(-(200 * minutos) // 960)))
        d = datetime.now(ET).date()
        atras = 0
        while atras < sesiones:
            d -= timedelta(days=1)
            if d.weekday() < 5:
                atras += 1
        return datetime.combine(d, dt_time(4, 0), tzinfo=ET).astimezone(timezone.utc)
    if label == "1d":
        return datetime.now(timezone.utc) - timedelta(days=500)
    if label == "1w":
        return datetime.now(timezone.utc) - timedelta(days=2500)
    # 1 MES necesita al menos ~220 velas para EMA200 + señal.
    return datetime.now(timezone.utc) - timedelta(days=9000)


def descargar_cierres(data_client, tickers, timeframe_label="1m"):
    """Descarga OHLC de Alpaca usando la temporalidad seleccionada."""
    salida = {}
    if not tickers:
        return salida

    for i in range(0, len(tickers), 30):
        lote = tickers[i:i + 30]
        try:
            # En Alpaca Basic conservamos el retraso histórico de ~20 minutos
            # que ya utilizaba la aplicación. La condición EMA20 se evalúa
            # sobre la última vela disponible de ese histórico.
            tf = _timeframe_alpaca(timeframe_label)
            inicio = _inicio_historial(timeframe_label)
            # Respetamos el mismo limitador de ritmo de Alpaca que los snapshots.
            # Sin esto, los 10 lotes técnicos podían salir de golpe y provocar
            # respuestas 429/errores intermitentes justo cuando había candidatos.
            if hasattr(data_client, "_scanner_rate_wait"):
                data_client._scanner_rate_wait()
            # SIN limit: en peticiones con varios símbolos, limit=10000 cuenta el TOTAL
            # de velas (ordenadas por símbolo). Con 30 tickers x cientos de velas el
            # tope se agotaba y los últimos tickers del lote llegaban sin velas (o con
            # velas viejas), dejando el scanner en 0. Sin limit, alpaca-py pagina
            # hasta traer todo.
            solicitud = StockBarsRequest(
                symbol_or_symbols=lote,
                timeframe=tf,
                start=inicio,
            )
            _fin_retraso = datetime.now(timezone.utc) - timedelta(minutes=16)
            if getattr(data_client, "_bars_retraso", False):
                solicitud = StockBarsRequest(symbol_or_symbols=lote, timeframe=tf, start=inicio, end=_fin_retraso)
            try:
                _owner_metricas = getattr(data_client, "_scanner_metricas_owner", None); _t_metric_bars = time.monotonic()
                if _owner_metricas is not None: _owner_metricas._metrica_sumar("bars")
                barras = data_client.get_stock_bars(solicitud)
            except Exception as _e1:
                _m1 = str(_e1).lower()
                if (not getattr(data_client, "_bars_retraso", False)) and ("subscription" in _m1 or "sip" in _m1):
                    # Plan sin datos SIP recientes: reintenta con ~16 min de retraso y lo recuerda.
                    data_client._bars_retraso = True
                    solicitud = StockBarsRequest(symbol_or_symbols=lote, timeframe=tf, start=inicio, end=_fin_retraso)
                    barras = data_client.get_stock_bars(solicitud)
                else:
                    raise
            datos = getattr(barras, "df", None)
            if _owner_metricas is not None: _owner_metricas._metrica_tiempo("tiempo_bars", _t_metric_bars)
        except Exception as e:
            try:
                if "_owner_metricas" in locals() and _owner_metricas is not None:
                    _owner_metricas._metrica_tiempo("tiempo_bars", _t_metric_bars); _owner_metricas._metrica_sumar("errores_alpaca")
                    if "429" in str(e).lower() or "rate limit" in str(e).lower(): _owner_metricas._metrica_sumar("alpaca_429")
            except Exception: pass
            print(f"⚠️ Error descargando velas de Alpaca (lote {len(lote)}): {e}")
            try:
                data_client._ultimo_error_barras = str(e)
            except Exception:
                pass
            continue

        if datos is None or datos.empty:
            continue

        try:
            if isinstance(datos.index, pd.MultiIndex):
                for ticker in lote:
                    try:
                        marco = datos.xs(ticker, level=0)[["open", "high", "low", "close"]].dropna()
                        marco = marco.sort_index()
                    except Exception:
                        continue
                    if len(marco) >= 40:
                        salida[ticker] = marco
            else:
                if all(col in datos.columns for col in ("open", "high", "low", "close")) and len(lote) == 1:
                    marco = datos[["open", "high", "low", "close"]].dropna().sort_index()
                    if len(marco) >= 40:
                        salida[lote[0]] = marco
        except Exception:
            continue
    return salida

def _filtro_activo(p, clave, defecto=False):
    """Determina si un filtro opcional está activo; acepta bool y valores de URL."""
    v = p.get(clave, defecto)
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "on", "si", "sí", "yes")

def filtrar_resultados(filas, p):
    resultado = []
    for c in filas:
        if not (p["precio_min"] <= c["precio"] <= p["precio_max"]):
            continue
        # GAP, flotación y volumen dejaron de ser filtros obligatorios.
        if _filtro_activo(p, "gap_activo", _filtro_activo(p, "f_gap_on", False)):
            gap = c.get("gap_pct")
            if gap is None or not (float(p.get("gap_min", 3.0)) <= float(gap) <= float(p.get("gap_max", 50.0))):
                continue
        if _filtro_activo(p, "flotacion_activa", _filtro_activo(p, "f_float_on", False)):
            if c.get("float_shares") is None or float(c.get("float_shares")) > float(p.get("flotacion_max", 20_000_000)):
                continue
        if _filtro_activo(p, "volumen_activo", _filtro_activo(p, "f_vol_on", False)):
            if c.get("volumen_dia", 0) < p.get("volumen_min", 15_000):
                continue
        # MACD según el selector (Positivo por defecto).
        if not cumple_macd(c, p):
            continue
        # RSI (14): solo se exige cuando el rango se aparta de 0-100.
        try:
            _rmin = float(p.get("rsi_min", 0.0))
            _rmax = float(p.get("rsi_max", 100.0))
        except (TypeError, ValueError):
            _rmin, _rmax = 0.0, 100.0
        if _rmin > 0.0 or _rmax < 100.0:
            _rsi = c.get("rsi")
            if _rsi is None or not (_rmin <= float(_rsi) <= _rmax):
                continue
        # Pestañas EMA20 / EMA50 / EMA200: estado + condición (naciendo / distancia).
        # Por defecto EMA20 = "Naciendo" (regla original); EMA50/200 sin condición.
        if not cumple_condiciones_ema(c, p):
            continue
        resultado.append(c)

    claves = {
        "Actualizado": lambda x: x["actualizado"],
        "Cambio %": lambda x: x["cambio_pct"],
        "Volumen": lambda x: x["volumen_dia"],
    }
    resultado.sort(key=claves.get(p.get("orden", "Actualizado"), claves["Actualizado"]), reverse=True)
    # Tolerante a configuraciones antiguas sin top_n. Esto evita que una
    # ausencia de esa clave aborte el ciclo completo del scanner.
    try:
        limite = max(1, int(p.get("top_n", 10)))
    except (TypeError, ValueError):
        limite = 10
    return resultado[:limite]


def filtrar_eventos(eventos, p):
    """Filtros numéricos del usuario aplicados al cuadro de eventos (sin EMA/MACD/Top N)."""
    salida = []
    for e in eventos:
        if not (p["precio_min"] <= e["precio"] <= p["precio_max"]):
            continue
        if _filtro_activo(p, "gap_activo", _filtro_activo(p, "f_gap_on", False)):
            gap = e.get("gap_pct")
            if gap is None or not (p["gap_min"] <= gap <= p["gap_max"]):
                continue
        if _filtro_activo(p, "flotacion_activa", _filtro_activo(p, "f_float_on", False)):
            if e.get("float_shares") is None or e.get("float_shares") > p["flotacion_max"]:
                continue
        if _filtro_activo(p, "volumen_activo", _filtro_activo(p, "f_vol_on", False)):
            if e.get("volumen_dia", 0) < p.get("volumen_min", 15_000):
                continue
        salida.append(e)
    return salida


# Yahoo Finance es solo respaldo. Estos controles son GLOBALES al proceso
# para que una recreación de ServicioScanner no vuelva a golpear Yahoo después
# de un 401/403/429 ya detectado.
_YAHOO_FALLBACK_LOCK = threading.Lock()
_YAHOO_FALLBACK_PAUSADO_HASTA = 0.0
_YAHOO_FALLBACK_ULTIMA_PETICION = 0.0

# ==========================================
# ⚡️ MOTOR COMPARTIDO (un solo hilo para TODOS los usuarios)
# ==========================================
class ServicioScanner:
    def __init__(self, api_key, secret_key, tg_token, tg_chat, fmp_api_key, filtros_dueno):
        self.api_key = api_key
        self.secret_key = secret_key
        self.tg_token = tg_token
        self.tg_chat = tg_chat
        self.fmp_api_key = fmp_api_key
        self.filtros_dueno = filtros_dueno
        self.sesion = "TODO EL MERCADO"
        self.timeframe = filtros_dueno.get("timeframe", "1m")
        self.ema_dist_max = float(filtros_dueno.get("ema_dist_max", 0.0))

        self.trading = TradingClient(api_key, secret_key)
        self.data = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)

        # Motor de velas en tiempo real: una sola conexión compartida y solo
        # para los candidatos que el scanner publica. No toma decisiones de trading.
        self.market_stream = AlpacaMarketStream(api_key, secret_key, feed="iex", max_symbols=30)

        self.motor_velas = None
        if MotorVelasBridge is not None:
            try:
                self.motor_velas = MotorVelasBridge(
                    api_key,
                    secret_key,
                    market_stream=self.market_stream,
                )
            except Exception as _e_mv:
                print(f"⚠️ Motor de velas no pudo iniciar: {_e_mv}")

        # Bot LONG: se carga de forma tolerante para que un fallo del componente
        # experimental del robot NO derribe la pagina completa del scanner.
        self.bot_long = None
        self.bot_long_error = None
        try:
            if self.motor_velas is None:
                raise RuntimeError("motor de velas no disponible")
            from BotTradeScanner.integracion.bot_long_realtime import BotLongRealtime
            self.bot_long = BotLongRealtime(self.motor_velas, intervalo_segundos=1.0)
            self.bot_long.iniciar()
        except Exception as exc:
            self.bot_long_error = str(exc)
            print(f"⚠️ Bot LONG no pudo iniciar: {exc}")

        self.encendido = cargar_estado_motor_guardado()
        # Control manual del administrador: si se apaga, el horario automático NO lo vuelve a encender.
        # El estado ON/OFF se carga desde disco para que un reinicio no lo vuelva a encender.
        self.hora_inicio_auto_min, self.hora_fin_auto_min = cargar_horario_guardado()
        self.resultados = []
        self.ultima_actualizacion = None
        self.duracion_ciclo = None
        self.ultimo_error = None
        self.n_radar_base = 0
        self.universo = []
        self.universo_ts = 0.0

        self.calendario_ts = 0.0
        self.dias_mercado_cache = set()
        self.auto_en_horario = False
        self.auto_motivo = "Esperando calendario de Alpaca"
        self.float_pendientes = 0
        self.float_sin_dato = 0

        # Diagnóstico temporal del embudo de filtros (visible solo al administrador).
        self.diagnostico_filtros = {
            "radar_base": 0,
            "tras_float": 0,
            "float_sin_dato": 0,
            "float_excede": 0,
            "tras_vol_rel": 0,
            "ema_arriba": 0,
            "macd_positivo": 0,
            "ema_y_macd": 0,
            "resultados": 0,
            "raw_tickers": [],
            "final_tickers_mismo_ciclo": [],
            "eliminados_post_ema_macd": [],
            "eliminados_post_ema_macd_count": 0,
            "gap_aplicado": True,
            "gap_min": self.filtros_dueno.get("gap_min", BASE_GAP_MIN),
            "gap_max": self.filtros_dueno.get("gap_max", BASE_GAP_MAX),
        }

        self.tg_msg_id = None
        self.tg_ultimo_hash = None
        self.telegram_estado = "No probado"
        self.telegram_ultimo_error = None

        self.eventos = []                    # cuadro "Eventos en vivo" (el más nuevo primero)
        self._ultimo_precio_evento = {}
        self.historial_ciclos = []            # últimos ciclos: permite ver cuándo entran/salen candidatos
        self._raw_tickers_ciclo_anterior = set()

        # PRUEBA 6: seguimiento temporal de señales EMA20+MACD.
        # Cada señal se observa durante una ventana fija y se conserva
        # el máximo precio visto para calcular MFE. No afecta filtros.
        self.prueba6_activos = {}
        self.prueba6_completadas = []

        self.cache_tecnico = {}
        # Estado POR TEMPORALIDAD: cada pantalla ve los resultados de la suya.
        self.tf_principal = str(self.timeframe or "1m").lower()
        self.tfs_activos = {self.tf_principal: time.time()}
        self.resultados_por_tf = {}
        # Pool técnico amplio y compartido; cada usuario filtra su propia vista.
        self.pool_por_tf = {}
        self.diag_por_tf = {}
        self.cache_tecnico_por_tf = {}
        self.cache_ema_extra_por_tf = {}
        self._raw_prev_por_tf = {}
        # Filtros (precio, gap, float, volumen...) que el usuario tiene activos EN CADA
        # temporalidad. Así 1m y 15m pueden buscar con condiciones distintas a la vez.
        self.filtros_por_tf = {}
        self._ultimos_snapshots = None
        self._despertar = threading.Event()
        self.cache_fund = self._leer_cache_fundamentales()
        # Control específico de FMP para no martillar la API cuando devuelve HTTP 429.
        self.fmp_pausado_hasta = 0.0
        self._ultima_peticion_fmp = 0.0
        self._bulk_float_running = False
        self._bulk_float_lock = threading.Lock()
        self._lock_fmp = threading.Lock()
        # Yahoo es solo respaldo; evitar que varios workers disparen .info simultaneamente.
        self._lock_yahoo = threading.Lock()

        self._lock_ritmo = threading.Lock()
        self._ultima_peticion = 0.0

        # Control del hilo para permitir un reinicio limpio desde el panel de administrador.
        self._detener_hilo = threading.Event()
        self._lock_reinicio = threading.Lock()
        self._metricas_lock = threading.Lock()
        self.metricas = {
            "inicio": time.time(), "ciclos": 0, "ciclos_por_tf": {},
            "duracion_ciclo_total": 0.0, "duracion_ciclo_min": None, "duracion_ciclo_max": 0.0,
            "universo_cargas": 0, "simbolos_universo": 0, "snapshots": 0, "bars": 0, "calendario": 0,
            "fmp_bulk": 0, "fmp_individual": 0, "fmp_429": 0, "fmp_errores": 0,
            "fmp_cache_hits": 0, "fmp_cache_misses": 0, "errores_alpaca": 0, "alpaca_429": 0,
            "tiempo_snapshots": 0.0, "tiempo_bars": 0.0, "tiempo_fmp": 0.0,
            "resultados_publicados": 0, "simbolos_procesados": 0,
            "ultimo_ciclo_ts": None, "ultimo_ciclo_duracion": None,
        }
        self.data._scanner_metricas_owner = self
        self._hilo = threading.Thread(target=self._bucle, daemon=True)
        self._hilo.start()

    # ---------- puente scanner -> motor de velas ----------
    def _sincronizar_motor_velas(self, resultados):
        """Entrega al motor de velas únicamente los candidatos publicados.

        El motor de velas observa trades y arma velas; no decide entradas/salidas.
        La lógica del bot real queda desacoplada y podrá consumir estos snapshots.
        """
        try:
            # BotLongRealtime sincroniza a su vez el puente de market-data.
            # Asi evitamos suscribir/procesar los mismos candidatos dos veces.
            if self.bot_long is not None:
                self.bot_long.sync_candidates(resultados)
        except Exception as exc:
            self.bot_long_error = str(exc)
            print(f"⚠️ Puente motor/bot LONG: {exc}")

    def estado_bot_long(self):
        """Estado publico/visual del bot LONG; no expone credenciales."""
        try:
            if self.bot_long is None:
                return {
                    "disponible": False,
                    "hilo_vivo": False,
                    "error": self.bot_long_error or "Bot LONG no inicializado",
                    "candidatos": [],
                    "ciclos": 0,
                    "posiciones_paper": [],
                    "posiciones_observadas": [],
                    "decisiones_guardadas": 0,
                }
            estado = dict(self.bot_long.status())
            estado["disponible"] = True
            if self.bot_long_error and not estado.get("ultimo_error"):
                estado["ultimo_error"] = self.bot_long_error
            return estado
        except Exception as exc:
            return {
                "disponible": False,
                "hilo_vivo": False,
                "error": str(exc),
                "candidatos": [],
                "ciclos": 0,
                "posiciones_paper": [],
                "posiciones_observadas": [],
                "decisiones_guardadas": 0,
            }

    def decisiones_bot_long(self, limite=20):
        try:
            if self.bot_long is None:
                return []
            return self.bot_long.decisiones_recientes(limite)
        except Exception:
            return []

    def snapshot_motor_velas(self, ticker):
        try:
            return self.motor_velas.snapshot(ticker)
        except Exception:
            return {"simbolo": str(ticker).upper(), "sin_datos": True}

    def estado_motor_velas(self):
        try:
            return self.motor_velas.status()
        except Exception as exc:
            return {"stream_hilo_vivo": False, "error": str(exc)}

    # ---------- instrumentación Fase 1 ----------
    def _metrica_sumar(self, clave, valor=1):
        try:
            with self._metricas_lock:
                self.metricas[clave] = self.metricas.get(clave, 0) + valor
        except Exception: pass

    def _metrica_tiempo(self, clave, inicio):
        self._metrica_sumar(clave, max(0.0, time.monotonic() - inicio))

    def metricas_snapshot(self):
        with self._metricas_lock:
            m = dict(self.metricas); m["ciclos_por_tf"] = dict(self.metricas.get("ciclos_por_tf", {}))
        m["uptime_segundos"] = max(0.0, time.time() - float(m.get("inicio", time.time())))
        m["ciclo_promedio"] = m["duracion_ciclo_total"] / m["ciclos"] if m.get("ciclos") else 0.0
        m["fmp_total"] = m.get("fmp_bulk", 0) + m.get("fmp_individual", 0)
        return m

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

    # ---------- horario automático ----------
    def _actualizar_calendario(self, ahora_et):
        """Consulta el calendario de Alpaca y lo guarda en memoria."""
        if self.dias_mercado_cache and time.time() - self.calendario_ts < TTL_CALENDARIO_MERCADO:
            return
        try:
            inicio = ahora_et.date()
            fin = inicio + timedelta(days=14)
            calendario = self.trading.get_calendar(
                GetCalendarRequest(start=inicio, end=fin)
            )
            self.dias_mercado_cache = {c.date for c in calendario}
            self.calendario_ts = time.time()
            self._metrica_sumar("calendario")
        except Exception as e:
            # El calendario es una ayuda para evitar ejecutar en fines de semana/feriados,
            # pero un fallo temporal de la consulta de calendario NO debe detener el scanner.
            # En ese caso usamos un fallback seguro de lunes a viernes y dejamos el error
            # visible en diagnóstico.
            self.ultimo_error = f"Calendario Alpaca: {e}"
            print(f"⚠️ Error consultando calendario de Alpaca; usando fallback L-V: {e}")
            self.dias_mercado_cache = {
                ahora_et.date() + timedelta(days=i)
                for i in range(15)
                if (ahora_et.date() + timedelta(days=i)).weekday() < 5
            }
            self.calendario_ts = time.time()

    def configurar_modo_operacion(self, sesion, timeframe, ema_dist_max=1.0, principal=False, filtros=None):
        # El scanner trabaja siempre en una única ventana continua 04:00–20:00 ET.
        # La temporalidad ya NO se comparte: cada una tiene sus propias velas,
        # su caché y sus resultados, así que cambiarla no borra nada de las demás.
        try:
            nueva_distancia = max(0.0, float(ema_dist_max))
        except Exception:
            nueva_distancia = 1.0
        self.sesion = "TODO EL MERCADO"
        self.ema_dist_max = nueva_distancia
        self.filtros_dueno["sesion"] = self.sesion
        self.filtros_dueno["ema_dist_max"] = self.ema_dist_max
        self.registrar_timeframe(timeframe, principal=principal, filtros=filtros)

    def registrar_timeframe(self, tf, principal=False, filtros=None):
        """Marca una temporalidad como pedida por alguna pantalla.

        Si se pasan `filtros`, quedan asociados a ESA temporalidad: el motor los usa
        al escanearla, sin mezclarlos con los de otras temporalidades."""
        tf = str(tf or "1m").lower()
        es_nueva = tf not in self.resultados_por_tf
        self.tfs_activos[tf] = time.time()
        if principal:
            self.tf_principal = tf
            self.timeframe = tf
            self.filtros_dueno["timeframe"] = tf
        filtros_cambiaron = False
        if isinstance(filtros, dict):
            nuevos = dict(filtros)
            nuevos["timeframe"] = tf
            filtros_cambiaron = (self.filtros_por_tf.get(tf) != nuevos)
            self.filtros_por_tf[tf] = nuevos
        if es_nueva or filtros_cambiaron:
            self._despertar.set()   # calcularla ya, sin esperar al siguiente ciclo

    def _filtros_para(self, tf):
        """Filtros del motor global para una temporalidad."""
        f = dict(self.filtros_dueno)
        propios = self.filtros_por_tf.get(str(tf or "").lower())
        if propios:
            f.update(propios)
        return f

    def _timeframes_a_procesar(self):
        ahora = time.time()
        principal = str(self.tf_principal).lower()
        vigentes = [(tf, ts) for tf, ts in list(self.tfs_activos.items()) if ahora - ts <= VIGENCIA_TIMEFRAME_ACTIVO]
        vigentes.sort(key=lambda x: x[1], reverse=True)
        lista = [principal]
        for tf, _ts in vigentes:
            if tf not in lista and len(lista) < MAX_TIMEFRAMES_ACTIVOS:
                lista.append(tf)
        # Liberar memoria de temporalidades que nadie usa desde hace rato.
        for tf in list(self.tfs_activos.keys()):
            if tf not in lista and ahora - self.tfs_activos.get(tf, 0) > VIGENCIA_TIMEFRAME_ACTIVO:
                self.tfs_activos.pop(tf, None)
                self.resultados_por_tf.pop(tf, None)
                self.diag_por_tf.pop(tf, None)
                self.cache_tecnico_por_tf.pop(tf, None)
                self.cache_ema_extra_por_tf.pop(tf, None)
                self._raw_prev_por_tf.pop(tf, None)
                self.filtros_por_tf.pop(tf, None)
        return lista

    def _esta_en_horario_automatico(self):
        """True solo de 04:00 a 16:00 ET en un día de mercado según Alpaca."""
        ahora_et = datetime.now(ET)
        self._actualizar_calendario(ahora_et)
        es_dia_mercado = ahora_et.date() in self.dias_mercado_cache
        minuto_actual = ahora_et.hour * 60 + ahora_et.minute + ahora_et.second / 60
        # La ventana global se mantiene en el estado del motor y solo la administra
        # el administrador. Cada usuario normal aplica su propia ventana a su vista.
        inicio = int(getattr(self, "hora_inicio_auto_min", 4 * 60))
        fin = int(getattr(self, "hora_fin_auto_min", 20 * 60))
        self.hora_inicio_auto_min = inicio
        self.hora_fin_auto_min = fin
        if inicio == fin:
            en_ventana = False
        elif inicio < fin:
            en_ventana = inicio <= minuto_actual < fin
        else:
            # Permite horarios que crucen medianoche, por ejemplo 22:00–04:00.
            en_ventana = minuto_actual >= inicio or minuto_actual < fin
        self.auto_en_horario = bool(es_dia_mercado and en_ventana)
        inicio_txt = f"{inicio // 60:02d}:{inicio % 60:02d}"
        fin_txt = f"{fin // 60:02d}:{fin % 60:02d}"
        if self.auto_en_horario:
            self.auto_motivo = f"Horario automático activo · {inicio_txt}–{fin_txt} ET"
        elif not es_dia_mercado:
            self.auto_motivo = "Fuera de día de mercado según Alpaca"
        else:
            self.auto_motivo = f"Fuera del horario automático · {inicio_txt}–{fin_txt} ET"
        return self.auto_en_horario

    def configurar_horario(self, inicio=None, fin=None):
        """Configura el horario global; solo el administrador debe llamarlo."""
        try:
            inicio = int(inicio); fin = int(fin)
        except Exception:
            inicio, fin = 4 * 60, 20 * 60
        inicio = max(0, min(1439, inicio)); fin = max(0, min(1439, fin))
        self.hora_inicio_auto_min = inicio
        self.hora_fin_auto_min = fin
        guardar_horario_en_disco(inicio, fin)
        self.auto_motivo = f"Ventana global · {inicio//60:02d}:{inicio%60:02d}–{fin//60:02d}:{fin%60:02d} ET"

    def reiniciar_scanner(self):
        """Reinicia de forma segura el motor compartido del scanner.

        Detiene el hilo anterior, limpia el estado de resultados/cachés de trabajo
        y crea un único hilo nuevo. No modifica las credenciales ni el horario.
        """
        with self._lock_reinicio:
            hilo_anterior = self._hilo
            self._detener_hilo.set()

            # Espera brevemente a que el hilo anterior termine su ciclo actual.
            if hilo_anterior is not None and hilo_anterior.is_alive() and hilo_anterior is not threading.current_thread():
                hilo_anterior.join(timeout=max(2.0, INTERVALO_ESCANEO_SEGUNDOS + 1.0))

            # Limpia únicamente el estado operativo que puede quedar obsoleto.
            self.resultados = []
            self.ultima_actualizacion = None
            self.duracion_ciclo = None
            self.ultimo_error = None
            self.n_radar_base = 0
            self.universo = []
            self.universo_ts = 0.0
            self.calendario_ts = 0.0
            self.dias_mercado_cache = set()
            self.auto_en_horario = False
            self.auto_motivo = "Scanner reiniciado; esperando el próximo ciclo"
            self.float_pendientes = 0
            self.float_sin_dato = 0
            self.diagnostico_filtros = {
                "radar_base": 0,
                "tras_float": 0,
                "tras_vol_rel": 0,
                "ema_arriba": 0,
                "macd_positivo": 0,
                "ema_y_macd": 0,
                "resultados": 0,
                "gap_aplicado": True,
                "gap_min": self.filtros_dueno.get("gap_min", BASE_GAP_MIN),
                "gap_max": self.filtros_dueno.get("gap_max", BASE_GAP_MAX),
            }
            self.tg_msg_id = None
            self.tg_ultimo_hash = None
            self.eventos = []
            self._ultimo_precio_evento = {}
            self.historial_ciclos = []
            self._raw_tickers_ciclo_anterior = set()
            self.candidatos_ema_macd_actual = []
            self.prueba6_activos = {}
            self.prueba6_completadas = []
            self.finales_ema_macd_actual = []
            self.cache_tecnico = {}
            self.cache_tecnico_por_tf = {}
            self.cache_ema_extra_por_tf = {}
            self.pool_por_tf = {}
            self.resultados_por_tf = {}
            self.diag_por_tf = {}
            self._raw_prev_por_tf = {}
            self.fmp_pausado_hasta = 0.0
            self._ultima_peticion_fmp = 0.0
            self._ultima_peticion = 0.0

            # Nuevo hilo único. Las credenciales y la configuración permanecen intactas.
            self._detener_hilo.clear()
            self._hilo = threading.Thread(target=self._bucle, daemon=True)
            self._hilo.start()

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
            self._metrica_sumar("universo_cargas")
            self._metrica_sumar("simbolos_universo", len(self.universo))
            print(f"🌐 Universo cargado: {len(self.universo)} tickers.")
        except Exception as e:
            self.ultimo_error = f"Universo: {e}"
            print(f"⚠️ Error cargando universo: {e}")

    # ---------- snapshots en paralelo ----------
    def _descargar_snapshots(self):
        lotes = [self.universo[i:i + TAMANO_LOTE_SNAPSHOT] for i in range(0, len(self.universo), TAMANO_LOTE_SNAPSHOT)]

        def pedir(lote):
            self._esperar_turno(); _t_metric = time.monotonic(); self._metrica_sumar("snapshots")
            try:
                _resp = self.data.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=lote)) or {}
                self._metrica_tiempo("tiempo_snapshots", _t_metric); return _resp
            except Exception as e:
                self._metrica_tiempo("tiempo_snapshots", _t_metric); self._metrica_sumar("errores_alpaca")
                if "429" in str(e).lower() or "rate limit" in str(e).lower(): self._metrica_sumar("alpaca_429")
                self.ultimo_error = f"Snapshot: {e}"; return {}

        snapshots = {}
        with ThreadPoolExecutor(max_workers=WORKERS_SNAPSHOT) as ex:
            for parcial in ex.map(pedir, lotes):
                snapshots.update(parcial)
        return snapshots

    # ---------- float y volumen promedio (FMP + Alpaca; sin Yahoo Finance) ----------
    @staticmethod
    def _extraer_float_fmp(payload):
        """Extrae el float de las distintas formas de respuesta de FMP."""
        claves = ("floatShares", "float_shares", "freeFloatShares", "freeFloat")

        def convertir(valor):
            if valor is None or isinstance(valor, bool):
                return None
            try:
                if isinstance(valor, str):
                    valor = valor.replace(",", "").strip()
                numero = float(valor)
                return numero if numero > 0 else None
            except (TypeError, ValueError):
                return None

        def buscar(obj):
            if isinstance(obj, dict):
                for clave in claves:
                    numero = convertir(obj.get(clave))
                    if numero is not None:
                        return numero
                for valor in obj.values():
                    numero = buscar(valor)
                    if numero is not None:
                        return numero
            elif isinstance(obj, list):
                for item in obj:
                    numero = buscar(item)
                    if numero is not None:
                        return numero
            return None

        return buscar(payload)

    def _actualizar_float_bulk(self):
        """Carga la tabla masiva de float de FMP y la mezcla con la caché local.

        El scanner no debe depender de 1-3 consultas individuales por ciclo para
        conocer el float. FMP publica un endpoint All Shares Float con hasta 5000
        registros por página; lo usamos como caché de referencia y dejamos la
        consulta individual solo como respaldo para símbolos que no aparezcan.
        """
        if not self.fmp_api_key:
            return False
        ahora = time.time()
        try:
            _meta_b = self.cache_fund.get("__bulk_meta__", {}) or {}
            ultima_bulk = float(_meta_b.get("ts", 0))
            _enc_prev = int(_meta_b.get("encontrados", 0) or 0)
        except Exception:
            ultima_bulk = 0.0
            _enc_prev = 0
        # Si la ultima carga no encontro NINGUN float (clave mala, limite 429, etc.) se
        # reintenta en ~15 min en vez de esperar 12 horas con el scanner sin datos de float.
        _ventana_bulk = FMP_BULK_FLOAT_TTL if _enc_prev > 0 else 7200
        if ahora - ultima_bulk < _ventana_bulk:
            return False
        try:
            universo_set = set(self.universo or [])
            encontrados = 0
            paginas = 0
            for pagina in range(FMP_BULK_MAX_PAGES):
                # Respetamos el mismo ritmo de FMP que las consultas individuales.
                with self._lock_fmp:
                    espera = self._ultima_peticion_fmp + FMP_BULK_MIN_INTERVAL_SEGUNDOS - time.time()
                    if espera > 0:
                        time.sleep(espera)
                    self._ultima_peticion_fmp = time.time()
                    self._metrica_sumar("fmp_bulk"); _t_metric_fmp = time.monotonic()
                    respuesta = requests.get(
                        FMP_BULK_FLOAT_URL,
                        params={
                            "page": pagina,
                            "limit": FMP_BULK_PAGE_SIZE,
                            "apikey": self.fmp_api_key,
                        },
                        timeout=20,
                    )
                self._metrica_tiempo("tiempo_fmp", _t_metric_fmp)
                if respuesta.status_code == 429: self._metrica_sumar("fmp_429")
                if respuesta.status_code == 429:
                    self.fmp_pausado_hasta = time.time() + PAUSA_FMP_429_SEGUNDOS
                    self.ultimo_error = "FMP bulk devolvió HTTP 429; se usará la caché existente y luego el endpoint individual."
                    break
                if respuesta.status_code in (401, 403):
                    self._metrica_sumar("fmp_errores")
                    self.ultimo_error = f"FMP bulk rechazó la API (HTTP {respuesta.status_code}); se mantiene el respaldo individual."
                    break
                if respuesta.status_code != 200:
                    self._metrica_sumar("fmp_errores")
                    self.ultimo_error = f"FMP bulk devolvió HTTP {respuesta.status_code}; se mantiene la caché existente."
                    break
                try:
                    payload = respuesta.json()
                except ValueError:
                    self.ultimo_error = "FMP bulk devolvió una respuesta no JSON."
                    break
                if not isinstance(payload, list) or not payload:
                    break
                paginas += 1
                for item in payload:
                    if not isinstance(item, dict):
                        continue
                    ticker = str(item.get("symbol") or "").strip().upper()
                    if not ticker or (universo_set and ticker not in universo_set):
                        continue
                    valor = self._extraer_float_fmp(item)
                    if valor is None:
                        continue
                    self.cache_fund[ticker] = {
                        "float": float(valor),
                        "float_source": "FMP bulk",
                        "float_status": "ok",
                        "ts": ahora,
                    }
                    encontrados += 1
                if len(payload) < FMP_BULK_PAGE_SIZE:
                    break
            self.cache_fund["__bulk_meta__"] = {"ts": ahora, "paginas": paginas, "encontrados": encontrados}
            self._guardar_cache_fundamentales()
            if encontrados:
                self.ultimo_error = None
                print(f"✓ FMP bulk float: {encontrados} símbolos del universo actualizados ({paginas} páginas).")
            return encontrados > 0
        except Exception as e:
            self.ultimo_error = f"Error FMP bulk float: {e}"
            print(f"⚠️ FMP bulk float: {e}")
            return False
        finally:
            self._bulk_float_running = False

    def _float_fmp(self, ticker):
        if not self.fmp_api_key:
            self.ultimo_error = "FMP_API_KEY no está configurada en Streamlit Secrets; no se puede obtener el float."
            return None

        ahora = time.time()
        if ahora < self.fmp_pausado_hasta:
            restante = max(1, int(self.fmp_pausado_hasta - ahora))
            minutos = restante // 60 + (1 if restante % 60 else 0)
            self.ultimo_error = (
                "FMP está en pausa por límite de solicitudes (HTTP 429). "
                f"Se reintentará en aproximadamente {minutos} min."
            )
            return None

        try:
            # FMP tiene un límite separado del ritmo de Alpaca. Espaciamos las
            # consultas para evitar una cascada de HTTP 429.
            with self._lock_fmp:
                ahora = time.time()
                espera_fmp = self._ultima_peticion_fmp + FMP_MIN_INTERVAL_SEGUNDOS - ahora
                if espera_fmp > 0:
                    time.sleep(espera_fmp)
                self._ultima_peticion_fmp = time.time()
                self._metrica_sumar("fmp_individual"); _t_metric_fmp = time.monotonic()
                respuesta = requests.get(
                    FMP_API_URL,
                    params={"symbol": ticker, "apikey": self.fmp_api_key},
                    timeout=8,
                )
            self._metrica_tiempo("tiempo_fmp", _t_metric_fmp)
            if respuesta.status_code == 429: self._metrica_sumar("fmp_429")
            if respuesta.status_code == 429:
                self.fmp_pausado_hasta = time.time() + PAUSA_FMP_429_SEGUNDOS
                self.ultimo_error = (
                    f"FMP devolvió HTTP 429 para {ticker}. "
                    "Se pausaron las consultas de float durante 15 minutos para evitar más bloqueos."
                )
                print(f"⚠️ FMP HTTP 429 para {ticker}; pausa de {PAUSA_FMP_429_SEGUNDOS}s")
                return None
            if respuesta.status_code in (401, 403):
                self._metrica_sumar("fmp_errores")
                self.ultimo_error = (
                    f"FMP rechazó la API para {ticker} (HTTP {respuesta.status_code}). "
                    "Revisa que FMP_API_KEY sea válida y tenga acceso a shares-float."
                )
                return None
            if respuesta.status_code != 200:
                self._metrica_sumar("fmp_errores")
                self.ultimo_error = f"FMP devolvió HTTP {respuesta.status_code} para {ticker}."
                return None
            try:
                payload = respuesta.json()
            except ValueError:
                self.ultimo_error = f"FMP devolvió una respuesta no JSON para {ticker}."
                return None
            valor = self._extraer_float_fmp(payload)
            if valor is None:
                print(f"⚠️ FMP sin float para {ticker}")
            else:
                self.ultimo_error = None
            return valor
        except Exception as e:
            self.ultimo_error = f"Error consultando FMP para {ticker}: {e}"
            print(f"⚠️ FMP float {ticker}: {e}")
            return None

    def _float_yahoo(self, ticker):
        """Yahoo pausado temporalmente: no realiza peticiones externas."""
        self._yahoo_estado = "desactivado temporalmente"
        return None

    def _circulacion_finnhub(self, ticker):
        """Respaldo 2: acciones en circulacion (Finnhub). La flotacion nunca es mayor que este numero,
        por eso solo se usa para APROBAR con seguridad el filtro de flotacion, nunca para descartar."""
        clave = getattr(self, "finnhub_api_key", None)
        if not clave:
            return None
        if getattr(self, "_finnhub_pausado_hasta", 0) > time.time():
            return None
        try:
            espera = getattr(self, "_finnhub_ultima", 0.0) + 1.1 - time.time()
            if espera > 0:
                time.sleep(espera)
            self._finnhub_ultima = time.time()
            r = requests.get(
                "https://finnhub.io/api/v1/stock/profile2",
                params={"symbol": ticker, "token": clave},
                timeout=8,
            )
            if r.status_code == 429:
                self._finnhub_pausado_hasta = time.time() + 120
                self._finnhub_estado = "HTTP 429 (pausa 2 min)"
                return None
            if r.status_code in (401, 403):
                self._finnhub_pausado_hasta = time.time() + 3600
                self._finnhub_estado = f"clave rechazada (HTTP {r.status_code})"
                return None
            if r.status_code != 200:
                self._finnhub_estado = f"HTTP {r.status_code}"
                return None
            datos = r.json() or {}
            v = datos.get("shareOutstanding")  # Finnhub lo entrega en millones
            self._finnhub_estado = "ok"
            if v in (None, "", 0):
                return None
            return float(v) * 1_000_000
        except Exception as e:
            self._finnhub_estado = f"error: {str(e)[:80]}"
            return None

    def _asegurar_fundamentales(self, tickers):
        ahora = time.time()
        faltan = []
        for t in tickers:
            e = self.cache_fund.get(t)
            if e is None:
                self._metrica_sumar("fmp_cache_misses")
                faltan.append(t)
                continue
            self._metrica_sumar("fmp_cache_hits")
            ts = float(e.get("ts", 0))
            if e.get("float") is None and ahora - ts > REINTENTO_FUNDAMENTALES:
                faltan.append(t)
            elif ahora - ts > VIGENCIA_FUNDAMENTALES:
                faltan.append(t)

        # Tope duro por ciclo.
        faltan = faltan[:MAX_FUNDAMENTALES_POR_CICLO]
        if not faltan:
            return

        def pedir(t):
            # Cadena de respaldo: FMP -> Yahoo (yfinance) -> Finnhub (solo acciones en circulacion).
            float_final = None
            fuente = "FMP"
            circulacion = None
            v = self._float_fmp(t)
            if v is not None:
                float_final, fuente = v, "FMP"
            else:
                v = self._float_yahoo(t)
                if v is not None:
                    float_final, fuente = v, "Yahoo"
                else:
                    circulacion = self._circulacion_finnhub(t)
            try:
                if float_final is not None:
                    float_final = float(float_final)
            except (TypeError, ValueError):
                float_final = None

            if float_final is not None:
                estado = "ok"
                if str(getattr(self, "ultimo_error", "") or "").startswith("FMP"):
                    self.ultimo_error = None
            elif circulacion is not None:
                estado, fuente = "max_circulacion", "Finnhub (circulación)"
            else:
                estado, fuente = "no_data", "FMP/Yahoo/Finnhub"

            entrada = {
                "float": float_final,
                "float_source": fuente,
                "float_status": estado,
                "ts": time.time(),
            }
            if circulacion is not None:
                entrada["outstanding"] = float(circulacion)
            return t, entrada

        with ThreadPoolExecutor(max_workers=WORKERS_FUNDAMENTALES) as ex:
            for t, entrada in ex.map(pedir, faltan):
                self.cache_fund[t] = entrada
        self._guardar_cache_fundamentales()

    # ---------- EMA20 / MACD ----------
    def _cache_ema_extra(self, tf):
        d = self.__dict__.setdefault("cache_ema_extra_por_tf", {})
        return d.setdefault(str(tf).lower(), {})

    def _asegurar_tecnico(self, tickers, tf=None):
        ahora = time.time()
        tf_actual = str(tf or getattr(self, "tf_principal", "1m")).lower()
        cache = self.cache_tecnico_por_tf.setdefault(tf_actual, {})
        ttl = _ttl_tecnico(tf_actual)
        pendientes = []
        for t in tickers:
            entrada = cache.get(t)
            if not entrada:
                pendientes.append(t)
                continue
            ts_cache = float(entrada[0]) if entrada else 0.0
            _swing_req = _filtro_activo(self._filtros_para(tf_actual), "swing_activo", False)
            _extra = self._cache_ema_extra(tf_actual).get(t, {})
            if ahora - ts_cache > ttl or (_swing_req and "swing_cross_ago" not in _extra):
                pendientes.append(t)

        if not pendientes:
            return

        # El descargador técnico usa este callback para respetar el rate-limit de Alpaca.
        try:
            self.data._scanner_rate_wait = self._esperar_turno
        except Exception:
            pass
        try:
            self.data._ultimo_error_barras = ""
        except Exception:
            pass
        series = descargar_cierres(self.data, pendientes, tf_actual)
        _err_barras = getattr(self.data, "_ultimo_error_barras", "")
        if _err_barras:
            self.ultimo_error = f"Velas Alpaca ({tf_actual}): {_err_barras}"
        extras_ema = self._cache_ema_extra(tf_actual)
        for t in pendientes:
            extras_ema[t] = evaluar_ema_condiciones(series.get(t))
            extras_ema[t].update(evaluar_swing(series.get(t)))
            (cruz_arriba, cruz_abajo, macd_pos, macd_neg, precio_act, ema_act,
             macd_val, barras_count, precio_prev, ema_prev, precio_actual,
             ema_actual, bb_upper, bb_dist_pct, rsi_val, ema50_act, ema200_act) = evaluar_tecnico(series.get(t))
            cache[t] = (
                ahora, tf_actual, cruz_arriba, cruz_abajo, macd_pos, macd_neg,
                precio_act, ema_act, macd_val, barras_count,
                precio_prev, ema_prev, precio_actual, ema_actual,
                bb_upper, bb_dist_pct, rsi_val, ema50_act, ema200_act
            )

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
                pedidos = set(tickers)
                nuevos = {}
                for n in respuesta.json().get("news", []):
                    simbolos = n.get("symbols", []) or []
                    con_noticia.update(simbolos)
                    item = {
                        "t": str(n.get("headline") or "")[:300],
                        "r": str(n.get("summary") or "")[:500],
                        "u": str(n.get("url") or ""),
                        "s": str(n.get("source") or ""),
                        "h": str(n.get("created_at") or ""),
                    }
                    for s in simbolos:
                        if s not in pedidos:
                            continue
                        lista = nuevos.setdefault(s, [])
                        if len(lista) >= 6 or (item["u"] and any(x["u"] == item["u"] for x in lista)):
                            continue
                        lista.append(item)
                detalle = self.__dict__.setdefault("noticias_detalle", {})
                detalle.update(nuevos)
                if len(detalle) > 400:
                    for _k in list(detalle.keys())[:len(detalle) - 400]:
                        detalle.pop(_k, None)
                return con_noticia & pedidos
        except Exception:
            pass
        return set()

    # ---------- Telegram ----------
    def _enviar_telegram(self, texto_tabla):
        """Telegram desactivado permanentemente en el scanner."""
        self.telegram_estado = "DESACTIVADO"
        self.telegram_ultimo_error = None
        return

    def _escribir_html(self, texto_tabla):
        contenido = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>SCANNER</title>
<meta http-equiv="refresh" content="30">
<style>body {{ background:#121212; color:#00ffcc; font-family:'Courier New',monospace; padding:20px; }}
pre {{ background:#1e1e1e; padding:25px; border-radius:8px; border:1px solid #333; color:#fff; }}</style>
</head><body><h2>SCANNER</h2><pre>{texto_tabla}</pre></body></html>"""
        try:
            with open(os.path.join(os.getcwd(), NOMBRE_ARCHIVO_HTML), "w", encoding="utf-8") as f:
                f.write(contenido)
        except Exception:
            pass

    # ---------- eventos en vivo (cuadro de abajo) ----------
    def _registrar_eventos(self, candidatos):
        """Guarda un evento cada vez que cambia el último precio de un ticker del radar.
        subiendo=True (verde): el precio subió frente al evento anterior de ese ticker.
        subiendo=False (rojo): bajó. La primera vez que aparece, se usa el cambio % del día."""
        try:
            nuevos = []
            for c in candidatos:
                previo = self._ultimo_precio_evento.get(c["ticker"])
                if previo is not None and previo == c["precio"]:
                    continue
                subiendo = (c["precio"] > previo) if previo is not None else (c["cambio_pct"] >= 0)
                self._ultimo_precio_evento[c["ticker"]] = c["precio"]
                nuevos.append({
                    "ticker": c["ticker"],
                    "precio": c["precio"],
                    "cambio_pct": c["cambio_pct"],
                    "volumen_dia": c["volumen_dia"],
                    "float_shares": c["float_shares"],
                    "volumen_relativo": c["volumen_relativo"],
                    "tiene_noticia": c["tiene_noticia"],
                    "actualizado": c["actualizado"],
                    "subiendo": subiendo,
                })
            if nuevos:
                try:
                    nuevos.sort(key=lambda ev: ev["actualizado"], reverse=True)
                except Exception:
                    pass
                self.eventos = (nuevos + self.eventos)[:MAX_EVENTOS]
        except Exception as ex:
            print(f"⚠️ Error registrando eventos: {ex}")

    def _registrar_historial_ciclo(self, enriquecidos, resultados_finales):
        """Conserva una fotografía de los candidatos por ciclo para depuración.
        No altera filtros ni resultados; solo guarda evidencia de entradas/salidas.
        """
        try:
            ahora = datetime.now(ET)
            finales = []
            for c in resultados_finales:
                finales.append({
                    "ticker": c.get("ticker"),
                    "precio": c.get("precio"),
                    "cambio_pct": c.get("cambio_pct"),
                    "volumen_dia": c.get("volumen_dia"),
                    "ema20": c.get("tecnico_ema20"),
                    "macd": c.get("tecnico_macd"),
                })
            crudos = []
            for c in enriquecidos:
                if c.get("cruzando_ema20") and c.get("macd_positivo"):
                    crudos.append({
                        "ticker": c.get("ticker"),
                        "precio": c.get("precio"),
                        "cambio_pct": c.get("cambio_pct"),
                    })
            entrada = {
                "hora": ahora.strftime("%H:%M:%S ET"),
                "radar_base": self.n_radar_base,
                "tras_float": getattr(self, "n_tras_float", 0),
                "tras_volumen": len(enriquecidos),
                "ema_arriba": sum(1 for c in enriquecidos if c.get("cruzando_ema20")),
                "macd_positivo": sum(1 for c in enriquecidos if c.get("macd_positivo")),
                "ema_y_macd": len(crudos),
                "crudos": crudos,
                "finales": finales,
            }
            self.historial_ciclos = ([entrada] + list(self.historial_ciclos))[:MAX_HISTORIAL_CICLOS]
        except Exception as ex:
            print(f"⚠️ Error guardando historial de ciclos: {ex}")

    # ---------- PRUEBA 6: MFE posterior a la señal ----------
    def _actualizar_prueba6(self, candidatos, snapshots=None):
        """Observa el máximo precio posterior a cada señal EMA20+MACD.

        La ventana es fija (VENTANA_PRUEBA6_MINUTOS) y el cálculo es
        exclusivamente diagnóstico. No elimina ni modifica candidatos.
        La señal se toma en el momento en que el scanner la detecta.
        """
        try:
            ahora = datetime.now(ET)
            ahora_ts = ahora.timestamp()
            candidatos_validos = {
                c.get("ticker"): c for c in candidatos
                if c.get("ticker") and c.get("cruzando_ema20") and c.get("macd_positivo")
            }

            # Actualizar señales ya abiertas con el precio de mercado actual,
            # incluso si el ticker dejó de cumplir EMA20+MACD en este ciclo.
            # Así medimos realmente el recorrido posterior y no solo el tiempo
            # durante el cual el candidato permanece visible.
            for ticker, obs in list(self.prueba6_activos.items()):
                precio_actual = None
                snap = (snapshots or {}).get(ticker)
                if snap is not None and getattr(snap, "latest_trade", None):
                    precio_actual = getattr(snap.latest_trade, "price", None)
                if precio_actual is None:
                    precio_actual = candidatos_validos.get(ticker, {}).get("precio")
                if precio_actual is not None:
                    obs["max_precio"] = max(float(obs["max_precio"]), float(precio_actual))

                # PRUEBA 7: registrar la primera vez que el MFE alcanza cada objetivo.
                precio_senal_obs = float(obs.get("precio_senal") or 0)
                if precio_senal_obs > 0:
                    mfe_actual_pct = (float(obs["max_precio"]) - precio_senal_obs) / precio_senal_obs * 100.0
                    alcanzados = obs.setdefault("objetivos", {})
                    for objetivo in PRUEBA7_OBJETIVOS_PCT:
                        clave = f"{objetivo:.2f}"
                        if clave not in alcanzados and mfe_actual_pct >= objetivo:
                            alcanzados[clave] = ahora_ts - obs["inicio_ts"]

                transcurridos = ahora_ts - obs["inicio_ts"]
                if transcurridos >= VENTANA_PRUEBA6_MINUTOS * 60:
                    precio_senal = float(obs["precio_senal"])
                    max_precio = float(obs["max_precio"])
                    mfe_pct = ((max_precio - precio_senal) / precio_senal * 100.0) if precio_senal > 0 else None
                    obs_final = dict(obs)
                    objetivos = dict(obs.get("objetivos", {}))
                    obs_final.update({
                        "fin_hora": ahora.strftime("%H:%M:%S ET"),
                        "mfe_pct": mfe_pct,
                        "duracion_min": transcurridos / 60.0,
                        "objetivos": objetivos,
                        "alcanza_025": "0.25" in objetivos,
                        "alcanza_050": "0.50" in objetivos,
                        "alcanza_100": "1.00" in objetivos,
                        "tiempo_025_min": (objetivos.get("0.25") / 60.0) if "0.25" in objetivos else None,
                        "tiempo_050_min": (objetivos.get("0.50") / 60.0) if "0.50" in objetivos else None,
                        "tiempo_100_min": (objetivos.get("1.00") / 60.0) if "1.00" in objetivos else None,
                    })
                    self.prueba6_completadas.insert(0, obs_final)
                    self.prueba6_completadas = self.prueba6_completadas[:100]
                    del self.prueba6_activos[ticker]

            # Abrir una observación nueva solo para una señal detectada que
            # todavía no esté siendo observada.
            for ticker, c in candidatos_validos.items():
                if ticker in self.prueba6_activos:
                    continue
                precio_senal = c.get("tecnico_precio_actual")
                if precio_senal is None:
                    precio_senal = c.get("precio")
                if precio_senal is None or float(precio_senal) <= 0:
                    continue
                bb_upper = c.get("bb_upper")
                bb_dist = c.get("bb_dist_pct")
                self.prueba6_activos[ticker] = {
                    "ticker": ticker,
                    "inicio_ts": ahora_ts,
                    "inicio_hora": ahora.strftime("%H:%M:%S ET"),
                    "precio_senal": float(precio_senal),
                    "bb_upper_senal": float(bb_upper) if bb_upper is not None else None,
                    "bb_dist_inicial": float(bb_dist) if bb_dist is not None else None,
                    "max_precio": float(c.get("precio") if c.get("precio") is not None else precio_senal),
                    "objetivos": {},
                }
        except Exception as ex:
            print(f"⚠️ Error en PRUEBA 6: {ex}")

    # ---------- ciclo principal ----------
    def _ciclo(self, tf=None, snapshots_pre=None):
        inicio = time.monotonic(); self._metrica_sumar("ciclos")
        _tf_metric = str(tf or self.tf_principal or self.timeframe or "1m").lower()
        with self._metricas_lock: self.metricas["ciclos_por_tf"][_tf_metric] = self.metricas["ciclos_por_tf"].get(_tf_metric, 0) + 1
        tf = str(tf or self.tf_principal or self.timeframe or "1m").lower()
        es_principal = (tf == str(self.tf_principal).lower())
        cache_tf = self.cache_tecnico_por_tf.setdefault(tf, {})
        # Filtros de ESTA temporalidad (precio, gap, volumen, float...). Se toman una vez
        # al inicio para que todo el ciclo use un conjunto coherente.
        filtros_tf = self._filtros_para(tf)
        # Inicializar siempre la duración para que las métricas no comparen
        # None contra float en ciclos donde el universo ya está cargado.
        self.duracion_ciclo = time.monotonic() - inicio
        if es_principal:
            self.ultimo_error = None

        if not self.universo or time.time() - self.universo_ts > 6 * 3600:
            self._cargar_universo()
        if not self.universo:
            self.ultima_actualizacion = datetime.now(ET)
            self.duracion_ciclo = time.monotonic() - inicio
            self._metrica_sumar("duracion_ciclo_total", self.duracion_ciclo)
            with self._metricas_lock:
                self.metricas["duracion_ciclo_min"] = self.duracion_ciclo if self.metricas["duracion_ciclo_min"] is None else min(self.metricas["duracion_ciclo_min"], self.duracion_ciclo)
                self.metricas["duracion_ciclo_max"] = max(self.metricas["duracion_ciclo_max"], self.duracion_ciclo)
                self.metricas["ultimo_ciclo_ts"] = time.time(); self.metricas["ultimo_ciclo_duracion"] = self.duracion_ciclo
                self.metricas["simbolos_procesados"] += 0
                if not self.ultimo_error:
                    self.ultimo_error = "No se pudo cargar el universo de acciones desde Alpaca."
                self.resultados_por_tf[tf] = []
                if es_principal:
                    self.resultados = []
                return

        # El float masivo se actualiza en un hilo auxiliar: NUNCA bloquea el radar.
        # Mientras termina, los candidatos nuevos usan el endpoint individual como respaldo.
        try:
            meta_bulk = self.cache_fund.get("__bulk_meta__", {}) if isinstance(self.cache_fund, dict) else {}
            _ventana_chk = FMP_BULK_FLOAT_TTL if int(meta_bulk.get("encontrados", 0) or 0) > 0 else 7200
            bulk_stale = time.time() - float(meta_bulk.get("ts", 0)) >= _ventana_chk
        except Exception:
            bulk_stale = True
        if bulk_stale and not getattr(self, "_bulk_float_running", False):
            with self._bulk_float_lock:
                if not self._bulk_float_running:
                    self._bulk_float_running = True
                    threading.Thread(target=self._actualizar_float_bulk, daemon=True).start()

        snapshots = snapshots_pre if snapshots_pre else self._descargar_snapshots()
        self._ultimos_snapshots = snapshots
        if not snapshots:
            self.ultima_actualizacion = datetime.now(ET)
            self.duracion_ciclo = time.monotonic() - inicio
            if not self.ultimo_error:
                self.ultimo_error = "Alpaca no devolvió snapshots de mercado en este ciclo."
            self.resultados_por_tf[tf] = []
            if es_principal:
                self.resultados = []
            return

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

            # GAP REAL, adaptado a la sesión:
            # - PRE/AFTER: antes de existir una apertura regular de hoy, la
            #   referencia operable es el último precio negociado vs. cierre previo.
            # - MERCADO ABIERTO: una vez emitida la daily bar, usamos la apertura
            #   regular de hoy vs. cierre previo.
            # Alpaca documenta que las daily bars se emiten después de abrir el
            # mercado, por lo que usar daily_bar.open en PRE-MARKET puede dejar el
            # radar sin GAP aunque haya movimiento real.
            sesion_actual = str(self.sesion or "PRE-MARKET").upper()
            apertura_hoy = getattr(snap.daily_bar, "open", None)
            usar_precio_gap = sesion_actual in ("PRE-MARKET", "AFTER-MARKET")
            try:
                if usar_precio_gap:
                    gap_pct = ((float(precio) - float(cierre_prev)) / float(cierre_prev)) * 100
                elif apertura_hoy is not None and float(apertura_hoy) > 0:
                    gap_pct = ((float(apertura_hoy) - float(cierre_prev)) / float(cierre_prev)) * 100
                else:
                    gap_pct = None
            except Exception:
                gap_pct = None

            # Volumen: durante mercado abierto/after usamos la daily bar. En
            # PRE-MARKET conservamos la mejor lectura disponible del snapshot;
            # no inventamos volumen acumulado que Alpaca no entrega en la daily bar
            # antes de la apertura regular.
            volumen_dia = getattr(snap.daily_bar, "volume", 0) or 0
            if usar_precio_gap and not volumen_dia:
                minuto = getattr(snap, "minute_bar", None)
                volumen_dia = getattr(minuto, "volume", 0) or 0

            base.append({
                "ticker": ticker,
                "precio": precio,
                "cambio_pct": cambio,
                "gap_pct": gap_pct,
                "volumen_dia": volumen_dia,
                "actualizado": snap.latest_trade.timestamp,
            })

        # Primero aplicamos SOLO los filtros baratos y disponibles en Alpaca.
        # IMPORTANTE: NO pedimos FLOAT aquí. FMP solo entrega aproximadamente
        # un ticker por intervalo y pedirlo antes de EMA/MACD hacía que casi
        # todo el universo quedara descartado por float desconocido.
        radar_gap = []
        for c in base:
            # El precio del usuario se aplica ANTES de recortar a los 300 de mayor volumen.
            # Antes solo se aplicaba al final, asi que un rango estrecho dejaba pocos resultados
            # aunque hubiera muchas acciones validas fuera de esos 300.
            try:
                if not (float(filtros_tf.get("precio_min", BASE_PRECIO_MIN)) <= float(c["precio"]) <= float(filtros_tf.get("precio_max", BASE_PRECIO_MAX))):
                    continue
            except Exception:
                pass
            if _filtro_activo(filtros_tf, "gap_activo", _filtro_activo(filtros_tf, "f_gap_on", False)):
                gap = c.get("gap_pct")
                if gap is None or not (float(filtros_tf.get("gap_min", 3.0)) <= float(gap) <= float(filtros_tf.get("gap_max", 50.0))):
                    continue
            if _filtro_activo(filtros_tf, "volumen_activo", _filtro_activo(filtros_tf, "f_vol_on", False)):
                if c.get("volumen_dia", 0) < filtros_tf.get("volumen_min", 15_000):
                    continue
            c["volumen_relativo"] = c["cambio_pct"]
            radar_gap.append(c)

        radar_base_total = len(base)
        self.n_radar_base = radar_base_total
        self.n_radar_gap = len(radar_gap)
        radar_gap.sort(key=lambda c: c["volumen_dia"], reverse=True)
        radar_gap = radar_gap[:MAX_ENRIQUECER]

        # EMA/MACD se calculan ANTES del float. Así FMP se usa únicamente
        # sobre candidatos técnicos reales y no sobre cientos de tickers.
        tickers_enr = [c["ticker"] for c in radar_gap]
        self._asegurar_tecnico(tickers_enr, tf)

        for c in radar_gap:
            tech = cache_tf.get(c["ticker"], (0, tf, False, False, False, False, None, None, None, 0, None, None, None, None, None, None, None, None, None))
            _, tecnico_timeframe, cruz_arriba, cruz_abajo, macd_pos, macd_neg, precio_tec, ema_tec, macd_tec, barras_tec, precio_prev_tec, ema_prev_tec, precio_actual_tec, ema_actual_tec, bb_upper_tec, bb_dist_tec, rsi_tec, ema50_tec, ema200_tec = tech
            c["cruzando_ema20"] = cruz_arriba
            c["cruzando_ema20_abajo"] = cruz_abajo
            c["macd_positivo"] = macd_pos
            c["macd_negativo"] = macd_neg
            c["tecnico_precio"] = precio_tec
            c["tecnico_ema20"] = ema_tec
            c["tecnico_macd"] = macd_tec
            c["tecnico_timeframe"] = str(tecnico_timeframe).lower()
            c["tecnico_barras"] = barras_tec
            c["tecnico_precio_anterior"] = precio_prev_tec
            c["tecnico_ema20_anterior"] = ema_prev_tec
            c["tecnico_precio_actual"] = precio_actual_tec
            c["tecnico_ema20_actual"] = ema_actual_tec
            try:
                c["ema_dist_pct"] = abs(float(precio_actual_tec) - float(ema_actual_tec)) / float(ema_actual_tec) * 100.0 if precio_actual_tec is not None and ema_actual_tec not in (None, 0) else None
            except Exception:
                c["ema_dist_pct"] = None
            c["bb_upper"] = bb_upper_tec
            c["bb_dist_pct"] = bb_dist_tec
            c["rsi"] = rsi_tec
            c["ema50"] = ema50_tec
            c["ema200"] = ema200_tec
            c["ema20_estado"] = "Por encima" if precio_actual_tec is not None and ema_actual_tec is not None and precio_actual_tec > ema_actual_tec else ("Por debajo" if precio_actual_tec is not None and ema_actual_tec is not None and precio_actual_tec < ema_actual_tec else "Neutro")
            c["ema50_estado"] = "Por encima" if precio_actual_tec is not None and ema50_tec is not None and precio_actual_tec > ema50_tec else ("Por debajo" if precio_actual_tec is not None and ema50_tec is not None and precio_actual_tec < ema50_tec else "Neutro")
            c["ema200_estado"] = "Por encima" if precio_actual_tec is not None and ema200_tec is not None and precio_actual_tec > ema200_tec else ("Por debajo" if precio_actual_tec is not None and ema200_tec is not None and precio_actual_tec < ema200_tec else "Neutro")
            c["cruce_ema20_confirmado"] = bool(cruz_arriba and precio_prev_tec is not None and ema_prev_tec is not None)
            c.update(self._cache_ema_extra(tf).get(c["ticker"], {}))
            c["tiene_noticia"] = False

        # Después de EMA/MACD, pedimos FLOAT solo a candidatos técnicos.
        # Esto elimina el cuello de botella que estaba dejando el scanner en 0.
        candidatos_tecnicos = [c for c in radar_gap if cumple_condiciones_ema(c, filtros_tf) and cumple_macd(c, filtros_tf)]
        # Las noticias son decorativas: consultamos solo una muestra de candidatos
        # técnicos, nunca los cientos de símbolos del radar base.
        con_noticia = self._noticias_recientes([c["ticker"] for c in candidatos_tecnicos[:100]])
        for c in candidatos_tecnicos:
            c["tiene_noticia"] = c["ticker"] in con_noticia
        self._asegurar_fundamentales([c["ticker"] for c in candidatos_tecnicos])

        limite_float = float(filtros_tf.get("flotacion_max", 20_000_000))
        float_activa = _filtro_activo(filtros_tf, "flotacion_activa", _filtro_activo(filtros_tf, "f_float_on", False))
        enriquecidos = []
        float_sin_dato_count = 0
        float_excede_count = 0
        for c in candidatos_tecnicos:
            entrada = self.cache_fund.get(c["ticker"], {})
            float_shares = entrada.get("float")
            if float_shares is None:
                _circ = entrada.get("outstanding")
                if float_activa and _circ is not None and float(_circ) <= limite_float:
                    # La flotacion nunca supera las acciones en circulacion: si estas ya cumplen, la flotacion tambien.
                    c["float_shares"] = float(_circ)
                    c["float_status"] = "max_circulacion"
                    c["float_source"] = "Finnhub (acciones en circulación)"
                    enriquecidos.append(c)
                    continue
                float_sin_dato_count += 1
                if float_activa:
                    continue
                c["float_shares"] = None
                c["float_status"] = "sin_dato"
                c["float_source"] = ""
                enriquecidos.append(c)
                continue
            if float_activa and float(float_shares) > limite_float:
                float_excede_count += 1
                continue
            c["float_shares"] = float_shares
            c["float_status"] = entrada.get("float_status", "ok")
            c["float_source"] = entrada.get("float_source", "FMP")
            enriquecidos.append(c)

        self.n_tras_float = len(enriquecidos)

        # Diagnóstico del embudo: no cambia ningún filtro ni el resultado del scanner.
        ema_arriba_count = sum(1 for c in radar_gap if cumple_condiciones_ema(c, filtros_tf))
        macd_positivo_count = sum(1 for c in radar_gap if c.get("macd_positivo"))
        ema_y_macd_count = sum(
            1 for c in enriquecidos
            if cumple_condiciones_ema(c, filtros_tf) and cumple_macd(c, filtros_tf)
        )
        # El filtro de volumen ya se aplicó al construir 'enriquecidos', así
        # que ese conteo ES el resultado "tras volumen". El paso previo
        # (antes de aplicar volumen) queda guardado en self.n_tras_float.
        tras_vol_rel_count = len(radar_gap)
        tecnicos_validos = sum(1 for c in radar_gap if c.get("tecnico_barras", 0) >= 40)
        ema_calculable = sum(1 for c in radar_gap if c.get("tecnico_ema20") is not None)
        macd_calculable = sum(1 for c in radar_gap if c.get("tecnico_macd") is not None)
        tickers_enr_unicos = len({c.get("ticker") for c in enriquecidos})
        # Conteo bruto que cumple EMA20 + MACD antes del límite de presentación.
        # En PRUEBA 4 top_n=50, por lo que el resultado final podrá mostrar hasta 50.
        candidatos_ema_macd_brutos = sum(
            1 for c in enriquecidos
            if cumple_condiciones_ema(c, filtros_tf) and cumple_macd(c, filtros_tf)
        )
        _diag_tf = {
            "radar_base": radar_base_total,
            "enviados_tecnico": len(radar_gap),
            "lote_tecnico": 30,
            "max_enriquecer": MAX_ENRIQUECER,
            "con_40_barras": tecnicos_validos,
            "ema_calculable": ema_calculable,
            "macd_calculable": macd_calculable,
            "tras_float": getattr(self, "n_tras_float", len(enriquecidos)),
            "float_sin_dato": float_sin_dato_count,
            "float_excede": float_excede_count,
            "tras_gap_volumen": tras_vol_rel_count,
            "tras_vol_rel": tras_vol_rel_count,
            "ema_arriba": ema_arriba_count,
            "macd_positivo": macd_positivo_count,
            "ema_y_macd": ema_y_macd_count,
            "candidatos_ema_macd_brutos": candidatos_ema_macd_brutos,
            "tickers_unicos": tickers_enr_unicos,
            "duplicados": len(enriquecidos) - tickers_enr_unicos,
            "resultados": len(filtrar_resultados(enriquecidos, filtros_tf)),
            "gap_aplicado": True,
            "gap_min": filtros_tf.get("gap_min", BASE_GAP_MIN),
            "gap_max": filtros_tf.get("gap_max", BASE_GAP_MAX),
            "sesion": str(self.sesion),
            "gap_modo": "precio_vs_cierre",
            "timeframe": tf,
        }
        self.diag_por_tf[tf] = _diag_tf
        if es_principal:
            self.diagnostico_filtros = _diag_tf

        # Guardamos una fotografía del resultado REAL de este ciclo antes de publicar
        # la lista nueva. Esto evita perder candidatos cuando desaparecen en el siguiente ciclo.
        p_hist = dict(filtros_tf)
        p_hist.update({"cruce_ema": "Hacia arriba", "macd": "Positivo", "top_n": 10, "orden": "Actualizado"})
        resultados_finales_hist = filtrar_resultados(enriquecidos, p_hist)

        # PRUEBA 6: iniciar/actualizar observaciones posteriores a la señal.
        # Esto se ejecuta antes de publicar el resultado y no modifica ningún filtro.
        if es_principal:
            self._actualizar_prueba6(candidatos_tecnicos, snapshots)

        # PRUEBA 4B: conservar las dos listas del MISMO ciclo.
        # El "raw" representa EMA20+MACD antes del filtro de float,
        # mientras que "final" representa la señal que ya cumple todos los filtros.
        candidatos_raw_actual = list(candidatos_tecnicos)
        if es_principal:
            self.candidatos_ema_macd_actual = list(candidatos_raw_actual)
            self.finales_ema_macd_actual = list(resultados_finales_hist)
        raw_tickers = {c.get("ticker") for c in candidatos_raw_actual}
        final_tickers = {c.get("ticker") for c in resultados_finales_hist}
        eliminados_mismo_ciclo = sorted(raw_tickers - final_tickers)

        # PRUEBA 4C: comparar candidatos EMA20+MACD con el ciclo inmediatamente anterior.
        # Esto solo diagnostica entradas/salidas naturales entre ciclos; no cambia filtros.
        raw_anterior = set(self._raw_prev_por_tf.get(tf, set()))
        mantenidos_entre_ciclos = sorted(raw_tickers & raw_anterior)
        entraron_este_ciclo = sorted(raw_tickers - raw_anterior)
        salieron_este_ciclo = sorted(raw_anterior - raw_tickers)
        self._raw_prev_por_tf[tf] = set(raw_tickers)
        if es_principal:
            self._raw_tickers_ciclo_anterior = set(raw_tickers)

        _diag_tf.update({
            "raw_tickers": sorted(x for x in raw_tickers if x),
            "raw_tickers_anterior": sorted(x for x in raw_anterior if x),
            "mantenidos_entre_ciclos": mantenidos_entre_ciclos,
            "entraron_este_ciclo": entraron_este_ciclo,
            "salieron_este_ciclo": salieron_este_ciclo,
            "final_tickers_mismo_ciclo": sorted(x for x in final_tickers if x),
            "eliminados_post_ema_macd": eliminados_mismo_ciclo,
            "eliminados_post_ema_macd_count": len(eliminados_mismo_ciclo),
        })
        if es_principal:
            self._registrar_historial_ciclo(enriquecidos, resultados_finales_hist)

        # Publicar exactamente la lista final del mismo ciclo. Así la tabla,
        # Telegram y el diagnóstico parten del mismo conjunto de señales.
        # Guardar el pool antes de los filtros personales permite que cada usuario
        # vea su propia configuración sin reconfigurar el motor compartido.
        self.pool_por_tf[tf] = list(enriquecidos)
        self.resultados_por_tf[tf] = list(resultados_finales_hist)
        if es_principal:
            self.resultados = list(resultados_finales_hist); self._metrica_sumar("resultados_publicados", len(resultados_finales_hist))
            # Conectar los candidatos publicados al motor de velas en tiempo real.
            # Esto ocurre fuera del navegador y no depende del refresh de los usuarios.
            self._sincronizar_motor_velas(resultados_finales_hist)
        self.float_pendientes = sum(
            1 for c in enriquecidos
            if c.get("float_shares") is None and c.get("float_status") == "pending"
        )
        self.float_sin_dato = sum(
            1 for c in enriquecidos
            if c.get("float_shares") is None and c.get("float_status") == "no_data"
        )
        self.ultima_actualizacion = datetime.now(ET)
        self.duracion_ciclo = time.monotonic() - inicio
        if es_principal:
            self._registrar_eventos(enriquecidos)

        # Telegram, eventos y archivos solo se generan para la temporalidad principal.
        if not es_principal:
            return

        # Telegram eliminado: no se construyen ni envían mensajes.

        else:
            self.telegram_estado = "Sin resultados para Telegram en este ciclo"

    def _bucle(self):
        while not self._detener_hilo.is_set():
            inicio = time.monotonic()
            try:
                # El motor es independiente del refresco de pantalla y trabaja
                # en ciclos de 10 s mientras esté encendido. La sesión seleccionada
                # se conserva para el contexto técnico/mercado, pero no bloquea
                # el hilo completo; de lo contrario PRE-MARKET podía dejar el motor
                # en ESPERA durante el mercado abierto y aparentar que no escaneaba.
                self._esta_en_horario_automatico()
                if self.encendido:
                    _snaps = None
                    for _tf in self._timeframes_a_procesar():
                        try:
                            self._ciclo(_tf, _snaps)
                        except Exception as _e_tf:
                            self.ultimo_error = f"Ciclo {_tf}: {_e_tf}"
                            print(f"⚠️ Error en escaneo {_tf}: {_e_tf}")
                            # Diagnóstico temporal: conservar la lógica del scanner intacta
                            # y mostrar la línea exacta que origina la excepción.
                            traceback.print_exc()
                        _snaps = self._ultimos_snapshots or None
            except Exception as e:
                self.ultimo_error = f"Ciclo: {e}"
                print(f"⚠️ Error en escaneo: {e}")
            espera = max(1.0, INTERVALO_ESCANEO_SEGUNDOS - (time.monotonic() - inicio))
            # Event.wait permite interrumpir el descanso inmediatamente al reiniciar.
            # Espera interrumpible: se detiene al reiniciar o se despierta cuando
            # alguien pide una temporalidad nueva.
            _fin_espera = time.monotonic() + espera
            while time.monotonic() < _fin_espera and not self._detener_hilo.is_set() and not self._despertar.is_set():
                self._detener_hilo.wait(timeout=0.5)
            self._despertar.clear()


@st.cache_resource
def obtener_servicio(api_key, secret_key, tg_token, tg_chat, fmp_api_key):
    print("⚙️ Iniciando el motor del scanner (una sola vez para todos los usuarios)...")
    return ServicioScanner(api_key, secret_key, tg_token, tg_chat, fmp_api_key, cargar_config_motor_compartido())


servicio = obtener_servicio(
    st.secrets["ALPACA_API_KEY"],
    st.secrets["ALPACA_SECRET_KEY"],
    st.secrets.get("TELEGRAM_BOT_TOKEN", None),
    st.secrets.get("TELEGRAM_CHAT_ID", "-1004440734539"),
    st.secrets.get("FMP_API_KEY", None),
)

# TELEGRAM: bloqueo de emergencia a nivel de instancia.
# El motor vive en st.cache_resource y puede conservar una instancia/hilo creado
# con una versión anterior del código. Aunque el método actual ya no envía,
# blindamos la instancia cacheada para que NUNCA vuelva a hacer una llamada a Telegram.
def _telegram_desactivado_runtime(_self, _texto_tabla=""):
    _self.telegram_estado = "DESACTIVADO"
    _self.telegram_ultimo_error = None
    return None

try:
    servicio.tg_token = None
    servicio.tg_chat = None
    servicio._enviar_telegram = _telegram_desactivado_runtime.__get__(servicio, type(servicio))
except Exception as _e_tg_block:
    print(f"⚠️ No se pudo blindar Telegram en la instancia cacheada: {_e_tg_block}")

# CONTROL EXCLUSIVO DEL MOTOR CENTRAL: solo una orden proveniente de una sesión
# que ya fue identificada como ADMIN puede cambiar el motor compartido.
if ES_ADMIN:
    try:
        _admin_motor_evento = st.session_state.pop("_admin_motor_evento", None)
        if _admin_motor_evento in ("True", "False"):
            servicio.encendido = (_admin_motor_evento == "True")
            guardar_estado_motor_en_disco(servicio.encendido)
    except Exception:
        pass
# El motor se guarda en cache y conserva el codigo VIEJO aunque subas un app.py nuevo.
# Aqui se le reasignan los metodos de la version actual para que los cambios apliquen sin reiniciar.
# Fase 1: si Streamlit conserva una instancia antigua en st.cache_resource,
# inicializamos las métricas sin reconstruir ni alterar el motor existente.
try:
    if not hasattr(servicio, "_metricas_lock"):
        servicio._metricas_lock = threading.Lock()
    if not hasattr(servicio, "metricas"):
        servicio.metricas = {
            "inicio": time.time(), "ciclos": 0, "ciclos_por_tf": {},
            "duracion_ciclo_total": 0.0, "duracion_ciclo_min": None, "duracion_ciclo_max": 0.0,
            "universo_cargas": 0, "simbolos_universo": 0, "snapshots": 0, "bars": 0, "calendario": 0,
            "fmp_bulk": 0, "fmp_individual": 0, "fmp_429": 0, "fmp_errores": 0,
            "fmp_cache_hits": 0, "fmp_cache_misses": 0, "errores_alpaca": 0, "alpaca_429": 0,
            "tiempo_snapshots": 0.0, "tiempo_bars": 0.0, "tiempo_fmp": 0.0,
            "resultados_publicados": 0, "simbolos_procesados": 0,
            "ultimo_ciclo_ts": None, "ultimo_ciclo_duracion": None,
        }
    servicio.data._scanner_metricas_owner = servicio
except Exception as _metricas_init_error:
    print(f"⚠️ No se pudo inicializar la instrumentación Fase 1: {_metricas_init_error}")

try:
    import types as _types
    for _nombre, _fn in list(vars(ServicioScanner).items()):
        if isinstance(_fn, _types.FunctionType) and not (_nombre.startswith("__") and _nombre.endswith("__")):
            servicio.__dict__[_nombre] = _types.MethodType(_fn, servicio)
except Exception as _e_patch:
    print(f"⚠️ No se pudo actualizar el motor en caliente: {_e_patch}")
try:
    servicio.finnhub_api_key = st.secrets.get("FINNHUB_API_KEY", None)
except Exception:
    servicio.finnhub_api_key = None

# Ventana operativa global: se conserva la configurada por el administrador.
# Los usuarios normales tienen su propia ventana de visualización.
servicio.sesion = "TODO EL MERCADO"

# Vigilancia del hilo: solo reiniciar si el hilo realmente murió.
# No reiniciamos por "stale" durante un F5/rerun: una demora temporal de Alpaca/FMP
# no significa que el hilo esté muerto y reiniciar aquí puede crear reconexiones,
# duplicar trabajo y elevar el consumo de CPU de Streamlit Cloud.
try:
    _hilo_ok = bool(getattr(getattr(servicio, "_hilo", None), "is_alive", lambda: False)())
    if not _hilo_ok:
        print("⚠️ Watchdog: reiniciando hilo del scanner porque está detenido.")
        servicio.reiniciar_scanner()
except Exception as _watchdog_error:
    print(f"⚠️ Watchdog del scanner: {_watchdog_error}")

# ==========================================
# 🎨 ESTILO OSCURO
# ==========================================
st.markdown("""
<style>
    :root {
        --ts-bg: #bdbdbd;
        --ts-panel: #d4d4d4;
        --ts-panel-2: #e1e1e1;
        --ts-gold: #d4af37;
        --ts-gold-bright: #f2d675;
        --ts-gold-dark: #7d641c;
        --ts-text: #111111;
        --ts-muted: #555555;
        --ts-red: #d64545;
        --ts-green: #37c77a;
    }

    .stApp {
        background: #bdbdbd !important;
        color: var(--ts-text) !important;
    }
    [data-testid="stHeader"], [data-testid="stSidebar"] {
        background: #030303 !important;
    }
    .block-container {
        max-width: 100% !important;
        width: 100% !important;
        padding-left: .35rem !important;
        padding-right: .35rem !important;
        padding-top: .45rem;
        padding-bottom: 1.5rem;
    }

    /* Paneles institucionales */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        background: linear-gradient(180deg, #0c0c0c 0%, #070707 100%) !important;
        border: 1px solid rgba(212,175,55,.32) !important;
        border-radius: 7px !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.025), 0 8px 30px rgba(0,0,0,.28) !important;
    }
    .simple-card {
        background: linear-gradient(180deg,#0c0c0c,#060606);
        border:1px solid rgba(212,175,55,.32);
        border-radius:7px;
        padding:8px 10px;
        margin-bottom:5px;
    }
    .simple-title {
        color: var(--ts-gold-bright);
        font-size:14px;
        font-weight:800;
        margin-bottom:4px;
        letter-spacing:.25px;
    }
    .simple-status {
        display:inline-block;
        padding:3px 7px;
        border:1px solid rgba(212,175,55,.45);
        border-radius:18px;
        color:var(--ts-gold-bright);
        background:#11100a;
        font-size:11px;
        margin-right:6px;
    }
    .small-note {
        background:#0e0d09;
        border:1px solid rgba(212,175,55,.28);
        border-radius:7px;
        padding:8px 10px;
        color:#d7d0bd;
        font-size:11px;
    }

    /* Controles */
    label, [data-testid="stWidgetLabel"] p {
        color:#d7d0bd !important;
        font-weight:700 !important;
        text-transform:none;
        font-size:11px !important;
    }
    div[data-testid="stNumberInput"] input,
    div[data-testid="stTextInput"] input,
    div[data-baseweb="select"] > div,
    div[data-testid="stTimeInput"] input {
        background:#101318 !important;
        color:#f1f1f1 !important;
        border-color:#3a4048 !important;
        box-shadow:none !important;
    }
    /* Controles planos: sin halo ni marco blanco alrededor */
    div[data-testid="stNumberInput"],
    div[data-testid="stTextInput"],
    div[data-testid="stTimeInput"],
    div[data-baseweb="select"],
    div[data-testid="stToggle"],
    div[data-testid="stNumberInput"] > div,
    div[data-testid="stTextInput"] > div,
    div[data-testid="stTimeInput"] > div {
        background:transparent !important;
        box-shadow:none !important;
        border:none !important;
        outline:none !important;
    }
    div[data-baseweb="select"] * { color:#f1f1f1 !important; box-shadow:none !important; }

    /* CORRECCIÓN DEFINITIVA: eliminar el marco/fondo blanco que Streamlit/BaseWeb
       agrega alrededor de los campos compactos. El fondo oscuro queda en el
       elemento que realmente contiene el valor, no en la envoltura blanca. */
    div[data-testid="stNumberInput"] > div,
    div[data-testid="stNumberInput"] > div > div,
    div[data-testid="stTextInput"] > div,
    div[data-testid="stTextInput"] > div > div,
    div[data-testid="stTimeInput"] > div,
    div[data-testid="stTimeInput"] > div > div,
    div[data-baseweb="input"],
    div[data-baseweb="input"] > div,
    div[data-baseweb="select"],
    div[data-baseweb="select"] > div,
    div[data-baseweb="select"] > div > div,
    div[data-baseweb="select"] [role="combobox"] {
        background:transparent !important;
        background-color:transparent !important;
        border-color:transparent !important;
        box-shadow:none !important;
        outline:none !important;
    }
    div[data-testid="stNumberInput"] input,
    div[data-testid="stTextInput"] input,
    div[data-testid="stTimeInput"] input,
    div[data-baseweb="input"] input,
    div[data-baseweb="select"] [role="combobox"] {
        background:#101318 !important;
        background-color:#101318 !important;
        color:#f1f1f1 !important;
        border:1px solid #3a4048 !important;
        box-shadow:none !important;
        outline:none !important;
    }
    div[data-testid="stNumberInput"] button,
    div[data-testid="stTimeInput"] button {
        background:#101318 !important;
        color:#f1f1f1 !important;
        border-color:#3a4048 !important;
        box-shadow:none !important;
    }
    div[data-testid="stNumberInput"] svg,
    div[data-testid="stTimeInput"] svg,
    div[data-baseweb="select"] svg {
        fill:#d7d0bd !important;
        color:#d7d0bd !important;
    }

    /* Desplegables legibles en móvil y escritorio: menú oscuro + texto claro.
       BaseWeb/Streamlit puede renderizar el menú fuera del contenedor del select,
       por eso estas reglas también cubren el popover/listbox. */
    [data-baseweb="popover"],
    [data-baseweb="menu"],
    [role="listbox"],
    ul[role="listbox"] {
        background:#101318 !important;
        color:#f1f1f1 !important;
        border:1px solid #3a4048 !important;
        box-shadow:none !important;
    }
    [data-baseweb="popover"] *,
    [data-baseweb="menu"] *,
    [role="listbox"] *,
    ul[role="listbox"] * {
        color:#f1f1f1 !important;
        background-color:transparent !important;
        text-shadow:none !important;
    }
    [role="option"] {
        color:#f1f1f1 !important;
        background:#101318 !important;
        font-size:11px !important;
        line-height:1.2 !important;
    }
    [role="option"]:hover,
    [role="option"][aria-selected="true"] {
        color:#ffffff !important;
        background:#243142 !important;
    }
    /* El valor seleccionado también debe conservar contraste cuando el campo es compacto. */
    div[data-baseweb="select"] [data-baseweb="select-value"],
    div[data-baseweb="select"] input,
    div[data-baseweb="select"] span {
        color:#f1f1f1 !important;
    }
    .stButton button {
        border-radius:6px !important;
        font-weight:800 !important;
        border:1px solid rgba(212,175,55,.55) !important;
        background:#11100c !important;
        color:var(--ts-gold-bright) !important;
        box-shadow:none !important;
        outline:none !important;
    }
    .stButton button:hover {
        border-color:var(--ts-gold-bright) !important;
        box-shadow:none !important;
    }
    [data-testid="stMetricValue"] { color:var(--ts-gold-bright) !important; }

    /* Alertas */
    [data-testid="stAlert"] {
        background:#0c0b08 !important;
        border-color:rgba(212,175,55,.34) !important;
        color:#ddd6c5 !important;
    }

    /* Dataframe */
    [data-testid="stDataFrame"] {
        border:1px solid rgba(212,175,55,.28) !important;
        border-radius:7px !important;
        overflow:hidden !important;
    }

    [data-testid="stDataFrame"] {
        border:1px solid #303640 !important;
        border-radius:5px !important;
        overflow:hidden !important;
        background:#101318 !important;
    }
    [data-testid="stDataFrame"] iframe {
        background:#101318 !important;
    }

    /* Compacto tipo Finviz: poco espacio vertical y texto pequeño, pero legible. */
    div[data-testid="stVerticalBlockBorderWrapper"] > div {
        gap: .20rem !important;
    }
    div[data-testid="stHorizontalBlock"] {
        gap:.18rem !important;
        margin-bottom:1px !important;
    }
    div[data-testid="stNumberInput"],
    div[data-testid="stTextInput"],
    div[data-testid="stTimeInput"],
    div[data-baseweb="select"] {
        margin-bottom:0 !important;
    }

    .inline-field-label {
        color:#c9c9c9 !important;
        font-size:10px !important;
        font-weight:700 !important;
        line-height:1.05 !important;
        min-height:28px !important;
        display:flex !important;
        align-items:center !important;
        white-space:nowrap !important;
    }
    .inline-toggle-label {
        color:#c9c9c9 !important;
        font-size:9px !important;
        font-weight:700 !important;
        line-height:1 !important;
        margin-bottom:0 !important;
        white-space:nowrap !important;
    }
    .inline-field-label + div { margin:0 !important; }
    div[data-testid="stNumberInput"] input,
    div[data-testid="stTextInput"] input {
        width:50% !important;
        max-width:72px !important;
        min-width:42px !important;
        height:25px !important;
        min-height:25px !important;
        padding:2px 5px !important;
        font-size:10px !important;
        box-shadow:none !important;
        outline:none !important;
    }
    div[data-baseweb="select"] {
        width:50% !important;
        max-width:105px !important;
        min-width:60px !important;
    }
    div[data-baseweb="select"] {
        min-height:28px !important;
        height:28px !important;
    }
    div[data-baseweb="select"] > div {
        min-height:25px !important;
        height:25px !important;
        font-size:9px !important;
    }
    @media (max-width: 900px) {
    }
    @media (max-width: 640px) {
        .block-container {
            max-width:100% !important;
            padding-left:.18rem !important;
            padding-right:.18rem !important;
            padding-top:.18rem !important;
        }
        .simple-title { font-size:12px !important; }
        .small-note { font-size:9px !important; }
        div[data-testid="stHorizontalBlock"] {
            gap:.18rem !important;
            flex-wrap:nowrap !important;
            align-items:flex-end !important;
        }
        div[data-testid="stHorizontalBlock"] > div {
            min-width:0 !important;
        }
        div[data-testid="stNumberInput"],
        div[data-testid="stTextInput"],
        div[data-baseweb="select"],
        div[data-testid="stToggle"] {
            min-width:0 !important;
        }
        div[data-testid="stNumberInput"] input,
        div[data-testid="stTextInput"] input {
            width:50% !important;
            max-width:48px !important;
            min-width:30px !important;
            font-size:7px !important;
            min-height:21px !important;
            height:21px !important;
            padding-left:2px !important;
            padding-right:2px !important;
            box-shadow:none !important;
        }
        div[data-baseweb="select"] {
            width:50% !important;
            max-width:70px !important;
            min-width:42px !important;
        }
        div[data-baseweb="select"] > div {
            font-size:7px !important;
            min-height:21px !important;
            height:21px !important;
            padding-left:2px !important;
            padding-right:2px !important;
        }
        [role="option"],
        [data-baseweb="menu"] * {
            font-size:9px !important;
            line-height:1.15 !important;
        }
        label, [data-testid="stWidgetLabel"] p {
            font-size:6.5px !important;
            line-height:1 !important;
        }
        .stButton button {
            font-size:7px !important;
            min-height:22px !important;
            height:24px !important;
            padding:1px 4px !important;
            white-space:nowrap !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"] {
            padding:3px !important;
        }
        [data-testid="stAlert"] {
            padding:3px 6px !important;
            margin:2px 0 !important;
            font-size:8px !important;
        }
        .simple-title { margin-bottom:2px !important; }
        div[data-testid="stHorizontalBlock"] { margin-bottom:1px !important; }
        [data-testid="stDataFrame"] {
            border-radius:4px !important;
        }
        [data-testid="stDataFrame"] {
            border-radius:4px !important;
            font-size:8px !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"] {
            padding:2px !important;
        }
        .simple-title {
            font-size:10px !important;
            line-height:1 !important;
        }
        .small-note {
            padding:3px 5px !important;
            line-height:1.05 !important;
        }
    }
</style>
""", unsafe_allow_html=True)
# ==============================================================================
# 🖥️ CARÁTULA FINVIZ — PRESENTACIÓN FINAL DEL SCANNER REAL
#    Esta sección solo presenta/filtra los datos del motor existente.
#    No reemplaza ni modifica el motor, sus hilos, cache, Alpaca, FMP ni pruebas.
# ==============================================================================

_JS_COLUMNAS = r'''
var TS_LAYOUT_DEFAULT_COLORS={L1:'#e53935',L2:'#1e88e5',L3:'#43a047',L4:'#f9a825',L5:'#8e44ad',L6:'#fb8c00',L7:'#f5f5f5',L8:'#212121',L9:'#00acc1',L10:'#ec407a'};
function _layoutKey(){try{return String(TS_USER_KEY).replace('tradeScannerLastState','tradeScannerLayouts')}catch(e){return 'tradeScannerLayouts'}}
function _layoutColorsLoad(){
  var raw='';try{raw=window.top.localStorage.getItem(_layoutKey())||''}catch(e){}
  if(!raw){try{raw=localStorage.getItem(_layoutKey())||''}catch(e){}}
  var o={};try{o=JSON.parse(raw||'{}')||{}}catch(e){o={}}
  Object.keys(TS_LAYOUT_DEFAULT_COLORS).forEach(function(id){if(!o[id])o[id]=TS_LAYOUT_DEFAULT_COLORS[id]});
  return o;
}
function _layoutColorsSave(o){var txt=JSON.stringify(o);try{window.top.localStorage.setItem(_layoutKey(),txt)}catch(e1){}try{localStorage.setItem(_layoutKey(),txt)}catch(e2){}}
function _layoutColor(layout){var o=_layoutColorsLoad();return o[String(layout)]||TS_LAYOUT_DEFAULT_COLORS[String(layout)]||'#777777'}
function guardarColorLayout(layout,color){
  var id=String(layout||'');if(!/^L([1-9]|10)$/.test(id))return;
  var col=String(color||'').trim();if(!/^#[0-9a-fA-F]{6}$/.test(col))return;
  var o=_layoutColorsLoad();o[id]=col;_layoutColorsSave(o);
  document.querySelectorAll('[data-layout-color="'+id+'"]').forEach(function(el){el.style.accentColor=col;el.value=col});
  document.querySelectorAll('[data-layout-chain="'+id+'"]').forEach(function(el){el.style.color=col;el.style.borderColor=col});
}
function aplicarColoresLayouts(){
  var o=_layoutColorsLoad();
  document.querySelectorAll('[data-layout-color]').forEach(function(el){var id=el.getAttribute('data-layout-color');var col=o[id]||TS_LAYOUT_DEFAULT_COLORS[id];if(col){el.value=col;el.style.accentColor=col;}});
  document.querySelectorAll('[data-layout-chain]').forEach(function(el){var id=el.getAttribute('data-layout-chain');var col=o[id]||TS_LAYOUT_DEFAULT_COLORS[id];if(col){el.style.color=col;el.style.borderColor=col;}});
}
var COLS=[['layout','🔗 Layout'],['ticker','Ticker'],['sector','Sector'],['precio','Precio ($)'],['cambio','Cambio %'],['volumen','Volumen'],['gap','Gap %'],['flot','Flotación (M)'],['ema20','EMA20'],['ema50','EMA50'],['ema200','EMA200'],['macd','MACD']];
function _colKey(){try{return String(TS_USER_KEY).replace('tradeScannerLastState','tradeScannerCols')}catch(e){return 'tradeScannerCols'}}
function _colLoad(){
  var ids=COLS.map(function(c){return c[0]});var raw='';
  try{raw=window.top.localStorage.getItem(_colKey())||''}catch(e){}
  if(!raw){try{raw=localStorage.getItem(_colKey())||''}catch(e){}}
  var o={};try{o=JSON.parse(raw||'{}')||{}}catch(e){o={}}
  var order=[];(Array.isArray(o.order)?o.order:[]).forEach(function(id){if(ids.indexOf(id)>=0&&order.indexOf(id)<0)order.push(id)});
  ids.forEach(function(id){if(order.indexOf(id)<0)order.push(id)});
  var hidden=(Array.isArray(o.hidden)?o.hidden:[]).filter(function(id){return ids.indexOf(id)>=0});
  return {order:order,hidden:hidden};
}
function _colSave(s){
  var txt=JSON.stringify(s);
  try{window.top.localStorage.setItem(_colKey(),txt)}catch(e1){}
  try{window.parent.localStorage.setItem(_colKey(),txt)}catch(e2){}
  try{localStorage.setItem(_colKey(),txt)}catch(e3){}
}
function aplicarColumnas(){
  var s=_colLoad();var tbl=document.querySelector('#resultados-tabla table');if(!tbl)return;
  var filas=tbl.querySelectorAll('tr');
  for(var r=0;r<filas.length;r++){
    var tr=filas[r];var celdas={};var hay=false;
    for(var k=0;k<tr.children.length;k++){var c=tr.children[k];var id=c.getAttribute('data-col');if(id){celdas[id]=c;hay=true}}
    if(!hay)continue;
    for(var i=0;i<s.order.length;i++){var cid=s.order[i];var cel=celdas[cid];if(!cel)continue;cel.style.display=(s.hidden.indexOf(cid)>=0)?'none':'';tr.appendChild(cel);}
  }
}
function renderColumnas(){
  var box=document.getElementById('cols_list');if(!box)return;
  var s=_colLoad();var nombres={};COLS.forEach(function(c){nombres[c[0]]=c[1]});
  box.innerHTML=s.order.map(function(id,i){
    var vis=s.hidden.indexOf(id)<0;
    return '<div class="col-row"><label><input type="checkbox" data-col-vis="'+id+'" '+(vis?'checked':'')+'> '+nombres[id]+'</label><span><button type="button" data-col-act="up" data-col-id="'+id+'"'+(i===0?' disabled':'')+'>▲</button><button type="button" data-col-act="down" data-col-id="'+id+'"'+(i===s.order.length-1?' disabled':'')+'>▼</button></span></div>';
  }).join('');
}
document.addEventListener('click',function(ev){
  var t=ev.target;var b=(t&&t.closest)?t.closest('[data-col-act]'):null;if(!b)return;
  ev.preventDefault();
  var act=b.getAttribute('data-col-act');var id=b.getAttribute('data-col-id');var s=_colLoad();
  if(act==='reset'){s={order:COLS.map(function(c){return c[0]}),hidden:[]};}
  else{
    var i=s.order.indexOf(id);if(i<0)return;
    var j=(act==='up')?i-1:i+1;if(j<0||j>=s.order.length)return;
    var tmp=s.order[i];s.order[i]=s.order[j];s.order[j]=tmp;
  }
  _colSave(s);aplicarColumnas();renderColumnas();
});
document.addEventListener('change',function(ev){
  var t=ev.target;if(!t||!t.getAttribute)return;var id=t.getAttribute('data-col-vis');if(!id)return;
  var s=_colLoad();var k=s.hidden.indexOf(id);
  if(t.checked){if(k>=0)s.hidden.splice(k,1)}else{if(k<0)s.hidden.push(id)}
  _colSave(s);aplicarColumnas();
});
document.addEventListener('DOMContentLoaded',function(){
  renderColumnas();aplicarColumnas();aplicarColoresLayouts();
  // Los campos numéricos del panel gris no llevan onchange individual.
  // Guardarlos al cambiar evita que el siguiente rerun los reconstruya con
  // el valor anterior.
  try{
    var _cfgIds=['price_min','price_max','gap_min','gap_max','float_max','txt_vol','ema_dist_max','rsi_min','rsi_max','ema20_dist','ema50_dist','ema200_dist'];
    _cfgIds.forEach(function(_id){
      var _el=document.getElementById(_id);
      if(!_el)return;
      _el.addEventListener('change',function(){try{pushConfig()}catch(e){}});
      _el.addEventListener('keydown',function(ev){
        if(ev.key==='Enter'){try{ev.preventDefault();pushConfig()}catch(e){}}
      });
    });
  }catch(e){}
});
window.addEventListener('storage',function(e){if(e&&e.key===_colKey()){aplicarColumnas();renderColumnas();}});
function _ajustarMarco(){
  try{
    var fe=window.frameElement;if(!fe)return;
    var mc=document.querySelector('.main-container')||document.body;
    var alto=Math.ceil(mc.getBoundingClientRect().height)+18;
    if(alto>80){fe.style.height=alto+'px';fe.setAttribute('height',String(alto));}
  }catch(e){}
}
window.addEventListener('load',function(){_ajustarMarco();setTimeout(_ajustarMarco,250);setTimeout(_ajustarMarco,1000);});
document.addEventListener('click',function(){setTimeout(_ajustarMarco,60);});
window.addEventListener('load',function(){try{var mc=document.querySelector('.main-container');if(mc)new ResizeObserver(function(){_ajustarMarco();}).observe(mc);}catch(e){}});
'''


def _render_scanner():
    try:
        servicio._esta_en_horario_automatico()
    except Exception:
        pass

    # Valores de interfaz seguros. Se leen de query_params para que los cambios
    # realizados desde la carátula puedan sobrevivir al rerun de Streamlit.
    def _qtxt(nombre, defecto):
        try:
            valor = st.query_params.get(nombre, defecto)
            if isinstance(valor, list):
                valor = valor[0] if valor else defecto
            return str(valor)
        except Exception:
            return str(defecto)


    def _qfloat(nombre, defecto):
        try:
            return float(_qtxt(nombre, defecto))
        except Exception:
            return float(defecto)


    def _qint(nombre, defecto):
        try:
            return int(float(_qtxt(nombre, defecto)))
        except Exception:
            return int(defecto)


    precio_min_ui = _qfloat("f_price_min", 0.50)
    precio_max_ui = _qfloat("f_price_max", 20.00)
    gap_min_ui = _qfloat("f_gap_min", 3.00)
    gap_max_ui = _qfloat("f_gap_max", 50.00)
    float_max_ui = _qint("f_float_max", 20_000_000)
    volumen_min_ui = _qint("f_vol", 15_000)
    ema_ui = _qtxt("f_ema", "Hacia arriba")
    ema20_estado_ui = _qtxt("ema20_estado", "Neutro")
    ema50_estado_ui = _qtxt("ema50_estado", "Neutro")
    ema200_estado_ui = _qtxt("ema200_estado", "Neutro")
    ema_cond_ui = {}
    ema_dist_ui = {}
    for _n, _def in ((20, "Naciendo"), (50, "Ninguna"), (200, "Ninguna")):
        _cnd = _qtxt(f"ema{_n}_cond", _def)
        ema_cond_ui[_n] = _cnd if _cnd in OPCIONES_COND_EMA else _def
        ema_dist_ui[_n] = max(0.0, min(25.0, _qfloat(f"ema{_n}_dist", 0.5)))
    macd_ui = _qtxt("f_mac", "Positivo")
    orden_ui = _qtxt("f_order", "Actualizado")
    swing_activo_ui = _qtxt("swing_activo", "OFF") == "ON"
    swing_origen_ui = _qtxt("swing_origen", "Bollinger inferior + debajo de EMA20")
    swing_objetivo_ui = _qtxt("swing_objetivo", "EMA50 o EMA200")
    swing_ventana_ui = max(1, min(30, _qint("swing_ventana", 10)))
    swing_tol_ui = max(0.0, min(10.0, _qfloat("swing_tolerancia", 1.0)))
    swing_origen_tol_ui = max(0.0, min(10.0, _qfloat("swing_origen_tolerancia", 1.0)))
    swing_multitimeframe_ui = _qtxt("swing_multitimeframe", "OFF") == "ON"
    _swing_tfs_raw = _qtxt("swing_tfs", "1d,1w,1mo")
    swing_tfs_ui = [x for x in [v.strip().lower() for v in _swing_tfs_raw.split(",")] if x in ("1d","1w","1mo")]
    if not swing_tfs_ui: swing_tfs_ui = ["1d","1w","1mo"]
    sesion_ui = "TODO EL MERCADO"
    timeframe_ui = _qtxt("timeframe", "1m")
    ema_dist_max_ui = _qfloat("ema_dist_max", 0.0)
    rsi_min_ui = _qfloat("rsi_min", 0.0)
    rsi_max_ui = _qfloat("rsi_max", 100.0)
    if timeframe_ui not in ("1m", "3m", "5m", "10m", "13m", "15m", "30m", "1h", "1d", "1w", "1mo"):
        timeframe_ui = "1m"
    ema_dist_max_ui = max(0.0, min(25.0, ema_dist_max_ui))
    if not PUBLIC_PREVIEW:
        # Un solo dato de distancia: el del panel nativo de EMA20 (antes esto quedaba en 0).
        ema_dist_max_ui = ema_dist_ui[20]
    rsi_min_ui = max(0.0, min(100.0, rsi_min_ui))
    rsi_max_ui = max(rsi_min_ui, min(100.0, rsi_max_ui))
    # (El registro de la temporalidad en el motor se hace más abajo, justo después de
    # armar params_ui, para entregarle junto con ella los filtros de esta pantalla.)

    # Regla fija del scanner: la señal es siempre EMA20 hacia arriba.
    # El selector sigue visible, pero no puede cambiar la lógica dura del motor.
    ema_ui = "Hacia arriba"
    # Las pestañas EMA20/50/200 son filtros reales (estado + condición).
    _estados_ok = ("Por encima", "Por debajo", "Neutro")
    if ema20_estado_ui not in _estados_ok:
        ema20_estado_ui = "Neutro"
    if ema50_estado_ui not in _estados_ok:
        ema50_estado_ui = "Neutro"
    if ema200_estado_ui not in _estados_ok:
        ema200_estado_ui = "Neutro"
    # Regla fija del scanner: MACD positivo es obligatorio.
    # El selector queda normalizado para que la interfaz no contradiga al motor.
    if macd_ui not in ("Positivo", "Negativo", "No exigir"):
        macd_ui = "Positivo"
    if orden_ui not in ("Actualizado", "Cambio %", "Volumen"):
        orden_ui = "Actualizado"

    params_ui = {
        "precio_min": precio_min_ui,
        "precio_max": precio_max_ui,
        "gap_min": gap_min_ui,
        "gap_max": gap_max_ui,
        "flotacion_max": float_max_ui,
        "volumen_min": volumen_min_ui,
        "cruce_ema": ema_ui,
        "macd": macd_ui,
        "orden": orden_ui,
        "top_n": 10,
        "sesion": sesion_ui,
        "timeframe": timeframe_ui,
        "ema_dist_max": ema_dist_max_ui,
        "rsi_min": rsi_min_ui,
        "rsi_max": rsi_max_ui,
        "ema20_estado": ema20_estado_ui,
        "ema50_estado": ema50_estado_ui,
        "ema200_estado": ema200_estado_ui,
        "ema20_cond": ema_cond_ui[20],
        "ema50_cond": ema_cond_ui[50],
        "ema200_cond": ema_cond_ui[200],
        "ema20_dist": ema_dist_ui[20],
        "ema50_dist": ema_dist_ui[50],
        "ema200_dist": ema_dist_ui[200],
        "swing_activo": swing_activo_ui,
        "swing_origen": swing_origen_ui,
        "swing_objetivo": swing_objetivo_ui,
        "swing_ventana": swing_ventana_ui,
        "swing_tolerancia": swing_tol_ui,
        "swing_origen_tolerancia": swing_origen_tol_ui,
        "swing_multitimeframe": swing_multitimeframe_ui,
        "swing_tfs": ",".join(swing_tfs_ui),
        "gap_activo": _qtxt("f_gap_on", "OFF") == "ON",
        "flotacion_activa": _qtxt("f_float_on", "OFF") == "ON",
        "volumen_activo": _qtxt("f_vol_on", "OFF") == "ON",
        "ema20_activa": _qtxt("ema20_on", "OFF") == "ON",
    }

    # IMPORTANTE: el hilo compartido debe usar exactamente los filtros actuales de la UI.
    # Antes el motor podía conservar una configuración vieja de cargar_config(),
    # mientras la pantalla mostraba otra, dejando el scanner aparentemente vacío.
    # Solo el administrador modifica el motor global. Un usuario normal registra
    # las temporalidades que necesita sobre el pool amplio, sin escribir sus filtros
    # en el singleton compartido.
    try:
        if ES_ADMIN:
            servicio.filtros_dueno.update(params_ui)
            servicio.sesion = sesion_ui
            if swing_activo_ui and swing_multitimeframe_ui:
                _tf_swing=swing_tfs_ui[:MAX_TIMEFRAMES_ACTIVOS]
                for _i_tf,_tf_s in enumerate(_tf_swing):
                    servicio.configurar_modo_operacion("TODO EL MERCADO",_tf_s,ema_dist_max_ui,principal=(_i_tf==0),filtros=params_ui)
                if _tf_swing:
                    timeframe_ui=_tf_swing[0]; params_ui["timeframe"]=timeframe_ui
            else:
                servicio.configurar_modo_operacion("TODO EL MERCADO", timeframe_ui, ema_dist_max_ui, principal=True, filtros=params_ui)
        elif USUARIO_AUTENTICADO:
            _pool_cfg = cargar_config_motor_compartido()
            if swing_activo_ui and swing_multitimeframe_ui:
                for _tf_s in swing_tfs_ui[:MAX_TIMEFRAMES_ACTIVOS]:
                    servicio.registrar_timeframe(_tf_s, principal=False, filtros=_pool_cfg)
                if swing_tfs_ui:
                    timeframe_ui=swing_tfs_ui[0]; params_ui["timeframe"]=timeframe_ui
            else:
                servicio.registrar_timeframe(timeframe_ui, principal=False, filtros=_pool_cfg)
    except Exception:
        pass

    _guardar_ultima_configuracion_servidor()


    # Estado personal: ON/OFF y horario del usuario no modifican el singleton global.
    if ES_ADMIN:
        _usuario_motor_activo = True
        _usuario_hora_ini = int(getattr(servicio, "hora_inicio_auto_min", 4*60))
        _usuario_hora_fin = int(getattr(servicio, "hora_fin_auto_min", 20*60))
    elif USUARIO_AUTENTICADO:
        _usuario_motor_activo = _qtxt("c_active", "True") == "True"
        def _hora_min_ui(_v, _d):
            try:
                _hh, _mm = str(_v).strip().split(":")
                return max(0, min(1439, int(_hh)*60 + int(_mm)))
            except Exception:
                return _d
        _usuario_hora_ini = _hora_min_ui(_qtxt("c_start", "04:00"), 4*60)
        _usuario_hora_fin = _hora_min_ui(_qtxt("c_end", "20:00"), 20*60)
    else:
        _usuario_motor_activo = True
        _usuario_hora_ini, _usuario_hora_fin = 4*60, 20*60
    _ahora_et_ui = datetime.now(ET)
    _min_actual_ui = _ahora_et_ui.hour*60 + _ahora_et_ui.minute
    if _usuario_hora_ini == _usuario_hora_fin:
        _usuario_en_horario = False
    elif _usuario_hora_ini < _usuario_hora_fin:
        _usuario_en_horario = _usuario_hora_ini <= _min_actual_ui < _usuario_hora_fin
    else:
        _usuario_en_horario = _min_actual_ui >= _usuario_hora_ini or _min_actual_ui < _usuario_hora_fin
    if PUBLIC_PREVIEW or not _usuario_motor_activo or not _usuario_en_horario:
        filas_reales = []
    else:
        try:
            _pool_tf = getattr(servicio, "pool_por_tf", {}) or {}
            _res_tf = getattr(servicio, "resultados_por_tf", {}) or {}
            _source_tf = _res_tf if ES_ADMIN else _pool_tf
            if swing_activo_ui and swing_multitimeframe_ui:
                _agrupados={}
                for _tf_mix in swing_tfs_ui[:MAX_TIMEFRAMES_ACTIVOS]:
                    for _rr in filtrar_resultados(list(_source_tf.get(_tf_mix, [])), dict(params_ui,timeframe=_tf_mix)):
                        _key_mix=str(_rr.get("ticker",""))
                        if _key_mix not in _agrupados:
                            _agrupados[_key_mix]=dict(_rr); _agrupados[_key_mix]["_swing_timeframes"]=[]
                        if _tf_mix not in _agrupados[_key_mix]["_swing_timeframes"]:
                            _agrupados[_key_mix]["_swing_timeframes"].append(_tf_mix)
                filas_reales=list(_agrupados.values())
            else:
                _lista_tf=list(_source_tf.get(timeframe_ui, []))
                if not _lista_tf and ES_ADMIN:
                    _lista_tf=list(getattr(servicio, "resultados", []) or [])
                filas_reales=filtrar_resultados(_lista_tf, params_ui)
            filas_reales=sorted(list(filas_reales),key=lambda x:x.get("actualizado") or datetime.min.replace(tzinfo=ET),reverse=True)[:10]
        except Exception as _ex_ui:
            filas_reales=[]
            print(f"⚠️ Filtro de pantalla: {_ex_ui}")


    def _num(v, default=0.0):
        try:
            if v is None or v == "":
                return default
            return float(v)
        except Exception:
            return default


    def _entero(v, default=0):
        try:
            if v is None or v == "":
                return default
            return int(float(v))
        except Exception:
            return default


    def _safe_text(v, default=""):
        return html_escape(str(v if v is not None else default))


    def _money(v):
        return f"${_num(v):,.2f}"


    def _pct(v):
        return f"{_num(v):+.2f}%"


    def _big(v):
        n = _num(v)
        if n >= 1_000_000:
            return f"{n/1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n/1_000:.0f}K"
        return f"{n:.0f}"


    _show_ema20=(_qtxt("ema20_on","OFF")=="ON" or ema20_estado_ui!="Neutro" or swing_activo_ui)
    _show_ema50=(ema50_estado_ui!="Neutro" or (swing_activo_ui and swing_objetivo_ui in ("EMA50","EMA50 o EMA200")))
    _show_ema200=(ema200_estado_ui!="Neutro" or (swing_activo_ui and swing_objetivo_ui in ("EMA200","EMA50 o EMA200")))
    def _row_html(row, indice=None):
        ticker = _safe_text(row.get("ticker", ""))
        sector = _safe_text(row.get("sector", "N/A"))
        precio = _num(row.get("precio"))
        cambio = _num(row.get("cambio_pct"))
        volumen = _entero(row.get("volumen_dia"))
        flotacion = _num(row.get("float_shares")) / 1_000_000 if row.get("float_shares") else 0.0
        ema_ok = bool(row.get("cruzando_ema20"))
        ema_down = bool(row.get("cruzando_ema20_abajo"))
        mac_pos = bool(row.get("macd_positivo"))
        mac_neg = bool(row.get("macd_negativo"))
        noticia = bool(row.get("tiene_noticia"))
        fila = "fila-alza" if cambio > 0 else ("fila-baja" if cambio < 0 else "")
        ema20_val = row.get("tecnico_ema20_actual", row.get("tecnico_ema20"))
        ema50_val = row.get("ema50")
        ema200_val = row.get("ema200")
        def _ema_cell(valor, estado):
            try:
                txt = f"${float(valor):.4f}"
            except Exception:
                txt = "N/D"
            return f"{txt} · {estado}"
        ema_txt = _ema_cell(ema20_val, row.get("ema20_estado", "Por encima" if ema_ok else ("Por debajo" if ema_down else "Neutro")))
        ema50_txt = _ema_cell(ema50_val, row.get("ema50_estado", "Neutro"))
        ema200_txt = _ema_cell(ema200_val, row.get("ema200_estado", "Neutro"))
        mac_txt = "Positivo" if mac_pos else ("Negativo" if mac_neg else "Neutro")
        mac_cls = "macd-positivo" if mac_pos else ("macd-negativo" if mac_neg else "macd-neutro")
        news = f" <button type='button' class='news-btn' title='Ver noticias' onclick='verNoticias(&quot;{ticker}&quot;)'>🔥</button>" if noticia else ""
        _layout_id = f"L{max(1, min(10, int(indice or 1)))}"
        _swing_tfs = row.get("_swing_timeframes") or []
        if isinstance(_swing_tfs, str):
            _swing_tfs = [_swing_tfs]
        _tf_badge=_safe_text(" · ".join(_swing_tfs))
        _ticker_extra=f" <span class='tf-badge'>{_tf_badge}</span>" if _tf_badge else ""
        return (
            f"<tr class='{fila}'>"
            f"<td class='layout-col' data-col='layout'><div class='chain-cell'>"
            f"<button type='button' class='layout-chain' data-layout-chain='{_layout_id}' title='{_layout_id} · enviar {ticker}' onclick='enviarLayoutLocal(&quot;{ticker}&quot;,&quot;{_layout_id}&quot;,_layoutColor(&quot;{_layout_id}&quot;))'>🔗</button>"
            f"<button type='button' class='layout-color-picker' data-layout-color='{_layout_id}' title='Color de {_layout_id} · abrir paleta' onclick='abrirPaletaLayout(event,&quot;{_layout_id}&quot;)'>▾</button>"
            f"</div></td>"
            f"<td data-col='ticker'><button type='button' class='ticker-layout-link' title='Enviar {ticker} a {_layout_id}' onclick='enviarLayoutLocal(&quot;{ticker}&quot;,&quot;{_layout_id}&quot;,_layoutColor(&quot;{_layout_id}&quot;))'><b>{ticker}</b></button>{_ticker_extra}{news}</td>"
            f"<td data-col='sector'>{sector}</td>"
            f"<td class='num-col' data-col='precio'>{_money(precio)}</td>"
            f"<td class='num-col' data-col='cambio'>{_pct(cambio)}</td>"
            f"<td class='num-col' data-col='volumen'>{_big(volumen)}</td>"
            f"<td class='num-col' data-col='gap'>{_pct(row.get('gap_pct'))}</td>"
            f"<td class='num-col' data-col='flot'>{flotacion:.2f}M</td>"
            + (f"<td data-col='ema20'>{_safe_text(ema_txt)}</td>" if _show_ema20 else "") +
            (f"<td data-col='ema50'>{_safe_text(ema50_txt)}</td>" if _show_ema50 else "") +
            (f"<td data-col='ema200'>{_safe_text(ema200_txt)}</td>" if _show_ema200 else "") +
            f"<td class='{mac_cls}' data-col='macd'>{mac_txt}</td></tr>"
        )


    # La sección RESULTADOS / VISUALIZACIÓN mantiene siempre las 10 líneas
    # horizontales del diseño. Cuando hay señales reales se colocan en las primeras
    # líneas; las restantes quedan disponibles con su engranaje de Layout.
    filas_visualizacion = list(filas_reales[:10])
    while len(filas_visualizacion) < 10:
        filas_visualizacion.append(None)


    def _row_visualizacion(item, indice):
        if item is None:
            return (
                "<tr class='fila-vacia'>"
                f"<td class='layout-col' data-col='layout'><div class='chain-cell'>"
                f"<button type='button' class='layout-chain' data-layout-chain='L{indice}' title='L{indice} · línea {indice}'>🔗</button>"
                f"<button type='button' class='layout-color-picker' data-layout-color='L{indice}' title='Color de L{indice} · abrir paleta' onclick='abrirPaletaLayout(event,&quot;L{indice}&quot;)'>▾</button>"
                f"</div></td>"
                f"<td data-col='ticker'><b>—</b></td><td data-col='sector'>—</td><td class='num-col' data-col='precio'>—</td>"
                "<td class='num-col' data-col='cambio'>—</td><td class='num-col' data-col='volumen'>—</td><td class='num-col' data-col='gap'>—</td>"
                "<td class='num-col' data-col='flot'>—</td>"
                + ("<td data-col='ema20'>—</td>" if _show_ema20 else "")
                + ("<td data-col='ema50'>—</td>" if _show_ema50 else "")
                + ("<td data-col='ema200'>—</td>" if _show_ema200 else "")
                + "<td class='macd-neutro' data-col='macd'>—</td></tr>"
            )
        return _row_html(item, indice)


    rows_html = "".join(_row_visualizacion(r, i + 1) for i, r in enumerate(filas_visualizacion))

    hora_ini, hora_fin = _usuario_hora_ini, _usuario_hora_fin
    _hora_txt = f"{hora_ini//60:02d}:{hora_ini%60:02d} - {hora_fin//60:02d}:{hora_fin%60:02d} ET"
    _estado_txt = (
        ("ON" if _usuario_motor_activo and _usuario_en_horario else "OFF")
        if not ES_ADMIN
        else ("ON" if servicio.encendido and servicio.ultima_actualizacion is not None else ("OFF" if not servicio.encendido else "ESPERA"))
    )
    start_time = f"{hora_ini//60:02d}:{hora_ini%60:02d}"
    end_time = f"{hora_fin//60:02d}:{hora_fin%60:02d}"
    # ADMIN ve siempre el estado real del motor central guardado en el servicio.
    # Los usuarios normales siguen usando su MOTOR personal, aislado del motor central.
    active_val = (
        "True" if getattr(servicio, "encendido", True) else "False"
    ) if ES_ADMIN else _qtxt("c_active", "True")
    try:
        if ES_ADMIN:
            servicio.configurar_horario(_usuario_hora_ini, _usuario_hora_fin)
    except Exception:
        pass
    lang_val = _qtxt("c_lang", "ESP")
    wnd_val = _qtxt("c_wnd", "Incrustada")
    broker_val = _qtxt("c_broker", st.session_state.get("bk_nombre", "Interactive Brokers"))
    bridge_val = _qtxt("c_url", st.session_state.get("bk_puente", "http://localhost:8080/layout"))

    # Enlace REAL entre el resultado del scanner y el puente de layout.
    # El navegador solicita el envío y Python ejecuta el POST, de modo que
    # el estado de Charles Schwab se conoce en el servidor y no se expone
    # ningún token OAuth al HTML/JavaScript.
    _pending_ticker = str(st.query_params.get("layout_send_ticker", "")).strip()
    _pending_layout = str(st.query_params.get("layout_send_color", "")).strip()
    _layout_from_browser = str(st.query_params.get("layout_from_browser", "")).strip() == "1"
    if _pending_ticker and _pending_layout and not _layout_from_browser:
        try:
            if broker_val == "Charles Schwab" and not _schwab_access_token():
                _ok_layout, _msg_layout = False, "Charles Schwab no está conectado. Autoriza Schwab antes de enviar activos."
            else:
                _ok_layout, _msg_layout = _schwab_send_layout_bridge(_pending_ticker, _pending_layout, bridge_val)
            st.session_state["layout_send_status"] = ("🟢 " if _ok_layout else "🔴 ") + _msg_layout
        except Exception as _ex_layout:
            st.session_state["layout_send_status"] = "🔴 Error enviando layout: " + str(_ex_layout)
        try:
            del st.query_params["layout_send_ticker"]
            del st.query_params["layout_send_color"]
        except Exception:
            pass

    # 🔄 Refresco de la interfaz: visitante fijo en 3 minutos; usuario registrado
    # puede seleccionar desde 5 segundos y valores mayores.
    _refresh_raw = st.session_state.get("_ts_refresh_canonico") if not PUBLIC_PREVIEW else "180"
    if _refresh_raw in (None, ""):
        _refresh_raw = st.query_params.get("refresh_sec", "180")
    try:
        refresh_sec = max(5, int(float(_refresh_raw)))
    except Exception:
        refresh_sec = 180
    if PUBLIC_PREVIEW:
        refresh_sec = 180
    refresh_options = [5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 45, 60, 90, 120, 180, 300, 600, 900, 1800, 3600]
    if refresh_sec not in refresh_options:
        refresh_options.append(refresh_sec)
    refresh_options = sorted(set(refresh_options))
    refresh_label = (f"{refresh_sec} s" if refresh_sec < 60 else (f"{refresh_sec//60} min" if refresh_sec % 60 == 0 else f"{refresh_sec} s"))
    fecha_hora_actual = datetime.now(ET).strftime("%d/%m/%Y %H:%M:%S ET")
    _email_top = st.session_state.get("usuario_auth", {}).get("email", "") if USUARIO_AUTENTICADO else ""

    h = "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
    h += "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
    h += "<title>TradeScanner</title>"
    h += "<style>"
    h += "*{box-sizing:border-box;}"
    h += "html,body{margin:0;padding:0;width:100%;min-height:100%;overflow:hidden;}body{background:#15181d;font-family:Verdana,Arial,sans-serif;font-size:12px;color:#000;overflow-x:hidden;padding-top:4px;}"
    h += ".main-container{width:100%;max-width:none;margin:0 auto;padding:4px;}"
    h += ".topbar{background:#20242a;border:1px solid #777;padding:7px 10px;margin-bottom:5px;display:flex;flex-direction:column;align-items:stretch;gap:4px;min-height:54px;position:sticky;top:0;z-index:1000;overflow:visible;}"
    h += ".brand{font-size:22px;font-weight:900;letter-spacing:.3px;color:#f1f3f5;white-space:nowrap;line-height:1.05;text-align:center;padding-top:5px;}.brand small{font-size:10px;font-weight:normal;color:#8f98a3;}"
    h += ".top-actions{display:flex;align-items:center;gap:6px;flex-wrap:wrap;justify-content:flex-end}.auth-link{display:inline-flex;align-items:center;height:27px;padding:0 9px;border:1px solid #555;background:#222;color:#fff;text-decoration:none;font-size:10px;font-weight:900;white-space:nowrap}.auth-link:hover{background:#333}.refresh-box{display:flex;align-items:center;gap:4px;font-size:9px;font-weight:bold;white-space:nowrap}.refresh-box select{width:82px;min-width:82px;height:25px;font-size:9px}"
    h += ".status-line{display:flex;align-items:center;justify-content:space-between;gap:10px;width:100%;border-top:1px solid #3c424a;padding-top:4px;}.status{font-weight:bold;white-space:nowrap;}.status.on{color:#3ddc84}.status.off{color:#ff6b6b}.status.wait{color:#f0b429}.date-time{font-size:9px;font-weight:bold;color:#b8c0ca;white-space:nowrap;margin-left:auto;}"
    h += ".tabs{display:flex;gap:3px;overflow-x:auto;background:#20242a;border:1px solid #777;padding:3px;margin-bottom:5px;white-space:nowrap;}"
    h += ".tab{font-size:10px;font-weight:bold;padding:4px 9px;background:#2a2f37;color:#dfe3e8;border:1px solid #555;cursor:pointer;}.tab.active{background:#11151a;color:#fff;border-bottom:2px solid #d4af37;}"
    h += ".filtros-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(max(230px,calc(25% - 18px)),1fr));gap:5px;background:#1d2127;border:1px solid #888;padding:6px;margin-bottom:6px;}"
    h += ".filtro-item{min-width:0;display:flex;align-items:center;justify-content:space-between;gap:8px;background:#292e36;border:1px solid #aaa;padding:5px 7px;min-height:38px;}"
    h += ".filtro-item label{font-weight:bold;color:#d8dde3;font-size:10px;white-space:nowrap;}.filtro-item>span{color:#d0d7e0;font-size:10px;line-height:1.3;}"
    h += "input,select,button{font-family:Verdana,Arial,sans-serif;font-size:11px;height:27px;border:1px solid #555;background:#171b20;color:#e7eaee;border-radius:0;outline:none;}"
    h += "input{min-width:0;width:105px;padding:1px 4px;}select{min-width:105px;max-width:170px;padding:1px 3px;}button{cursor:pointer;background:#30353d;color:#fff;font-weight:bold;padding:2px 8px;}"
    h += ".range{display:flex;gap:2px;align-items:center;}.range span{font-size:8px;color:#8d96a0;}"
    h += ".logo{display:flex;align-items:center;justify-content:center;background:#252a31;border:1px dashed #666;font-weight:900;color:#f1f3f5;min-height:34px;font-size:14px;}"
    h += ".engine{font-weight:bold;}.subline{background:#252b33;border:1px solid #8b949e;padding:7px 9px;margin-bottom:6px;font-size:11px;font-weight:700;color:#f0f2f4 !important;display:flex;gap:16px;flex-wrap:wrap;line-height:1.35;}.subline span,.subline span *{color:#f0f2f4 !important;opacity:1 !important;text-shadow:none !important;}.subline b{color:#f0f2f4 !important;font-weight:900;opacity:1 !important;text-shadow:none !important;}"
    h += ".result-title{background:#2d333b;color:#f0f2f4 !important;border:1px solid #777;border-bottom:0;padding:5px 8px;font-size:11px;font-weight:900;letter-spacing:.2px;opacity:1 !important;text-shadow:none !important;}"
    h += ".table-wrapper{width:100%;overflow-x:auto;background:#171a1f;border:1px solid #777;}table{width:100%;min-width:930px;border-collapse:collapse;table-layout:auto;}"
    h += "th{background:#2d333b;color:#f0f2f4;font-weight:bold;padding:5px 6px;border:1px solid #888;font-size:10px;text-align:left;white-space:nowrap;}"
    h += "td{padding:4px 6px;border:1px solid #3b424b;font-size:11px;color:#dce1e6;white-space:nowrap;height:25px;}"
    h += ".fila-alza{background:#1e3325}.fila-baja{background:#3a2426}.fila-vacia{background:#1c2025;color:#7f8995}.num-col{text-align:right}.empty-row{text-align:center!important;padding:18px!important;color:#9aa3ad;font-style:italic;}"
    h += ".macd-positivo{background:#b7dca0;color:#155724;font-weight:bold;text-align:center}.macd-negativo{background:#f4b084;color:#721c24;font-weight:bold;text-align:center}.macd-neutro{background:#e2e3e5;text-align:center;}"
    h += ".layout-col{width:120px;text-align:center;background:#242930;}.engranaje-select{width:112px;font-size:9px;height:21px;}"
    h += ".footer-note{margin-top:4px;font-size:8px;color:#7f8995;display:flex;justify-content:space-between;gap:8px;}"
    h += ".tab-panel{display:none;background:#20252b;color:#dce1e6;border:1px solid #888;border-top:0;padding:7px;margin-bottom:6px;font-size:10px;}.tab-panel.active{display:block;}.panel-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:5px;}.panel-card{background:#292e36;border:1px solid #4a515b;padding:7px;min-height:44px;}.panel-card b{display:block;margin-bottom:3px;font-size:9px;color:#f1f3f5;}.panel-card span{font-size:10px;color:#b8c0ca;}.technical-control{display:flex;flex-direction:column;align-items:stretch;gap:5px}.technical-control select{width:100%;max-width:none;}"
    h += "@media(max-width:900px){.filtros-grid{grid-template-columns:repeat(2,minmax(0,1fr));}.brand{font-size:16px;}.status{font-size:10px;white-space:normal;text-align:right;}}"
    h += "@media(max-width:520px){.main-container{padding:3px 3px 8px;width:100%;}.topbar{position:sticky;top:0;min-height:86px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:5px;padding:10px 6px;margin:0 0 5px;overflow:visible;}.brand{font-size:20px;white-space:nowrap;line-height:1.05;width:100%;text-align:center;padding-top:7px;}.brand small{display:block;font-size:8px;margin-top:3px;}.status-line{gap:5px;align-items:center;}.status{font-size:9px;white-space:nowrap;text-align:left;width:auto;line-height:1.2;}.date-time{font-size:8px;white-space:nowrap;}"
    h += ".tabs{display:grid;grid-template-columns:repeat(6,1fr);gap:2px;overflow:visible;width:100%;}.tab{font-size:8px;padding:6px 2px;flex:1 1 auto;width:100%;}.filtros-grid{grid-template-columns:1fr;gap:4px;padding:5px;}.filtro-item{min-height:34px;padding:4px 6px;gap:6px;}.filtro-item label{font-size:9px;flex:0 0 auto;}.filtro-item input,.filtro-item select{font-size:10px;height:25px;max-width:none;width:auto;min-width:120px;}.filtro-item .range{flex:1;min-width:0;}.filtro-item .range input{width:100%;min-width:70px;}.logo{min-height:38px;font-size:15px;}.subline{font-size:9px;gap:8px;padding:6px;}.result-title{font-size:10px;padding:6px 7px;}.table-wrapper{overflow-x:auto;-webkit-overflow-scrolling:touch;}.table-wrapper table{min-width:930px;}.footer-note{font-size:8px;flex-direction:column;gap:2px}.engranaje-select{width:112px;height:24px;font-size:10px}.panel-grid{grid-template-columns:1fr;gap:4px}.technical-control select{min-width:0;width:100%;}.tab-panel{font-size:9px;padding:6px}}"
    h += ".technical-subtabs{display:flex;gap:4px;margin-top:6px}.technical-subtab{flex:1;height:28px;background:#20242a;color:#fff;border:1px solid #555;font-size:9px;font-weight:900}.technical-subtab.active{background:#3a4048}.technical-subpanel{display:none;margin-top:4px}.technical-subpanel.active{display:block}.saved-config{display:grid;grid-template-columns:1.2fr 1fr auto auto;gap:5px;align-items:center;border-top:1px solid #444;padding:5px 0;font-size:9px}.saved-config button{height:23px;font-size:8px;background:#252a31;color:#fff;border:1px solid #555}.saved-empty{color:#9aa2ad;font-size:9px}@media(max-width:640px){.technical-subtabs{display:grid;grid-template-columns:1fr 1fr}.saved-config{grid-template-columns:1fr 1fr}}"
    _css_ocultar = "".join(
        f"th[data-col='ema{_n}'],td[data-col='ema{_n}']{{display:none!important}}"
        for _n, _est in ((20, ema20_estado_ui), (50, ema50_estado_ui), (200, ema200_estado_ui))
        if _est == "Neutro"
    )
    if _css_ocultar:
        h += _css_ocultar
    h += "#news-panel{display:none;position:fixed;top:0;right:0;width:350px;height:100%;overflow-y:auto;background:#1d2127;border-left:2px solid #f59e0b;z-index:999;padding:8px;box-sizing:border-box;color:#e5e9ee}.news-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;color:#fbbf24;font-size:13px}.news-close{background:#333;color:#fff;border:1px solid #777;cursor:pointer}.news-item{background:#171a1f;border:1px solid #555;padding:7px;margin-bottom:7px}.news-title{font-weight:bold;color:#fff;font-size:12px;margin-bottom:3px}.news-meta{color:#9aa3ad;font-size:10px;margin-bottom:4px}.news-sum{color:#d0d7e0;font-size:11px;margin-bottom:5px}.news-item a{color:#60a5fa;font-size:11px}.news-btn{background:transparent;border:0;cursor:pointer;font-size:14px;padding:0 2px}.float-btn{background:#2d3748;color:#fff;border:1px solid #888;cursor:pointer;font-size:10px;padding:4px 8px;margin-left:auto}"
    h += ".refresh-bar{display:flex;align-items:center;gap:8px;background:#1d2127;border:1px solid #888;padding:5px 8px;margin-bottom:6px}.refresh-bar label{font-weight:bold;color:#d8dde3;font-size:10px;white-space:nowrap}.refresh-bar span{color:#b8c0ca;font-size:10px}"
    # ===== FINVIZ-STYLE DESKTOP SHELL =====
    # Solo presentación: no modifica filtros, motor, persistencia ni eventos.
    h += "<style>";
    h += "html,body{width:100%;height:100%;margin:0;padding:0;overflow:hidden;background:#1f232b;color:#dfe3e8;}";
    h += "body{font-size:11px;padding-top:0;font-family:Arial,Verdana,sans-serif;}";
    h += ".main-container{width:calc(100% - 68px);max-width:1284px;min-width:0;margin:0 auto;padding:0 0 10px;}";
    h += ".topbar{display:grid;grid-template-columns:minmax(0,1fr) auto;grid-template-rows:auto auto;align-items:center;gap:4px 10px;min-height:50px;padding:5px 8px;margin:0 0 4px;background:#252a32;border:1px solid #3f4650;box-shadow:none;position:relative;}";
    h += ".brand{grid-column:1;grid-row:1;font-size:21px;line-height:1;font-weight:900;text-align:left;padding:0;letter-spacing:.2px;}";
    h += ".brand small{font-size:8px;color:#8993a0;margin-left:5px;}";
    h += ".top-actions{grid-column:2;grid-row:1;display:flex;align-items:center;justify-content:flex-end;gap:4px;}";
    h += ".status-line{grid-column:1/-1;grid-row:2;height:19px;min-height:19px;border-top:1px solid #353b44;padding-top:3px;display:flex;align-items:center;gap:8px;font-size:9px;}";
    h += ".status{font-size:9px;}.date-time{font-size:9px;}";
    h += ".tabs{display:flex;align-items:stretch;gap:1px;height:30px;min-height:30px;overflow:hidden;background:#252a32;border:1px solid #414852;padding:2px;margin:0 0 4px;}";
    h += ".tab{height:24px;min-width:92px;padding:3px 10px;border:0;border-right:1px solid #414852;background:#2d333c;color:#cbd1d8;font-size:10px;font-weight:700;line-height:18px;}";
    h += ".tab:hover{background:#353c46;}.tab.active{background:#2476d8;color:#fff;border:1px solid #4a9cff;border-bottom:2px solid #68b0ff;}";
    h += ".tab-panel{border:1px solid #414852;border-top:0;background:#252a32;padding:5px;margin:0 0 4px;font-size:9px;}";
    h += ".panel-grid{gap:3px;}.panel-card{background:#2b3038;border:1px solid #414852;padding:5px;min-height:34px;}";
    h += ".filtros-grid{grid-template-columns:repeat(5,minmax(0,1fr));gap:2px;background:#252a32;border:1px solid #414852;padding:4px;margin:0 0 4px;}";
    h += ".filtro-item{min-width:0;min-height:31px;height:31px;padding:3px 5px;gap:4px;background:#2b3038;border:1px solid #3d444e;}";
    h += ".filtro-item label{font-size:9px;color:#bfc6cf;line-height:1;}.filtro-item>span{font-size:9px;line-height:1.1;}";
    h += "input,select,button{height:23px;font-size:10px;border-radius:2px;}input{width:82px;padding:1px 3px;}select{min-width:82px;max-width:135px;padding:1px 3px;}.range{gap:1px;}";
    h += ".range input{width:60px;}.logo{min-height:31px;height:31px;font-size:12px;background:#303640;border:1px solid #4a515c;}";
    h += ".refresh-bar{height:28px;min-height:28px;padding:3px 6px;margin:0 0 4px;background:#252a32;border:1px solid #414852;gap:6px;}.refresh-bar label,.refresh-bar span{font-size:9px;}";
    h += ".subline{min-height:28px;height:auto;padding:5px 7px;margin:0 0 4px;background:#252a32;border:1px solid #414852;font-size:9px;gap:12px;line-height:1.1;}";
    h += ".result-title{height:28px;line-height:17px;padding:5px 7px;margin:0;background:#303640;color:#eef2f6!important;border:1px solid #4a515c;border-bottom:0;font-size:10px;}";
    h += ".table-wrapper{width:100%;max-width:100%;overflow-x:auto;overflow-y:hidden;background:#20252c;border:1px solid #4a515c;}";
    h += "table{width:100%;min-width:0;table-layout:auto;border-collapse:collapse;}";
    h += "th{height:27px;padding:4px 5px;background:#303640;color:#e7ebef;border:1px solid #505864;font-size:9px;font-weight:700;white-space:nowrap;}";
    h += "td{height:25px;padding:3px 5px;border:1px solid #363d47;font-size:10px;line-height:18px;white-space:nowrap;}";
    h += "tbody tr:nth-child(even){background:#292e36;}tbody tr:nth-child(odd){background:#242a31;}";
    h += ".layout-col{width:86px;text-align:center}.chain-cell{display:flex;align-items:center;justify-content:center;gap:3px}.layout-chain{width:28px;height:22px;padding:0;border:1px solid #666;background:#20252b;color:#fff;border-radius:4px;font-size:15px;line-height:20px;cursor:pointer}.layout-chain:hover{filter:brightness(1.2)}.layout-color-picker{width:22px;height:22px;padding:1px;border:1px solid #555;background:#20252b;cursor:pointer}.ticker-layout-link{border:0;background:transparent;color:inherit;padding:0;font:inherit;cursor:pointer}.ticker-layout-link:hover{text-decoration:underline}.engranaje-select{width:70px;height:20px;font-size:8px;}";
    h += ".layout-color-picker{position:relative;display:inline-flex;align-items:center;justify-content:center;width:18px;height:22px;padding:0;border:0;background:transparent !important;color:#d51f1f;font-size:15px;font-weight:900;line-height:20px;cursor:pointer;border-radius:3px;}";
    h += ".layout-color-picker:hover{filter:brightness(1.25);transform:scale(1.08)}";
    h += ".layout-palette{position:fixed;display:none;z-index:100000;background:#20242a;border:1px solid #6b737d;border-radius:5px;padding:5px;box-shadow:0 8px 24px rgba(0,0,0,.55);width:178px;}";
    h += ".layout-palette.open{display:grid;grid-template-columns:1fr 1fr;gap:3px;}";
    h += ".layout-palette button{height:28px;display:flex;align-items:center;gap:6px;padding:2px 5px;background:#2b3038;border:1px solid #4a515b;color:#eef2f6;font-size:9px;font-weight:700;text-align:left;cursor:pointer;}";
    h += ".layout-palette button:hover{background:#3a414b;border-color:#8b949e;}";
    h += ".layout-palette .swatch{width:18px;height:18px;border-radius:3px;border:1px solid rgba(255,255,255,.5);flex:0 0 auto;}";
    h += ".layout-palette .num{width:13px;text-align:center;color:#d7dce2;font-size:9px;}";
    h += "@media(max-width:520px){.layout-palette{width:170px;}}";
    h += ".schwab-item{grid-column:1/-1!important;align-items:center!important;display:flex!important;flex-direction:row!important;flex-wrap:nowrap!important;width:100%!important;gap:8px!important;}.schwab-item label{flex:0 0 auto!important;white-space:nowrap!important;}.schwab-item .schwab-info{flex:1 1 auto!important;min-width:0!important;text-align:left!important;white-space:nowrap!important;overflow:hidden!important;text-overflow:ellipsis!important;margin-left:0!important;font-size:9px!important;line-height:1!important;}.schwab-item button{white-space:nowrap!important;margin-left:auto!important;flex:0 0 auto!important;min-width:190px!important;}@media(max-width:640px){.schwab-item{flex-wrap:nowrap!important;}.schwab-item .schwab-info{white-space:nowrap!important;}.schwab-item button{margin-left:auto!important;min-width:180px!important;}}";
    h += ".footer-note{margin-top:3px;font-size:8px;color:#7f8995;}";
    h += "@media(max-width:1100px){.main-container{width:calc(100% - 24px);}.filtros-grid{grid-template-columns:repeat(4,minmax(0,1fr));}.tab{min-width:82px;padding-left:7px;padding-right:7px;}}";
    h += "@media(max-width:900px){.main-container{width:100%;padding:0 3px 8px;}.filtros-grid{grid-template-columns:repeat(3,minmax(0,1fr));}.brand{font-size:18px;}}";
    h += "@media(max-width:640px){.main-container{width:100%;padding:0 3px 8px;}.topbar{display:flex;flex-direction:column;min-height:78px;padding:7px 6px;}.brand{width:100%;text-align:center;font-size:18px;}.top-actions{width:100%;justify-content:center;}.status-line{width:100%;}.tabs{height:auto;overflow-x:auto;}.tab{min-width:82px;}.filtros-grid{grid-template-columns:repeat(2,minmax(0,1fr));}.filtro-item{height:32px;min-height:32px;}.table-wrapper{overflow-x:auto;}.table-wrapper table{min-width:930px;}}";
    h += "</style>";
    h += "</style>"
    h += "<script>window.addEventListener('load',function(){try{var raw=window.top.localStorage.getItem(TS_USER_KEY)||localStorage.getItem(TS_USER_KEY)||'';var o=JSON.parse(raw||'{}');var _tabPersist=_leerPestana(TS_ACTIVE_TAB_KEY);var _subPersist=_leerPestana(TS_ACTIVE_SUBTAB_KEY);if(o&&o._scrollY!=null){setTimeout(function(){try{window.scrollTo(0,Number(o._scrollY)||0);window.parent.scrollTo(0,Number(o._scrollY)||0);}catch(e){}},180);}}catch(e){}});"
    h += "function setQ(k,v){var q=_qtop();q.set(k,v);_goto(q);}"
    h += "function cambiarTimeframeTecnico(v){var q=_qtop();q.set('timeframe',v);q.set('technical_timeframe',v);_goto(q);}"
    h += "var TS_AUTH=" + ("true" if USUARIO_AUTENTICADO else "false") + ";"
    h += "var TS_BASE_QUERY=" + json.dumps({str(k): str(v) for k, v in st.query_params.items()}, ensure_ascii=False) + ";"
    h += "var TS_AUTH_SESSION=" + json.dumps(str(st.query_params.get("auth_session", ""))) + ";"
    h += "var TS_COMP=" + ("true" if _TS_COMP_OK else "false") + ";"
    h += "var TS_USER_KEY='tradeScannerLastState';try{var _em=" + json.dumps(str(_email_top or '')) + ";if(_em)TS_USER_KEY+='_'+btoa(unescape(encodeURIComponent(_em))).replace(/[^a-zA-Z0-9]/g,'_').slice(0,80)}catch(e){}"
    h += "try{if(TS_AUTH){var __sid=_qtop().get('auth_session');if(__sid)window.top.localStorage.setItem('tradeScannerAuthSession',__sid)}}catch(e){}"
    h += "function _qtop(){try{if(TS_COMP)return new URLSearchParams(TS_BASE_QUERY||{});return new URLSearchParams(window.top.location.search||'')}catch(e){try{return new URLSearchParams(TS_BASE_QUERY||{})}catch(_e){return new URLSearchParams()}}}"
    h += "function abrirRobotLong(){try{var q=_qtop();q.set('robot','1');var sid=q.get('auth_session')||TS_AUTH_SESSION||_authSid();if(TS_AUTH&&sid)q.set('auth_session',sid);window.top.location.href='/?'+q.toString();}catch(e){try{window.top.location.href='/?robot=1'}catch(_e){}}}"
    h += "function _authSid(){try{var sid=TS_AUTH_SESSION||'';if(sid){try{window.localStorage.setItem('tradeScannerAuthSession',sid)}catch(e){}return sid}try{return window.localStorage.getItem('tradeScannerAuthSession')||''}catch(e){return ''}}catch(e){return ''}}"
    h += "var TS_PERSIST_KEYS=['f_price_min','f_price_max','f_gap_min','f_gap_max','f_float_max','f_vol','f_ema','f_mac','f_order','market_session','timeframe','technical_timeframe','ema_dist_max','rsi_min','rsi_max','ema20_estado','ema50_estado','ema200_estado','c_active','c_start','c_end','c_lang','c_wnd','c_broker','c_url','refresh_sec','f_gap_on','f_float_on','f_vol_on','ema20_on','ema20_cond','ema50_cond','ema200_cond','ema20_dist','ema50_dist','ema200_dist'];var TS_ACTIVE_TAB_KEY='tradeScannerActiveTab_'+TS_USER_KEY;var TS_ACTIVE_SUBTAB_KEY='tradeScannerActiveSubTab_'+TS_USER_KEY;function _guardarPestanas(tabId,subId){try{if(tabId){window.top.localStorage.setItem(TS_ACTIVE_TAB_KEY,String(tabId));try{sessionStorage.setItem(TS_ACTIVE_TAB_KEY,String(tabId))}catch(e){}}if(subId){window.top.localStorage.setItem(TS_ACTIVE_SUBTAB_KEY,String(subId));try{sessionStorage.setItem(TS_ACTIVE_SUBTAB_KEY,String(subId))}catch(e){}}}catch(e){}}function _leerPestana(clave){var v='';try{v=window.top.localStorage.getItem(clave)||''}catch(e1){}if(!v){try{v=sessionStorage.getItem(clave)||''}catch(e2){}}return v;}function _guardarUltimaConfiguracion(q){try{var o={};TS_PERSIST_KEYS.forEach(function(k){var v=q.get(k);if(v!==null&&v!=='')o[k]=String(v)});o._savedAt=Date.now();var tab=document.querySelector('.tab.active');var sub=document.querySelector('.technical-subtab.active');o._scrollY=window.parent.scrollY||window.scrollY||0;try{window.top.localStorage.setItem(TS_USER_KEY,JSON.stringify(o))}catch(e1){}try{window.parent.localStorage.setItem(TS_USER_KEY,JSON.stringify(o))}catch(e2){}try{localStorage.setItem(TS_USER_KEY,JSON.stringify(o))}catch(e3){}try{if(o.c_lang)window.top.localStorage.setItem('tradeScannerLanguage',String(o.c_lang))}catch(e4){}}catch(e){}}"
    h += "function _restaurarUltimaConfiguracion(){try{if(!TS_AUTH)return;var q=_qtop();var hayConfig=false;TS_PERSIST_KEYS.forEach(function(k){if(q.get(k)!==null&&String(q.get(k))!=='')hayConfig=true});if(hayConfig)return;var raw='';try{raw=window.top.localStorage.getItem(TS_USER_KEY)||''}catch(e1){}if(!raw){try{raw=window.parent.localStorage.getItem(TS_USER_KEY)||''}catch(e2){}}if(!raw){try{raw=localStorage.getItem(TS_USER_KEY)||''}catch(e3){}}var o={};try{o=JSON.parse(raw||'{}')||{}}catch(e4){o={}}var changed=false;TS_PERSIST_KEYS.forEach(function(k){if(o[k]!==undefined&&o[k]!==null&&String(o[k])!==''){q.set(k,String(o[k]));changed=true}});if(!o.c_lang){var lg='';try{lg=window.top.localStorage.getItem('tradeScannerLanguage')||''}catch(e5){}if(lg&&TS_LANGS[lg]&&q.get('c_lang')!==lg){q.set('c_lang',lg);changed=true}}if(changed){q.set('_u',String(Date.now()));_navegarMismaApp(q)}}catch(e){}}"
    h += "function _navegarMismaApp(q){try{q.delete('_ts');q.set('_u',String(Date.now()));try{if(TS_AUTH&&!q.get('auth_session')){var _sx=TS_AUTH_SESSION||_authSid();if(_sx)q.set('auth_session',_sx);}}catch(_es){}if(TS_COMP){try{window.parent.postMessage({tsNav:1,q:q.toString()},'*');return;}catch(_ce){try{window.top.postMessage({tsNav:1,q:q.toString()},'*');return;}catch(_cte){}}}var u='/?'+q.toString();try{window.top.location.replace(u);return;}catch(e1){}try{window.parent.location.replace(u);return;}catch(e2){}try{window.location.replace(u);return;}catch(e3){try{console.warn('TS: navegacion bloqueada',e3);}catch(_e){}}}catch(e){try{console.warn('TS: navegacion bloqueada',e);}catch(_e){}}}"
    h += "function _goto(q){var cur=_qtop();var sid=cur.get('auth_session')||TS_AUTH_SESSION||_authSid();if(TS_AUTH && sid)q.set('auth_session',sid);_guardarUltimaConfiguracion(q);q.set('_ts',String(Date.now()));_navegarMismaApp(q)}"
    h += "function cfgActual(){var q=_qtop();var o={};q.forEach(function(v,k){o[k]=v});return o;}"
    h += "function showTab(id,btn){try{document.querySelectorAll('.tab-panel').forEach(function(x){x.classList.remove('active')});document.querySelectorAll('.tab').forEach(function(x){x.classList.remove('active')});var p=document.getElementById(id);if(p)p.classList.add('active');if(btn)btn.classList.add('active');var q=_qtop();q.set('_active_tab',String(id));_guardarPestanas(id,_leerPestana(TS_ACTIVE_SUBTAB_KEY)||'');_guardarUltimaConfiguracion(q);_goto(q);}catch(e){try{console.warn('TS: no se pudo cambiar de pestaña',e)}catch(_e){}}}"
    h += "function aplicarTecnicas(){var q=_qtop();['ema20_estado','ema50_estado','ema200_estado','ema20_cond','ema50_cond','ema200_cond','ema20_dist','ema50_dist','ema200_dist','swing_activo','swing_origen','swing_objetivo','swing_ventana','swing_tolerancia','swing_origen_tolerancia','swing_multitimeframe','rsi_min','rsi_max'].forEach(function(k){var e=document.getElementById(k);if(e)q.set(k,e.value)});var _stfs=[];document.querySelectorAll('.swing-tf-check:checked').forEach(function(e){_stfs.push(e.value)});q.set('swing_tfs',_stfs.join(','));_guardarUltimaConfiguracion(q);_goto(q);}"
    h += "function _configStorageKey(){return 'tradeScannerConfigs_'+TS_USER_KEY;}function _leerConfiguracionesPersonal(){var a=[];var raw='';try{raw=window.top.localStorage.getItem(_configStorageKey())||''}catch(e1){}if(!raw){try{raw=window.parent.localStorage.getItem(_configStorageKey())||''}catch(e2){}}if(!raw){try{raw=localStorage.getItem(_configStorageKey())||''}catch(e3){}}if(!raw){try{raw=localStorage.getItem('tradeScannerConfigs')||''}catch(e4){}}try{a=JSON.parse(raw||'[]')}catch(e5){a=[]}return Array.isArray(a)?a:[];}function _guardarConfiguracionesPersonal(a){var txt=JSON.stringify(a.slice(0,50));try{window.top.localStorage.setItem(_configStorageKey(),txt)}catch(e1){}try{window.parent.localStorage.setItem(_configStorageKey(),txt)}catch(e2){}try{localStorage.setItem(_configStorageKey(),txt)}catch(e3){}try{localStorage.setItem('tradeScannerConfigs',txt)}catch(e4){}}function guardarConfiguracionPersonal(){var n=(document.getElementById('config_name').value||'').trim();if(!n){alert('Escribe un nombre.');return}var q=_qtop();['ema20_estado','ema50_estado','ema200_estado','ema20_cond','ema50_cond','ema200_cond','ema20_dist','ema50_dist','ema200_dist','swing_activo','swing_origen','swing_objetivo','swing_ventana','swing_tolerancia','swing_origen_tolerancia','swing_multitimeframe'].forEach(function(k){var e=document.getElementById(k);if(e)q.set(k,e.value)});var _stfs=[];document.querySelectorAll('.swing-tf-check:checked').forEach(function(e){_stfs.push(e.value)});q.set('swing_tfs',_stfs.join(','));var o={};q.forEach(function(v,k){o[k]=v});o.nombre=n;o._savedAt=Date.now();var a=_leerConfiguracionesPersonal();a=a.filter(function(x){return String((x&&x.nombre)||'').trim().toLowerCase()!==n.toLowerCase()});a.unshift(o);_guardarConfiguracionesPersonal(a);_guardarUltimaConfiguracion(q);document.getElementById('config_name').value='';renderConfiguraciones();_goto(q);}"
    h += "function showTechnicalSubTab(id,btn){document.querySelectorAll('.technical-subpanel').forEach(function(x){x.classList.remove('active')});document.querySelectorAll('.technical-subtab').forEach(function(x){x.classList.remove('active')});var p=document.getElementById(id);if(p)p.classList.add('active');if(btn)btn.classList.add('active');try{var q=_qtop();q.set('_active_subtab',String(id));var u='/?'+q.toString();try{window.top.location.href=u;}catch(e){try{window.parent.location.href=u;}catch(_e){window.location.href=u;}}}catch(e){}if(id==='load-config-panel')renderConfiguraciones();}"
    h += "function renderConfiguraciones(){var b=document.getElementById('saved_configs_list');if(!b)return;var t=(document.getElementById('config_search').value||'').toLowerCase();var a=_leerConfiguracionesPersonal();a=a.filter(function(x){return String((x&&x.nombre)||'').toLowerCase().indexOf(t)>=0});b.innerHTML=a.length?a.map(function(x){var n=String((x&&x.nombre)||'').replace(/[<>]/g,'');var key=encodeURIComponent(String((x&&x.nombre)||''));return '<div class=\"saved-config\"><b>'+n+'</b><span>'+String(x.timeframe||'1m')+' · EMA20 '+String(x.ema20_estado||'Neutro')+' · EMA50 '+String(x.ema50_estado||'Neutro')+' · EMA200 '+String(x.ema200_estado||'Neutro')+'</span><button type=\"button\" class=\"btn-cargar-config\" data-config-name=\"'+key+'\">CARGAR</button><button type=\"button\" class=\"btn-eliminar-config\" data-config-name=\"'+key+'\">ELIMINAR</button></div>'}).join(''):'<span class=\"saved-empty\">No hay configuraciones guardadas.</span>'; }var _tsScrollTimer=null;window.addEventListener('scroll',function(){if(!TS_AUTH)return;if(_tsScrollTimer)return;_tsScrollTimer=setTimeout(function(){_tsScrollTimer=null;try{_guardarUltimaConfiguracion(_qtop());}catch(e){}},250);},{passive:true});"
    h += """document.addEventListener(\'DOMContentLoaded\',function(){setTimeout(function(){try{_restaurarUltimaConfiguracion()}catch(e){};try{renderConfiguraciones();try{var lg=(document.getElementById(\'cfg_lang\')||{}).value||\'\';if(lg&&TS_LANGS[lg])aplicarIdioma(lg);}catch(e){};var ids=[\'price_min\',\'price_max\',\'gap_min\',\'gap_max\',\'float_max\',\'txt_vol\',\'sel_ema\',\'sel_mac\',\'sel_order\',\'cfg_active\',\'cfg_start\',\'cfg_end\',\'cfg_lang\',\'cfg_wnd\',\'timeframe\',\'technical_timeframe\',\'ema_dist_max\',\'rsi_min\',\'rsi_max\',\'ema20_estado\',\'ema50_estado\',\'ema200_estado\',\'ema20_cond\',\'ema50_cond\',\'ema200_cond\',\'ema20_dist\',\'ema50_dist\',\'ema200_dist\',\'f_gap_on\',\'f_float_on\',\'f_vol_on\',\'ema20_on\',\'refresh_sec_inside\',\'cfg_broker\',\'cfg_url\'];ids.forEach(function(id){var el=document.getElementById(id);if(!el)return;el.addEventListener(\'change\',function(){try{if(id===\'refresh_sec_inside\')cambiarRefresh(el.value);else if(id===\'timeframe\'||id===\'technical_timeframe\'){var _tfv=String(el.value||\'1m\');var _qtf=_qtop();_qtf.set(\'timeframe\',_tfv);_qtf.set(\'technical_timeframe\',_tfv);_guardarUltimaConfiguracion(_qtf);_goto(_qtf);}else if([\'ema20_estado\',\'ema50_estado\',\'ema200_estado\',\'ema20_cond\',\'ema50_cond\',\'ema200_cond\',\'ema20_dist\',\'ema50_dist\',\'ema200_dist\'].indexOf(id)>=0){pushConfig();}else pushConfig();}catch(e){try{_guardarUltimaConfiguracion(_qtop());}catch(_e){}}});el.addEventListener(\'input\',function(){try{var q=_qtop();var map={price_min:\'f_price_min\',price_max:\'f_price_max\',gap_min:\'f_gap_min\',gap_max:\'f_gap_max\',float_max:\'f_float_max\',txt_vol:\'f_vol\',sel_ema:\'f_ema\',sel_mac:\'f_mac\',sel_order:\'f_order\',market_session:\'market_session\',timeframe:\'timeframe\',technical_timeframe:\'technical_timeframe\',ema_dist_max:\'ema_dist_max\',rsi_min:\'rsi_min\',rsi_max:\'rsi_max\',ema20_estado:\'ema20_estado\',ema50_estado:\'ema50_estado\',ema200_estado:\'ema200_estado\',ema20_cond:\'ema20_cond\',ema50_cond:\'ema50_cond\',ema200_cond:\'ema200_cond\',ema20_dist:\'ema20_dist\',ema50_dist:\'ema50_dist\',ema200_dist:\'ema200_dist\',cfg_active:\'c_active\',cfg_start:\'c_start\',cfg_end:\'c_end\',cfg_lang:\'c_lang\',cfg_wnd:\'c_wnd\',cfg_broker:\'c_broker\',cfg_url:\'c_url\',f_gap_on:\'f_gap_on\',f_float_on:\'f_float_on\',f_vol_on:\'f_vol_on\',ema20_on:\'ema20_on\',refresh_sec_inside:\'refresh_sec\'};var k=map[id];if(id==='bridge_url_conn'){q.set('c_url',el.value);_guardarUltimaConfiguracion(q)}else if(k){q.set(k,el.value);_guardarUltimaConfiguracion(q);}}catch(e){}});});},100)});"""
    h += "document.addEventListener('click',function(ev){var tab=ev.target.closest?ev.target.closest('.tab[data-tab-target]'):null;if(tab){ev.preventDefault();showTab(tab.getAttribute('data-tab-target'),tab);return;}var sub=ev.target.closest?ev.target.closest('.technical-subtab[data-subtab-target]'):null;if(sub){ev.preventDefault();showTechnicalSubTab(sub.getAttribute('data-subtab-target'),sub);return;}var save=ev.target.closest?ev.target.closest('.btn-guardar-config'):null;if(save){ev.preventDefault();guardarConfiguracionPersonal();return;}var btn=ev.target.closest?ev.target.closest('.btn-eliminar-config'):null;if(btn){ev.preventDefault();ev.stopPropagation();borrarConfiguracionPersonal(decodeURIComponent(btn.getAttribute('data-config-name')||''));return;}var cargar=ev.target.closest?ev.target.closest('.btn-cargar-config'):null;if(cargar){ev.preventDefault();ev.stopPropagation();cargarConfiguracionPersonal(decodeURIComponent(cargar.getAttribute('data-config-name')||''));return;}});"
    h += "var TS_LANGS={ESP:{'RADAR':'RADAR','TÉCNICOS':'TÉCNICOS','TECHNICAL':'TECHNICAL','CONFIGURACIÓN':'CONFIGURACIÓN','RESULTADOS':'RESULTADOS','COLUMNAS':'COLUMNAS','PRECIO ($)':'PRECIO ($)','GAP (%)':'GAP (%)','FLOTACIÓN ≤':'FLOTACIÓN ≤','VOLUMEN ≥':'VOLUMEN ≥','MACD':'MACD','ORDENAR':'ORDENAR','IDIOMA':'IDIOMA','VENTANA':'VENTANA','TEMPORALIDAD':'TEMPORALIDAD','MOTOR':'MOTOR','HORARIO (ET)':'HORARIO (ET)','HORARIO DEL SCANNER':'HORARIO DEL SCANNER','LAYOUT':'LAYOUT','BROKER':'BROKER','PUENTE DE LAYOUT':'PUENTE DE LAYOUT','🔌 CONEXIONES':'🔌 CONEXIONES','GUARDAR':'GUARDAR','ELIMINAR':'ELIMINAR','CARGAR':'CARGAR'},ENG:{'RADAR':'RADAR','TÉCNICOS':'TECHNICALS','TECHNICAL':'TECHNICAL','CONFIGURACIÓN':'SETTINGS','RESULTADOS':'RESULTS','COLUMNAS':'COLUMNS','PRECIO ($)':'PRICE ($)','GAP (%)':'GAP (%)','FLOTACIÓN ≤':'FLOAT ≤','VOLUMEN ≥':'VOLUME ≥','MACD':'MACD','ORDENAR':'SORT','IDIOMA':'LANGUAGE','VENTANA':'WINDOW','TEMPORALIDAD':'TIMEFRAME','MOTOR':'ENGINE','HORARIO (ET)':'SCHEDULE (ET)','HORARIO DEL SCANNER':'SCANNER SCHEDULE','LAYOUT':'LAYOUT','BROKER':'BROKER','PUENTE DE LAYOUT':'LAYOUT BRIDGE','GUARDAR':'SAVE','ELIMINAR':'DELETE','CARGAR':'LOAD'},POR:{'RADAR':'RADAR','TÉCNICOS':'TÉCNICOS','TECHNICAL':'TÉCNICO','CONFIGURACIÓN':'CONFIGURAÇÃO','RESULTADOS':'RESULTADOS','COLUMNAS':'COLUNAS','PRECIO ($)':'PREÇO ($)','GAP (%)':'GAP (%)','FLOTACIÓN ≤':'FLOAT ≤','VOLUMEN ≥':'VOLUME ≥','ORDENAR':'ORDENAR','IDIOMA':'IDIOMA','VENTANA':'JANELA','TEMPORALIDAD':'PERÍODO','MOTOR':'MOTOR','GUARDAR':'SALVAR','ELIMINAR':'EXCLUIR','CARGAR':'CARREGAR'},FRA:{'RADAR':'RADAR','TÉCNICOS':'TECHNIQUES','TECHNICAL':'TECHNIQUE','CONFIGURACIÓN':'CONFIGURATION','RESULTADOS':'RÉSULTATS','COLUMNAS':'COLONNES','PRECIO ($)':'PRIX ($)','GAP (%)':'GAP (%)','FLOTACIÓN ≤':'FLOTATION ≤','VOLUMEN ≥':'VOLUME ≥','ORDENAR':'TRIER','IDIOMA':'LANGUE','VENTANA':'FENÊTRE','TEMPORALIDAD':'UNITÉ DE TEMPS','MOTOR':'MOTEUR','GUARDAR':'ENREGISTRER','ELIMINAR':'SUPPRIMER','CARGAR':'CHARGER'},DEU:{'RADAR':'RADAR','TÉCNICOS':'TECHNIK','TECHNICAL':'TECHNIK','CONFIGURACIÓN':'EINSTELLUNGEN','RESULTADOS':'ERGEBNISSE','COLUMNAS':'SPALTEN','PRECIO ($)':'PREIS ($)','GAP (%)':'GAP (%)','FLOTACIÓN ≤':'FLOAT ≤','VOLUMEN ≥':'VOLUMEN','ORDENAR':'SORTIEREN','IDIOMA':'SPRACHE','VENTANA':'FENSTER','TEMPORALIDAD':'ZEITRAHMEN','MOTOR':'MOTOR','GUARDAR':'SPEICHERN','ELIMINAR':'LÖSCHEN','CARGAR':'LADEN'},ITA:{'RADAR':'RADAR','TÉCNICOS':'TECNICI','TECHNICAL':'TECNICO','CONFIGURACIÓN':'CONFIGURAZIONE','RESULTADOS':'RISULTATI','COLUMNAS':'COLONNE','PRECIO ($)':'PREZZO ($)','GAP (%)':'GAP (%)','FLOTACIÓN ≤':'FLOAT ≤','VOLUMEN ≥':'VOLUME','ORDENAR':'ORDINA','IDIOMA':'LINGUA','VENTANA':'FINESTRA','TEMPORALIDAD':'TIMEFRAME','MOTOR':'MOTORE','GUARDAR':'SALVA','ELIMINAR':'ELIMINA','CARGAR':'CARICA'},CHN:{'RADAR':'雷达','TÉCNICOS':'技术','TECHNICAL':'技术分析','CONFIGURACIÓN':'设置','RESULTADOS':'结果','COLUMNAS':'列','PRECIO ($)':'价格 ($)','GAP (%)':'跳空 (%)','FLOTACIÓN ≤':'流通股 ≤','VOLUMEN ≥':'成交量 ≥','ORDENAR':'排序','IDIOMA':'语言','VENTANA':'窗口','TEMPORALIDAD':'时间周期','MOTOR':'引擎','GUARDAR':'保存','ELIMINAR':'删除','CARGAR':'加载'},JPN:{'RADAR':'レーダー','TÉCNICOS':'テクニカル','TECHNICAL':'テクニカル分析','CONFIGURACIÓN':'設定','RESULTADOS':'結果','COLUMNAS':'列','PRECIO ($)':'価格 ($)','GAP (%)':'ギャップ (%)','FLOTACIÓN ≤':'浮動株 ≤','VOLUMEN ≥':'出来高 ≥','ORDENAR':'並べ替え','IDIOMA':'言語','VENTANA':'ウィンドウ','TEMPORALIDAD':'時間足','MOTOR':'エンジン','GUARDAR':'保存','ELIMINAR':'削除','CARGAR':'読み込み'}};"
    h += "function aplicarIdioma(lang){var d=TS_LANGS[lang]||TS_LANGS.ESP;document.querySelectorAll('label,.tab,.result-title,.panel-card b,th').forEach(function(el){var o=el.getAttribute('data-orig');var t=(el.textContent||'').trim();if(!o){if(TS_LANGS.ESP[t]!==undefined){o=t;el.setAttribute('data-orig',t)}else return}var tr=(lang&&lang!=='ESP'&&d[o])?d[o]:o;if(el.textContent!==tr)el.textContent=tr});document.documentElement.lang=(lang||'ESP').toLowerCase();try{localStorage.setItem('tradeScannerLanguage',lang)}catch(e){}}"
    h += "function _sq(q,k,id){var e=document.getElementById(id);if(e&&e.value!==undefined&&e.value!==null)q.set(k,e.value)}"
    h += "function pushConfig(){var q=_qtop();"
    h += "_sq(q,'f_price_min','price_min');_sq(q,'f_price_max','price_max');"
    h += "_sq(q,'f_gap_min','gap_min');_sq(q,'f_gap_max','gap_max');"
    h += "_sq(q,'f_float_max','float_max');_sq(q,'f_vol','txt_vol');"
    h += "_sq(q,'f_ema','sel_ema');_sq(q,'f_mac','sel_mac');"
    h += "_sq(q,'f_order','sel_order');_sq(q,'c_active','cfg_active');"
    h += "['f_gap_on','f_float_on','f_vol_on','ema20_on'].forEach(function(id){var e=document.getElementById(id);if(e)q.set(id,e.value)});"
    h += "if(!q.get('c_start'))q.set('c_start','04:00');if(!q.get('c_end'))q.set('c_end','20:00');"
    h += "_sq(q,'c_lang','cfg_lang');_sq(q,'c_wnd','cfg_wnd');q.set('market_session','TODO EL MERCADO');var _tfEl=document.getElementById('timeframe');var _ttfEl=document.getElementById('technical_timeframe');var _tfVal=(_tfEl&&_tfEl.value)?_tfEl.value:((_ttfEl&&_ttfEl.value)?_ttfEl.value:'1m');q.set('timeframe',_tfVal);q.set('technical_timeframe',_tfVal);_sq(q,'ema_dist_max','ema_dist_max');_sq(q,'rsi_min','rsi_min');_sq(q,'rsi_max','rsi_max');['ema20_estado','ema50_estado','ema200_estado','ema20_cond','ema50_cond','ema200_cond','ema20_dist','ema50_dist','ema200_dist','swing_activo','swing_origen','swing_objetivo','swing_ventana','swing_tolerancia','swing_origen_tolerancia','swing_multitimeframe'].forEach(function(k){var e=document.getElementById(k);if(e)q.set(k,e.value)});var _stfs=[];document.querySelectorAll('.swing-tf-check:checked').forEach(function(e){_stfs.push(e.value)});q.set('swing_tfs',_stfs.join(','));"
    h += "_sq(q,'c_broker','cfg_broker');_sq(q,'c_url','cfg_url');"
    h += "try{var _dbg={t:Date.now(),fields:{}};['price_min','price_max','gap_min','gap_max','float_max','txt_vol','sel_mac','sel_order','timeframe'].forEach(function(id){var e=document.getElementById(id);if(e)_dbg.fields[id]=String(e.value)});q.set('_ts_diag',JSON.stringify(_dbg));}catch(_de){}"
    h += "_guardarUltimaConfiguracion(q);q.set('_ts',Date.now());try{_navegarMismaApp(q)}catch(e){_navegarMismaApp(q);}}"
    h += "function conectarSchwab(){var q=_qtop();q.set('schwab_connect','1');_guardarUltimaConfiguracion(q);_navegarMismaApp(q);}"
    h += "function _bridgeUrlUi(){var e=document.getElementById('bridge_url_conn')||document.getElementById('cfg_url');var u=e&&e.value?String(e.value).trim():'';return u.replace(/\\/$/,'')}";
    h += "function _bridgeFetch(url,opts){opts=opts||{};try{opts.targetAddressSpace='loopback'}catch(e){}return fetch(url,opts)}";
    h += "function probarPuente(){var u=_bridgeUrlUi();var s=document.getElementById('bridge_status');if(!u){if(s)s.innerHTML='🔴 Escribe la dirección del puente';return;}if(s)s.innerHTML='🟡 Probando...';var health=u.replace(/\\/layout\\/?$/i,'/health');_bridgeFetch(health,{method:'GET',mode:'cors'}).then(function(r){if(!r.ok)throw new Error('HTTP '+r.status);return r.json()}).then(function(d){if(s)s.innerHTML='🟢 PUENTE CONECTADO · '+(d.service||'Listo');try{var q=_qtop();q.set('c_url',u);_guardarUltimaConfiguracion(q)}catch(e){}}).catch(function(e){if(s)s.innerHTML='🔴 No se pudo conectar al puente';});}";
    h += "function enviarLayoutLocal(t,layout,color){var u=_bridgeUrlUi();if(!u||!t)return;var s=document.getElementById('bridge_status');var col=color||_layoutColor(layout);_bridgeFetch(u,{method:'POST',mode:'cors',headers:{'Content-Type':'application/json'},body:JSON.stringify({broker:document.getElementById('cfg_broker')?document.getElementById('cfg_broker').value:'Charles Schwab',ticker:String(t),layout:String(layout),layout_color:String(layout),color:String(col),timestamp:Date.now()/1000})}).then(function(r){if(!r.ok)throw new Error('HTTP '+r.status);return r.json().catch(function(){return {ok:true}})}).then(function(){if(s)s.innerHTML='🟢 '+String(t)+' → '+String(layout)+' · '+String(col)}).catch(function(){if(s)s.innerHTML='🔴 Puente no disponible · verifica que esté abierto en tu PC';});}";
    h += '''var TS_LAYOUT_LINK_COLORS=[{n:1,name:'Red',c:'#d51f1f'},{n:2,name:'Yellow',c:'#f1cf16'},{n:3,name:'Blue',c:'#178fca'},{n:4,name:'Green',c:'#159447'},{n:5,name:'Purple',c:'#9a4de3'},{n:6,name:'Maroon',c:'#8b2f18'},{n:7,name:'Orange',c:'#d86b08'},{n:8,name:'Brown',c:'#8b5a2b'},{n:9,name:'Lilac',c:'#b56bd9'},{n:10,name:'Cyan',c:'#11a9b5'}];function _layoutColorsLoad(){var o={};try{o=JSON.parse(localStorage.getItem('tsLayoutColors_'+TS_USER_KEY)||'{}')||{}}catch(e){}return o;}function _layoutColorsSave(o){try{localStorage.setItem('tsLayoutColors_'+TS_USER_KEY,JSON.stringify(o))}catch(e){}try{window.top.localStorage.setItem('tsLayoutColors_'+TS_USER_KEY,JSON.stringify(o))}catch(e){}}function _layoutColor(layout){var o=_layoutColorsLoad();if(o[layout])return o[layout];var x=TS_LAYOUT_LINK_COLORS.find(function(v){return 'L'+v.n===layout});return x?x.c:'#d51f1f';}function guardarColorLayout(layout,color){var o=_layoutColorsLoad();o[String(layout)]=String(color);_layoutColorsSave(o);aplicarColoresLayouts();}function aplicarColoresLayouts(){document.querySelectorAll('[data-layout-color]').forEach(function(el){var l=el.getAttribute('data-layout-color');var c=_layoutColor(l);el.style.setProperty('color',c);el.style.setProperty('border-color',c);el.setAttribute('data-current-color',c);});}function cerrarPaletaLayout(){var p=document.getElementById('layout-palette');if(p){p.classList.remove('open');p.style.display='none';}}function seleccionarColorLayout(layout,color){guardarColorLayout(layout,color);cerrarPaletaLayout();}function abrirPaletaLayout(ev,layout){try{ev.preventDefault();ev.stopPropagation();}catch(e){}var old=document.getElementById('layout-palette');if(old)old.remove();var p=document.createElement('div');p.id='layout-palette';p.className='layout-palette open';TS_LAYOUT_LINK_COLORS.forEach(function(v){var b=document.createElement('button');b.type='button';b.title=v.n+' · '+v.name;b.innerHTML='<span class="num">'+v.n+'</span><span class="swatch" style="background:'+v.c+'"></span><span>'+v.name+'</span>';b.onclick=function(e){e.preventDefault();e.stopPropagation();seleccionarColorLayout(layout,v.c);};p.appendChild(b);});document.body.appendChild(p);var r=ev.currentTarget.getBoundingClientRect();var left=Math.min(Math.max(6,r.left),window.innerWidth-184);var top=r.bottom+4;if(top+184>window.innerHeight)top=Math.max(6,r.top-184);p.style.left=left+'px';p.style.top=top+'px';p.style.display='grid';}document.addEventListener('click',function(e){var p=document.getElementById('layout-palette');if(p&&!p.contains(e.target)&&!e.target.closest('.layout-color-picker'))cerrarPaletaLayout();});document.addEventListener('DOMContentLoaded',function(){setTimeout(aplicarColoresLayouts,80);});''';
    h += "function cambiarLayout(t,e){var v=e&&e.value;if(!v)return;enviarLayoutLocal(t,v,_layoutColor(v));try{var q=_qtop();q.set('c_url',_bridgeUrlUi());q.delete('layout_send_ticker');q.delete('layout_send_color');q.delete('layout_from_browser');_guardarUltimaConfiguracion(q)}catch(err){}}";
    h += "function showTab(id,btn){document.querySelectorAll('.tab-panel').forEach(function(p){p.classList.remove('active');});document.querySelectorAll('.tab').forEach(function(b){b.classList.remove('active');});var p=document.getElementById(id);if(p)p.classList.add('active');if(btn)btn.classList.add('active');try{var q=_qtop();q.set('_active_tab',String(id));var u='/?'+q.toString();try{window.top.location.href=u;}catch(e){try{window.parent.location.href=u;}catch(_e){window.location.href=u;}}}catch(e){}if(id==='panel-resultados'){var r=document.getElementById('resultados-tabla');if(r)r.scrollIntoView({behavior:'smooth',block:'start'});}}"
    h += "function abrirAutenticacion(){try{var q=new URLSearchParams();q.set('auth','1');_navegarMismaApp(q);}catch(e){try{window.top.location.href='/?auth=1';}catch(_e){window.location.href='/?auth=1';}}}"
    h += "function cambiarRefresh(v){var _n=Date.now();if(window._tsRf===String(v)&&_n-(window._tsRfT||0)<1500)return;window._tsRf=String(v);window._tsRfT=_n;var q=_qtop();q.set('refresh_sec',String(v));var sid=q.get('auth_session')||TS_AUTH_SESSION||_authSid();if(TS_AUTH && sid)q.set('auth_session',sid);_guardarUltimaConfiguracion(q);q.set('_u',String(Date.now()));q.set('_ts',String(Date.now()));_navegarMismaApp(q)}"
    h += ""
    h += _JS_COLUMNAS
    h += "function abrirVentanaFlotante(){try{var q=_qtop();q.set('embed','true');q.set('c_wnd','Incrustada');var base='';try{base=window.top.location.origin+window.top.location.pathname}catch(e1){}if(!base){try{base=window.parent.location.origin+window.parent.location.pathname}catch(e2){base='/'}}var w=window.open(base+'?'+q.toString(),'tsFloatWin','popup=yes,width='+Math.min(1400,screen.availWidth-80)+',height='+Math.min(950,screen.availHeight-80)+',left=40,top=40,resizable=yes,scrollbars=yes');if(!w){alert('El navegador bloqueo la ventana. Permite ventanas emergentes para este sitio y vuelve a intentar.')}else if(w.focus){w.focus()}}catch(e){alert('No se pudo abrir la ventana: '+e.message)}}"
    h += "function cerrarNoticias(){var p=document.getElementById('news-panel');if(p)p.style.display='none';document.body.style.paddingRight='';try{sessionStorage.removeItem('tsNewsOpen')}catch(e){}}"
    h += "function verNoticias(t){try{var p=document.getElementById('news-panel');if(!p)return;var lista=(typeof TS_NEWS!=='undefined'&&TS_NEWS[t])||[];p.innerHTML='';var hd=document.createElement('div');hd.className='news-head';var tt=document.createElement('b');tt.textContent='\U0001F525 '+t+' - noticias';var cx=document.createElement('button');cx.type='button';cx.className='news-close';cx.textContent='X';cx.onclick=cerrarNoticias;hd.appendChild(tt);hd.appendChild(cx);p.appendChild(hd);if(!lista.length){var e0=document.createElement('div');e0.className='news-item';e0.textContent='Sin detalle de noticias disponible en este momento.';p.appendChild(e0)}lista.forEach(function(n){var d=document.createElement('div');d.className='news-item';var ti=document.createElement('div');ti.className='news-title';ti.textContent=n.t||'(sin titulo)';d.appendChild(ti);var m=document.createElement('div');m.className='news-meta';var f='';try{f=n.h?new Date(n.h).toLocaleString():''}catch(e1){}m.textContent=[n.s||'',f].filter(Boolean).join(' - ');d.appendChild(m);if(n.r){var r=document.createElement('div');r.className='news-sum';r.textContent=n.r;d.appendChild(r)}if(n.u&&n.u.indexOf('http')===0){var a=document.createElement('a');a.href=n.u;a.target='_blank';a.rel='noopener noreferrer';a.textContent='Leer articulo completo';d.appendChild(a)}p.appendChild(d)});p.style.display='block';document.body.style.paddingRight='360px';try{sessionStorage.setItem('tsNewsOpen',t)}catch(e2){}}catch(e){}}"
    h += "document.addEventListener('input',function(ev){var t=ev.target;if(t&&t.tagName==='INPUT'&&t.type!=='checkbox')window._tsDirty=true},true);document.addEventListener('change',function(){window._tsDirty=false},true);document.addEventListener('focusout',function(){window._tsDirty=false},true);"
    h += "window.addEventListener('load',function(){setTimeout(function(){try{window.parent.postMessage({tsReady:1},'*')}catch(e){}},400)});"
    h += "</script></head><body>"
    _head_html = h  # encabezado común (CSS + JS) para los dos marcos
    h += "<div class='main-container'>"
    h += "<div class='topbar'><div class='brand'>TRADE<span style='color:#8f98a3'>SCANNER</span> <small>04:00–20:00 ET · REAL TIME</small></div>"
    h += "<div class='top-actions'>"
    h += "<a href='#' onclick='abrirRobotLong();return false;' style='display:inline-flex;align-items:center;justify-content:center;height:25px;padding:0 9px;margin-right:5px;border:1px solid #555;background:#252a31;color:#fff;border-radius:4px;text-decoration:none;font-size:10px;font-weight:700;'>🤖 ROBOT LONG</a>"
    # REFRESH / CUENTA / SALIR: los pinta la barra nativa (ts_ctrl_bar) superpuesta aquí.
    h += "</div>"
    _status_line_html = f"<div class='status-line'><div class='status {'on' if _estado_txt=='ON' else ('off' if _estado_txt=='OFF' else 'wait')}'>{'🟢' if _estado_txt=='ON' else ('🔴' if _estado_txt=='OFF' else '🟡')} MOTOR {_estado_txt} · HORARIO {_safe_text(_hora_txt)}</div><div class='date-time'>🕒 {fecha_hora_actual}</div></div>"
    h += "</div>"  # cierra topbar
    _le = {"Por encima": "ARRIBA", "Por debajo": "ABAJO", "Neutro": "NEUTRO"}
    _estados_ema = {20: ema20_estado_ui, 50: ema50_estado_ui, 200: ema200_estado_ui}

    def _cond_txt(n, cond):
        d = ema_dist_ui[n]
        return {"Ninguna": "sin condición extra", "Naciendo": "primera vela naciendo",
                "Distancia": f"a ≤ {d:g}% de la EMA", "Naciendo o distancia": f"naciendo o a ≤ {d:g}%",
                "Pullback a la baja": f"vela con pullback a la baja (mín. a ≤ {d:g}% de la EMA)",
                "Pullback a la alta": f"vela con pullback a la alta (máx. a ≤ {d:g}% de la EMA)"}.get(cond, cond)

    _ema_resumen_html = "".join(
        f"<div>EMA{n}: <b>{_le.get(_estados_ema[n], _estados_ema[n])}</b> · {_cond_txt(n, ema_cond_ui[n])}</div>"
        for n in (20, 50, 200)
    )
    _tab_validos = {"panel-radar", "panel-tecnicos", "panel-technical", "panel-config", "panel-conexiones", "panel-resultados", "panel-columnas"}
    _active_tab_ui = str(st.query_params.get("_active_tab", "") or "").strip()
    if _active_tab_ui not in _tab_validos or (_active_tab_ui == "panel-conexiones" and PUBLIC_PREVIEW):
        _active_tab_ui = "panel-radar"
    _subtab_validos = {"save-config-panel", "load-config-panel"}
    _active_subtab_ui = str(st.query_params.get("_active_subtab", "") or "").strip()
    if _active_subtab_ui not in _subtab_validos:
        _active_subtab_ui = "save-config-panel"

    h += "<div class='tabs'>"
    h += f"<button type='button' class='tab {'active' if _active_tab_ui == 'panel-radar' else ''}' data-tab-target='panel-radar'>RADAR</button>"
    h += f"<button type='button' class='tab {'active' if _active_tab_ui == 'panel-tecnicos' else ''}' data-tab-target='panel-tecnicos'>TÉCNICOS</button>"
    h += f"<button type='button' class='tab {'active' if _active_tab_ui == 'panel-technical' else ''}' data-tab-target='panel-technical'>TECHNICAL</button>"
    h += f"<button type='button' class='tab {'active' if _active_tab_ui == 'panel-config' else ''}' data-tab-target='panel-config'>CONFIGURACIÓN</button>"
    if not PUBLIC_PREVIEW:
        h += f"<button type='button' class='tab {'active' if _active_tab_ui == 'panel-conexiones' else ''}' data-tab-target='panel-conexiones'>🔌 CONEXIONES</button>"
    h += f"<button type='button' class='tab {'active' if _active_tab_ui == 'panel-resultados' else ''}' data-tab-target='panel-resultados'>RESULTADOS</button>"
    h += f"<button type='button' class='tab {'active' if _active_tab_ui == 'panel-columnas' else ''}' data-tab-target='panel-columnas'>COLUMNAS</button>"
    h += "</div>"
    h += f"<div id='panel-radar' class='tab-panel {'active' if _active_tab_ui == 'panel-radar' else ''}'><b>RADAR</b><br>Filtros principales del radar: precio, gap, flotación y volumen.</div>"
    h += f"<div id='panel-tecnicos' class='tab-panel {'active' if _active_tab_ui == 'panel-tecnicos' else ''}'><div class='panel-grid'>"
    if PUBLIC_PREVIEW:
        h += f"<div class='panel-card'><b>CRUCE EMA20</b><span>Condición actual: {_safe_text(ema_ui)} · vela nueva sobre EMA20.</span></div>"
    else:
        h += f"<div class='panel-card'><b>CONDICIONES EMA · ACTUALES</b><span>{_ema_resumen_html}</span></div>"
    h += f"<div class='panel-card'><b>MACD</b><span>Condición actual: {_safe_text(macd_ui)}.</span></div>"
    h += f"<div class='panel-card'><b>VOLUMEN</b><span>Mínimo configurado: {_big(volumen_min_ui)}.</span></div>"
    h += f"<div class='panel-card'><b>GAP</b><span>Rango configurado: {gap_min_ui:.1f}%–{gap_max_ui:.1f}%.</span></div>"
    h += "</div></div>"
    h += f"<div id='panel-technical' class='tab-panel {'active' if _active_tab_ui == 'panel-technical' else ''}'><div class='panel-grid'>"
    h += "<div class='panel-card technical-control' style='grid-column:1/-1;'><b>SWING EMA20 → EMA50 / EMA200</b>"
    h += f"<select id='swing_activo' onchange='aplicarTecnicas()'><option value='OFF' {'selected' if not swing_activo_ui else ''}>OFF · detector apagado</option><option value='ON' {'selected' if swing_activo_ui else ''}>ON · detectar swing</option></select>"
    h += f"<select id='swing_origen' onchange='aplicarTecnicas()'><option value='Bollinger inferior + debajo de EMA20' {'selected' if swing_origen_ui=='Bollinger inferior + debajo de EMA20' else ''}>Bollinger inferior + debajo de EMA20</option><option value='Bollinger inferior' {'selected' if swing_origen_ui=='Bollinger inferior' else ''}>Bollinger inferior</option><option value='Debajo de EMA20' {'selected' if swing_origen_ui=='Debajo de EMA20' else ''}>Debajo de EMA20</option></select>"
    h += f"<select id='swing_objetivo' onchange='aplicarTecnicas()'><option value='EMA50 o EMA200' {'selected' if swing_objetivo_ui=='EMA50 o EMA200' else ''}>Primer toque EMA50 o EMA200</option><option value='EMA50' {'selected' if swing_objetivo_ui=='EMA50' else ''}>Primer toque EMA50</option><option value='EMA200' {'selected' if swing_objetivo_ui=='EMA200' else ''}>Primer toque EMA200</option></select>"
    h += f"<div class='range'><span>Máx. velas desde cruce</span><input type='number' min='1' max='30' step='1' id='swing_ventana' value='{swing_ventana_ui}' onchange='aplicarTecnicas()'><span>Toque %</span><input type='number' min='0' max='10' step='0.1' id='swing_tolerancia' value='{swing_tol_ui:g}' onchange='aplicarTecnicas()'><span>Origen %</span><input type='number' min='0' max='10' step='0.1' id='swing_origen_tolerancia' value='{swing_origen_tol_ui:g}' onchange='aplicarTecnicas()'></div>"
    h += f"<select id='swing_multitimeframe' onchange='aplicarTecnicas()'><option value='OFF' {'selected' if not swing_multitimeframe_ui else ''}>Una temporalidad</option><option value='ON' {'selected' if swing_multitimeframe_ui else ''}>Multitemporal</option></select>"
    h += "<div class='range'><span>Temporalidades:</span>"
    for _mtf,_mtxt in (("1d","1D"),("1w","1W"),("1mo","1M")):
        h += f"<label style='display:inline-flex;align-items:center;gap:3px;'><input type='checkbox' class='swing-tf-check' value='{_mtf}' {'checked' if _mtf in swing_tfs_ui else ''} onchange='aplicarTecnicas()'>{_mtxt}</label>"
    h += "</div><span>Zona inferior → cruce EMA20 desde abajo → continuación → primer toque EMA50/EMA200. En multitemporal se agrupan los tickers.</span></div>"
    _lbl_cond = (("Ninguna", "SIN CONDICIÓN EXTRA"), ("Naciendo", "PRIMERA VELA NACIENDO"),
                 ("Distancia", "A ≤ DISTANCIA % DE LA EMA"), ("Naciendo o distancia", "NACIENDO O ≤ DISTANCIA %"))
    for _n, _ename, _eval in ((20, "EMA20", ema20_estado_ui), (50, "EMA50", ema50_estado_ui), (200, "EMA200", ema200_estado_ui)):
        _eid = f"ema{_n}_estado"
        h += f"<div class='panel-card technical-control'><b>{_ename}</b>"
        h += f"<select id='{_eid}' onchange='aplicarTecnicas()'><option value='Por encima' {'selected' if _eval=='Por encima' else ''}>ARRIBA (vela sobre {_ename})</option><option value='Por debajo' {'selected' if _eval=='Por debajo' else ''}>ABAJO (vela bajo {_ename})</option><option value='Neutro' {'selected' if _eval=='Neutro' else ''}>NEUTRO</option></select>"
        h += f"<select id='ema{_n}_cond' onchange='aplicarTecnicas()'>"
        _opts_cond = list(_lbl_cond)
        # Pullback EMA50/EMA200 reemplazado por el detector Swing.
        for _v, _t in _opts_cond:
            h += f"<option value='{_v}' {'selected' if ema_cond_ui[_n]==_v else ''}>{_t}</option>"
        h += "</select>"
        h += f"<div class='range'><input type='number' step='0.1' min='0' max='25' id='ema{_n}_dist' value='{ema_dist_ui[_n]:g}' onchange='aplicarTecnicas()'><span>% distancia máx.</span></div>"
        h += f"<span>Filtro real frente a {_ename} en {timeframe_ui.upper()}.</span></div>"
    h += f"<div class='panel-card technical-control'><b>RSI (14) · RANGO</b><div class='range'><input type='number' step='1' min='0' max='100' id='rsi_min' value='{rsi_min_ui:g}'><span>–</span><input type='number' step='1' min='0' max='100' id='rsi_max' value='{rsi_max_ui:g}'></div><button onclick='pushConfig()' style='width:100%;height:24px;'>APLICAR RSI</button><span>Filtra las señales por RSI(14) en la temporalidad seleccionada.</span></div>"
    h += f"<div class='panel-card'><b>MACD</b><span>{_safe_text(macd_ui)} · cálculo actual: {timeframe_ui.upper()} · EMA20/MACD/RSI usan esta misma temporalidad.</span></div>"
    h += "<div class='panel-card'><b>MEDIAS</b><span>EMA20 · EMA50 · EMA200 calculadas en el timeframe seleccionado.</span></div>"
    h += "<div class='panel-card'><b>BOLLINGER</b><span>Bandas y distancia a banda.</span></div>"
    h += "<div class='panel-card'><b>MFI</b><span>Money Flow Index.</span></div>"
    h += "<div class='panel-card'><b>VOLATILIDAD</b><span>ATR · Beta.</span></div>"
    h += "<div class='panel-card'><b>PERFORMANCE</b><span>Semana · mes · trimestre · YTD · año.</span></div>"
    h += "<div class='panel-card'><b>GAP / VOLUMEN</b><span>Gap % · volumen actual · volumen promedio · relativo.</span></div>"
    h += "</div></div>"
    h += "<div class='technical-subtabs'><button type='button' class='technical-subtab active' data-subtab-target='save-config-panel'>💾 GUARDAR CONFIGURACIÓN</button><button type='button' class='technical-subtab' data-subtab-target='load-config-panel'>📂 MIS CONFIGURACIONES</button></div>"
    h += "<div id='save-config-panel' class='technical-subpanel active'><div class='panel-card technical-control'><b>💾 GUARDAR CONFIGURACIÓN PERSONAL</b><div class='range'><input id='config_name' type='text' placeholder='Nombre de configuración'><button type='button' class='btn-guardar-config'>GUARDAR</button></div><span>Los filtros y la posición de la pantalla se guardan automáticamente. Aquí puedes crear una copia con nombre.</span></div></div>"
    h += "<div id='load-config-panel' class='technical-subpanel'><div class='panel-card technical-control'><b>📂 MIS CONFIGURACIONES</b><input id='config_search' type='text' placeholder='Buscar configuración' oninput='renderConfiguraciones()'><div id='saved_configs_list'></div></div></div>"
    h += f"<div id='panel-config' class='tab-panel {'active' if _active_tab_ui == 'panel-config' else ''}'><div class='panel-card broker-main-card' style='grid-column:1/-1;border:1px solid #d4af37;background:#242a31;'>"
    h += f"<b style='font-size:12px;color:#d4af37;'>🔗 BROKER ENTRELAZADO CON EL SCANNER</b><span style='display:block;margin-bottom:5px;'>Broker activo: <strong>{_safe_text(broker_val)}</strong> · Los activos encontrados pueden enviarse desde el engranaje de Layout.</span>"
    h += "<span style='display:block;'>Charles Schwab: OAuth 2.0 · Credenciales: <strong>SCHWAB_CLIENT_ID</strong>, <strong>SCHWAB_CLIENT_SECRET</strong> y <strong>SCHWAB_REDIRECT_URI</strong> en Streamlit Secrets.</span>"
    h += "</div><div class='panel-grid'>"
    h += f"<div class='panel-card'><b>MOTOR</b><span>{_safe_text(_estado_txt)} · Horario {_safe_text(_hora_txt)}</span></div>"
    h += f"<div class='panel-card'><b>BROKER</b><span>{_safe_text(broker_val)} · API Key/Secret Key se introducen en Configuración y no se muestran en resultados.</span></div>"
    h += f"<div class='panel-card'><b>VENTANA</b><span>{_safe_text(wnd_val)}</span></div>"
    h += f"<div class='panel-card'><b>PUENTE DE LAYOUT</b><span>{_safe_text(bridge_val)}</span></div>"
    h += "</div></div>"
    h += "<style>.tf-badge{font-size:9px;font-weight:900;color:#d4af37;margin-left:3px}.col-row{display:flex;justify-content:space-between;align-items:center;border-top:1px solid #444;padding:4px 0}.col-row label{font-size:11px;cursor:pointer}.col-row button{width:30px;height:22px;background:#252a31;color:#fff;border:1px solid #555;margin-left:3px;cursor:pointer}.col-row button:disabled{opacity:.3;cursor:default}#cols_list{margin:6px 0}</style>"
    if PUBLIC_PREVIEW:
        h += "<style>.filtros-grid select,.filtros-grid input,.filtros-grid button,.panel-card select,.panel-card input,.panel-card button,.technical-subtab,.engranaje-select{pointer-events:none!important;opacity:.58!important;cursor:not-allowed!important}.tab{pointer-events:auto!important;opacity:1!important}</style>"
    h += f"<div id='panel-columnas' class='tab-panel {'active' if _active_tab_ui == 'panel-columnas' else ''}'><b>COLUMNAS DE LA TABLA</b><br>Marca una columna para mostrarla u ocultarla y usa ▲ ▼ para moverla de lugar. Se guarda en tu navegador y no afecta al motor.<div id='cols_list'></div><button type='button' data-col-act='reset' style='height:24px;padding:0 10px;background:#252a31;color:#fff;border:1px solid #555;cursor:pointer;'>RESTABLECER</button></div>"
    if not PUBLIC_PREVIEW:
        h += f"<div id='panel-conexiones' class='tab-panel {'active' if _active_tab_ui == 'panel-conexiones' else ''}'><div class='panel-grid'>"
        h += "<div class='panel-card' style='grid-column:1/-1;'><b>🔌 PUENTE DE LAYOUT</b><span style='display:block;margin-top:6px;'>Escribe la dirección del puente que está funcionando en tu PC. Normalmente: <b>http://localhost:8080/layout</b></span>"
        h += "<div style='display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:8px;'><input id='bridge_url_conn' type='text' value='" + _safe_text(bridge_val) + "' style='flex:1 1 320px;min-width:240px;height:30px;padding:4px 7px;box-sizing:border-box;'><button type='button' onclick=\"probarPuente()\" style='height:30px;padding:0 14px;font-weight:700;'>🔗 CONECTAR PUENTE</button></div>"
        h += "<div id='bridge_status' style='margin-top:8px;font-size:11px;'>⚪ PUENTE SIN PROBAR</div>"
        h += "<div style='margin-top:8px;font-size:10px;color:#8f98a3;'>El puente se ejecuta en la computadora del usuario. El scanner no necesita instalarse allí.</div></div>"
        h += "<div class='panel-card' style='grid-column:1/-1;'><b>🏦 BROKER</b><span style='display:block;margin-top:6px;'>La conexión de la cuenta se añadirá en el siguiente paso con autorización segura. No se piden claves en esta fase.</span>"
        h += "<div style='margin-top:8px;font-size:11px;color:#cbd1d8;'>Objetivo: que el usuario solo tenga que autorizar y pulsar CONECTAR.</div></div>"
        h += "</div></div>"
    h += f"<div id='panel-resultados' class='tab-panel {'active' if _active_tab_ui == 'panel-resultados' else ''}'><b>RESULTADOS EN VIVO</b><br>Las señales encontradas por el motor aparecen en la tabla de 10 líneas inferior.</div>"
    def _ctl_res(label, texto, campos):
        def _v(i, d=""):
            for k, v in campos:
                if k == i:
                    return str(v)
            return d
        if label == "PRECIO ($)":
            return f"<div class='filtro-item'><label>{label}</label><div class='range'><input type='number' step='0.01' id='price_min' value='{_safe_text(_v('price_min', precio_min_ui))}' onchange='pushConfig()'><span>–</span><input type='number' step='0.01' id='price_max' value='{_safe_text(_v('price_max', precio_max_ui))}' onchange='pushConfig()'></div></div>"
        if label == "GAP (%)":
            return f"<div class='filtro-item'><label>{label}</label><div class='range'><input type='number' step='0.1' id='gap_min' value='{_safe_text(_v('gap_min', gap_min_ui))}' onchange='pushConfig()'><span>–</span><input type='number' step='0.1' id='gap_max' value='{_safe_text(_v('gap_max', gap_max_ui))}' onchange='pushConfig()'></div></div>"
        if label == "FLOTACIÓN ≤":
            return f"<div class='filtro-item'><label>{label}</label><input type='number' id='float_max' value='{_safe_text(_v('float_max', float_max_ui))}' onchange='pushConfig()'></div>"
        if label == "VOLUMEN ≥":
            return f"<div class='filtro-item'><label>{label}</label><input type='number' id='txt_vol' value='{_safe_text(_v('txt_vol', volumen_min_ui))}' onchange='pushConfig()'></div>"
        if label == "MACD":
            v=_v('sel_mac', macd_ui)
            return f"<div class='filtro-item'><label>{label}</label><select id='sel_mac' onchange='pushConfig()'><option value='Positivo' {'selected' if v=='Positivo' else ''}>Positivo</option><option value='Negativo' {'selected' if v=='Negativo' else ''}>Negativo</option><option value='No exigir' {'selected' if v=='No exigir' else ''}>No exigir</option></select></div>"
        if label == "ORDENAR":
            v=_v('sel_order', orden_ui)
            return f"<div class='filtro-item'><label>{label}</label><select id='sel_order' onchange='pushConfig()'><option value='Actualizado' {'selected' if v=='Actualizado' else ''}>Actualizado</option><option value='Cambio %' {'selected' if v=='Cambio %' else ''}>Cambio %</option><option value='Volumen' {'selected' if v=='Volumen' else ''}>Volumen</select></div>"
        if label in ("MOTOR CENTRAL", "MOTOR PERSONAL"):
            v=_v('cfg_active', active_val)
            return f"<div class='filtro-item'><label>{label}</label><select id='cfg_active' onchange='pushConfig()'><option value='True' {'selected' if v=='True' else ''}>🟢 ON</option><option value='False' {'selected' if v!='True' else ''}>🔴 OFF</option></select></div>"
        if label == "IDIOMA":
            v=_v('cfg_lang', lang_val)
            langs=(('ESP','Español'),('ENG','English'),('POR','Português'),('FRA','Français'),('DEU','Deutsch'),('ITA','Italiano'),('CHN','中文'),('JPN','日本語'))
            opts=''.join(f"<option value='{k}' {'selected' if v==k else ''}>{name}</option>" for k,name in langs)
            return f"<div class='filtro-item'><label>{label}</label><select id='cfg_lang' onchange='pushConfig();aplicarIdioma(this.value)'>{opts}</select></div>"
        if label == "BROKER":
            v=_v('cfg_broker', broker_val)
            return f"<div class='filtro-item'><label>{label}</label><select id='cfg_broker' onchange='pushConfig()'><option value='Interactive Brokers' {'selected' if v=='Interactive Brokers' else ''}>Interactive Brokers</option><option value='Tradestation' {'selected' if v=='Tradestation' else ''}>Tradestation</option><option value='Charles Schwab' {'selected' if v=='Charles Schwab' else ''}>Charles Schwab</option><option value='Otro' {'selected' if v=='Otro' else ''}>Otro</option></select></div>"
        if label == "VENTANA":
            v=_v('cfg_wnd', wnd_val)
            return f"<div class='filtro-item'><label>{label}</label><select id='cfg_wnd' onchange='pushConfig();if(this.value===&quot;Flotante&quot;)abrirVentanaFlotante()'><option value='Incrustada' {'selected' if v=='Incrustada' else ''}>Incrustada</option><option value='Flotante' {'selected' if v=='Flotante' else ''}>Flotante</option></select></div>"
        if label == "MOTOR":
            v=_v('cfg_active', active_val)
            return f"<div class='filtro-item'><label>{label}</label><select id='cfg_active' onchange='pushConfig()'><option value='True' {'selected' if v=='True' else ''}>🟢 ON</option><option value='False' {'selected' if v!='True' else ''}>🔴 OFF</option></select></div>"
        if label == "PUENTE DE LAYOUT":
            v=_v('cfg_url', bridge_val)
            return f"<div class='filtro-item'><label>{label}</label><input type='text' id='cfg_url' value='{_safe_text(v)}' style='width:100%;' onchange='pushConfig()'></div>"
        return f"<div class='filtro-item'><label>{label}</label><span style='font-size:11px;'>{_safe_text(texto)}</span></div>"

    if ES_ADMIN and not PUBLIC_PREVIEW:
        _m1 = servicio.metricas_snapshot(); _up_h = int(_m1.get("uptime_segundos",0)//3600); _up_m = int((_m1.get("uptime_segundos",0)%3600)//60)
        h += ("<div class='simple-card' style='margin:4px 0 6px;'><div class='simple-title'>📊 MOTOR · CONSUMO REAL · FASE 1</div>"
              f"<div style='font-size:10px;line-height:1.55;color:#d7d0bd;'><b>Uptime:</b> {_up_h}h {_up_m}m · <b>Ciclos:</b> {int(_m1.get('ciclos',0))} · <b>Prom:</b> {_m1.get('ciclo_promedio',0):.2f}s · <b>Máx:</b> {_m1.get('duracion_ciclo_max',0):.2f}s<br>"
              f"<b>Universo:</b> {int(_m1.get('simbolos_universo',0)):,} · <b>Snapshots:</b> {int(_m1.get('snapshots',0))} · <b>Bars:</b> {int(_m1.get('bars',0))} · <b>Resultados:</b> {int(_m1.get('resultados_publicados',0))}<br>"
              f"<b>FMP:</b> {int(_m1.get('fmp_total',0))} · <b>Bulk:</b> {int(_m1.get('fmp_bulk',0))} · <b>Individual:</b> {int(_m1.get('fmp_individual',0))} · <b>429:</b> {int(_m1.get('fmp_429',0))} · <b>Cache H/M:</b> {int(_m1.get('fmp_cache_hits',0))}/{int(_m1.get('fmp_cache_misses',0))}<br>"
              f"<b>Alpaca errores:</b> {int(_m1.get('errores_alpaca',0))} · <b>429:</b> {int(_m1.get('alpaca_429',0))} · <b>Tiempo Snap:</b> {_m1.get('tiempo_snapshots',0):.1f}s · <b>Bars:</b> {_m1.get('tiempo_bars',0):.1f}s · <b>FMP:</b> {_m1.get('tiempo_fmp',0):.1f}s</div></div>")

    h += "<div class='filtros-grid'>"
    h += "<div class='logo'>TRADE SCANNER</div>"
    # (El selector de REFRESH vive solo en la barra nativa superior; antes estaba duplicado aqui.)
    if PUBLIC_PREVIEW:
        h += "<div class='filtro-item'><label>MOTOR</label><select disabled><option>👀 SOLO LECTURA</option></select></div>"
    elif ES_ADMIN:
        h += _ctl_res("MOTOR CENTRAL", "🟢 ON" if active_val == "True" else "🔴 OFF", [("cfg_active", active_val)])
    else:
        h += _ctl_res("MOTOR PERSONAL", "🟢 ON" if active_val == "True" else "🔴 OFF", [("cfg_active", active_val)])
    if ES_ADMIN:
        h += f"<div class='filtro-item'><label>HORARIO GLOBAL</label><div class='range'><input type='time' id='cfg_start' value='{start_time}'><span>–</span><input type='time' id='cfg_end' value='{end_time}'></div></div>"
    elif USUARIO_AUTENTICADO:
        h += f"<div class='filtro-item'><label>MI HORARIO</label><div class='range'><input type='time' id='cfg_start' value='{start_time}'><span>–</span><input type='time' id='cfg_end' value='{end_time}'></div></div>"
    else:
        h += "<div class='filtro-item'><label>HORARIO (ET)</label><span>04:00 – 20:00 · solo lectura</span></div>"
    if PUBLIC_PREVIEW:
        _langs_pub=(('ESP','Español'),('ENG','English'),('POR','Português'),('FRA','Français'),('DEU','Deutsch'),('ITA','Italiano'),('CHN','中文'),('JPN','日本語'))
        h += "<div class='filtro-item'><label>IDIOMA</label><select id='cfg_lang' onchange='pushConfig();aplicarIdioma(this.value)'>"
        for _lk, _ln in _langs_pub:
            h += f"<option value='{_lk}' {'selected' if lang_val==_lk else ''}>{_ln}</option>"
        h += "</select></div>"
    else:
        h += _ctl_res("IDIOMA", lang_val, [("cfg_lang", lang_val)])
    if PUBLIC_PREVIEW:
        h += f"<div class='filtro-item'><label>VENTANA</label><select id='cfg_wnd' onchange='pushConfig();if(this.value===&quot;Flotante&quot;)abrirVentanaFlotante()'><option value='Incrustada' {'selected' if wnd_val=='Incrustada' else ''}>Incrustada</option><option value='Flotante' {'selected' if wnd_val=='Flotante' else ''}>Flotante</option></select></div>"
    else:
        h += _ctl_res("VENTANA", wnd_val, [("cfg_wnd", wnd_val)])
    if ES_ADMIN:
        h += f"<div class='filtro-item'><label>HORARIO DEL SCANNER</label><span>GLOBAL · {start_time}–{end_time} ET</span></div>"
    elif USUARIO_AUTENTICADO:
        h += f"<div class='filtro-item'><label>MI HORARIO</label><span>{start_time}–{end_time} ET</span></div>"
    else:
        h += "<div class='filtro-item'><label>HORARIO DEL SCANNER</label><span>04:00–20:00 ET · solo lectura</span></div>"
    h += "<div class='filtro-item'><label>TEMPORALIDAD</label>"
    if swing_activo_ui and swing_multitimeframe_ui:
        h += "<select id='timeframe' disabled><option>NEUTRO · MULTITEMPORAL</option></select><span>SWING MULTITEMPORAL</span></div>"
    else:
        h += "<select id='timeframe' onchange='cambiarTimeframeTecnico(this.value)'>"
        for _tf in (("1m","1 MIN"),("3m","3 MIN"),("5m","5 MIN"),("10m","10 MIN"),("13m","13 MIN"),("15m","15 MIN"),("30m","30 MIN"),("1h","1 HORA"),("1d","1 DÍA"),("1w","1 SEMANA"),("1mo","1 MES")):
            h += f"<option value='{_tf[0]}' {'selected' if timeframe_ui==_tf[0] else ''}>{_tf[1]}</option>"
        h += "</select>"
    h += "</div>"
    h += f"<div class='filtro-item'><label>FLOAT · FILTRO</label><select id='f_float_on' onchange='pushConfig()'><option value='OFF' {'selected' if _qtxt('f_float_on','OFF')=='OFF' else ''}>OFF · informativo</option><option value='ON' {'selected' if _qtxt('f_float_on','OFF')=='ON' else ''}>ON · filtrar</option></select></div>"
    if PUBLIC_PREVIEW:
        h += f"<div class='filtro-item'><label>DISTANCIA EMA20 ≤ %</label><input type='number' step='0.1' id='ema_dist_max' value='{ema_dist_max_ui:g}'></div>"
    else:
        h += f"<input type='hidden' id='ema_dist_max' value='{ema_dist_max_ui:g}'>"
    if PUBLIC_PREVIEW:
        h += f"<div class='filtro-item'><label>PRECIO ($)</label><div class='range'><input type='number' step='0.01' id='price_min' value='{precio_min_ui:g}' onchange='pushConfig()'><span>–</span><input type='number' step='0.01' id='price_max' value='{precio_max_ui:g}' onchange='pushConfig()'></div></div>"
    else:
        h += "<input type='hidden' id='price_min' value='" + _safe_text(precio_min_ui) + "'><input type='hidden' id='price_max' value='" + _safe_text(precio_max_ui) + "'>"
    h += f"<div class='filtro-item'><label>VOLUMEN · FILTRO</label><select id='f_vol_on' onchange='pushConfig()'><option value='OFF' {'selected' if _qtxt('f_vol_on','OFF')=='OFF' else ''}>OFF · informativo</option><option value='ON' {'selected' if _qtxt('f_vol_on','OFF')=='ON' else ''}>ON · filtrar</option></select></div>"
    h += f"<div class='filtro-item'><label>GAP · FILTRO</label><select id='f_gap_on' onchange='pushConfig()'><option value='OFF' {'selected' if _qtxt('f_gap_on','OFF')=='OFF' else ''}>OFF · informativo</option><option value='ON' {'selected' if _qtxt('f_gap_on','OFF')=='ON' else ''}>ON · filtrar</option></select></div>"
    h += f"<div class='filtro-item'><label>EMA20 · FILTRO</label><select id='ema20_on' onchange='pushConfig()'><option value='OFF' {'selected' if _qtxt('ema20_on','OFF')=='OFF' else ''}>OFF · informativo</option><option value='ON' {'selected' if _qtxt('ema20_on','OFF')=='ON' else ''}>ON · filtrar</option></select></div>"
    if PUBLIC_PREVIEW:
        h += f"<div class='filtro-item'><label>FLOTACIÓN ≤</label><input type='number' id='float_max' value='{float_max_ui}' onchange='pushConfig()'></div>"
    else:
        h += _ctl_res("FLOTACIÓN ≤", f"{float_max_ui:,}", [("float_max", str(float_max_ui))])
    if PUBLIC_PREVIEW:
        h += f"<div class='filtro-item'><label>VOLUMEN ≥</label><input type='number' id='txt_vol' value='{volumen_min_ui}' onchange='pushConfig()'></div>"
    else:
        h += _ctl_res("VOLUMEN ≥", f"{volumen_min_ui:,}", [("txt_vol", str(volumen_min_ui))])
    if PUBLIC_PREVIEW:
        h += f"<div class='filtro-item'><label>GAP (%)</label><div class='range'><input type='number' step='0.1' id='gap_min' value='{gap_min_ui:g}' onchange='pushConfig()'><span>–</span><input type='number' step='0.1' id='gap_max' value='{gap_max_ui:g}' onchange='pushConfig()'></div></div>"
    else:
        h += "<input type='hidden' id='gap_min' value='" + _safe_text(gap_min_ui) + "'><input type='hidden' id='gap_max' value='" + _safe_text(gap_max_ui) + '">'
    if PUBLIC_PREVIEW:
        h += f"<div class='filtro-item'><label>CRUCE EMA</label><select id='sel_ema'><option value='Hacia arriba' {'selected' if ema_ui=='Hacia arriba' else ''}>Vela nueva sobre EMA20</option><option value='Hacia abajo' {'selected' if ema_ui=='Hacia abajo' else ''}>Hacia abajo</option><option value='Neutro' {'selected' if ema_ui=='Neutro' else ''}>Neutro</option></select></div>"
    else:
        # Las condiciones EMA se muestran una sola vez en el panel TÉCNICOS.
        # Aquí no se repite el resumen ni se presenta un valor "fijo".
        h += "<input type='hidden' id='sel_ema' value='" + _safe_text(ema_ui) + "'>"
    if PUBLIC_PREVIEW:
        h += f"<div class='filtro-item'><label>MACD</label><select id='sel_mac'><option value='Positivo' {'selected' if macd_ui=='Positivo' else ''}>Positivo</option><option value='Negativo' {'selected' if macd_ui=='Negativo' else ''}>Negativo</option><option value='No exigir' {'selected' if macd_ui=='No exigir' else ''}>No exigir</option></select></div>"
    else:
        h += _ctl_res("MACD", macd_ui, [("sel_mac", macd_ui)])
    if PUBLIC_PREVIEW:
        h += f"<div class='filtro-item'><label>ORDENAR</label><select id='sel_order'><option value='Actualizado' {'selected' if orden_ui=='Actualizado' else ''}>Actualizado</option><option value='Cambio %' {'selected' if orden_ui=='Cambio %' else ''}>Cambio %</option><option value='Volumen' {'selected' if orden_ui=='Volumen' else ''}>Volumen</option></select></div>"
    else:
        h += _ctl_res("ORDENAR", orden_ui, [("sel_order", orden_ui)])
    if PUBLIC_PREVIEW:
        h += f"<div class='filtro-item'><label>BROKER</label><select id='cfg_broker'><option value='Interactive Brokers' {'selected' if broker_val in ('Interactive Brokers','Interactive Brokers (TWS)') else ''}>Interactive Brokers</option><option value='Tradestation' {'selected' if broker_val=='Tradestation' else ''}>Tradestation</option><option value='Charles Schwab' {'selected' if broker_val=='Charles Schwab' else ''}>Charles Schwab</option><option value='Otro' {'selected' if broker_val in ('Otro','Otro (webhook)') else ''}>Otro</option></select></div>"
    else:
        h += _ctl_res("BROKER", broker_val, [("cfg_broker", broker_val)])
    if PUBLIC_PREVIEW:
        h += f"<div class='filtro-item'><label>PUENTE DE LAYOUT</label><input type='text' id='cfg_url' value='{_safe_text(bridge_val)}' style='width:100%;'></div>"
    else:
        h += _ctl_res("PUENTE DE LAYOUT", bridge_val, [("cfg_url", bridge_val)])
    if PUBLIC_PREVIEW:
        h += "<div class='filtro-item' style='justify-content:center;'><button onclick='pushConfig()' style='width:100%;height:22px;'>APLICAR / GUARDAR CONEXIÓN</button></div>"
    else:
        h += "<div class='filtro-item'><label>CONTROLES</label><span style='font-size:10px;line-height:1.35;'>Los filtros, temporalidad, EMA, idioma y refresh se cambian directamente dentro de este cuadro gris.</span></div>"
    h += "<div class='filtro-item schwab-item'><label>CHARLES SCHWAB</label><span class='schwab-info'>OAuth 2.0 · La API oficial no expone layouts de thinkorswim; el envío al layout se realiza mediante el PUENTE configurado.</span><button type='button' onclick='conectarSchwab()' style='width:auto;min-width:190px;height:26px;flex:0 0 auto;'>🔐 CONECTAR / AUTORIZAR SCHWAB</button></div>"
    h += "</div>"
    _schwab_status_txt = str(st.session_state.get("schwab_status", ""))
    _schwab_connected = bool(_schwab_access_token())
    _schwab_url = _schwab_authorize_url()
    if str(st.query_params.get("schwab_connect", "0")) == "1":
        if _schwab_url:
            h += f"<div class='panel-card' style='margin:6px 0;'><b>CHARLES SCHWAB</b><span>Autoriza tu cuenta con OAuth 2.0.</span><a href='{_safe_text(_schwab_url)}' target='_top' style='display:inline-block;margin-top:5px;padding:5px 9px;background:#d4af37;color:#000;text-decoration:none;font-weight:800;border-radius:3px;'>ABRIR AUTORIZACIÓN SCHWAB</a></div>"
        else:
            h += "<div class='panel-card' style='margin:6px 0;'><b>CHARLES SCHWAB</b><span>Configura SCHWAB_CLIENT_ID, SCHWAB_CLIENT_SECRET y SCHWAB_REDIRECT_URI en Streamlit Secrets.</span></div>"
    if _schwab_status_txt:
        h += f"<div class='panel-card' style='margin:6px 0;'><b>ESTADO SCHWAB</b><span>{_safe_text(_schwab_status_txt)}</span></div>"
    if _schwab_connected:
        h += "<div class='panel-card' style='margin:6px 0;border-color:#37c77a;'><b>🟢 CHARLES SCHWAB CONECTADO</b><span>La autorización OAuth está activa en esta sesión.</span></div>"
    _layout_status = str(st.session_state.get("layout_send_status", ""))
    if _layout_status:
        h += f"<div class='panel-card' style='margin:6px 0;border-color:#d4af37;'><b>ENVÍO AL LAYOUT</b><span>{_safe_text(_layout_status)}</span></div>"
    if PUBLIC_PREVIEW:
        h += "<div class='refresh-bar'><label>⏱ REFRESH</label><select disabled><option>⏱ 3 min (fijo)</option></select><span>Regístrate para elegir tu propio refresh.</span><button type='button' class='float-btn' onclick='abrirVentanaFlotante()'>⧉ VENTANA FLOTANTE</button></div>"
    else:
        h += "<div class='refresh-bar'><label>⏱ REFRESH</label><select id='refresh_sec_inside' onchange='cambiarRefresh(this.value)'>"
        for _rv in refresh_options:
            _rl = f"{_rv}s" if _rv < 60 else (f"{_rv//60} min" if _rv % 60 == 0 else f"{_rv}s")
            h += f"<option value='{_rv}' {'selected' if refresh_sec==_rv else ''}>⏱ {_rl}</option>"
        h += "</select><span>Cada cuánto se actualizan los resultados.</span><button type='button' class='float-btn' onclick='abrirVentanaFlotante()'>⧉ VENTANA FLOTANTE</button></div>"
    h += f"<div class='subline'><span><b>Señales:</b> {len(filas_reales)}</span><span><b>Velas:</b> {'NEUTRO · MULTITEMPORAL' if (swing_activo_ui and swing_multitimeframe_ui) else timeframe_ui.upper()}</span><span><b>Precio:</b> ${precio_min_ui:.2f}–${precio_max_ui:.2f}</span><span><b>Gap:</b> {gap_min_ui:.1f}%–{gap_max_ui:.1f}%</span><span><b>Float:</b> ≤ {float_max_ui/1_000_000:.1f}M</span><span><b>Vol:</b> ≥ {_big(volumen_min_ui)}</span><span><b>EMA20:</b> { _safe_text(ema_ui) }</span><span><b>MACD:</b> { _safe_text(macd_ui) }</span><span><b>RSI:</b> {rsi_min_ui:.0f}–{rsi_max_ui:.0f}</span></div>"
    # El diagnóstico del embudo se conserva internamente en el motor y no se muestra
    # como un bloque fijo antes de RESULTADOS.
    # Un único marco HTML para TODO el scanner.
    # Antes se separaba en dos components.html(); eso dejaba la carátula gris
    # en un iframe y los resultados en otro, y en determinadas cargas el primero
    # aparecía vacío. Ahora todo comparte el mismo DOM y CSS.
    _h_a = h + "</div></body></html>"

    h = _head_html + "<div class='main-container'>" + _status_line_html
    # El diagnóstico del embudo permanece interno en el motor.
    # No se muestra como texto fijo antes de RESULTADOS.
    h += "<div class='result-title'>RESULTADOS · VISUALIZACIÓN · 10 LÍNEAS</div>"
    h += "<div id='resultados-tabla' class='table-wrapper'><table><thead><tr>"
    h += f"<th class='layout-col' data-col='layout'>🔗 Layout</th><th data-col='ticker'>Ticker</th><th data-col='sector'>Sector</th><th data-col='precio'>Precio ($)</th><th data-col='cambio'>Cambio %</th><th data-col='volumen'>Volumen</th><th data-col='gap'>Gap %</th><th data-col='flot'>Flotación (M)</th>"
    if _show_ema20: h += f"<th data-col='ema20'>EMA20 ({timeframe_ui})</th>"
    if _show_ema50: h += f"<th data-col='ema50'>EMA50 ({timeframe_ui})</th>"
    if _show_ema200: h += f"<th data-col='ema200'>EMA200 ({timeframe_ui})</th>"
    h += f"<th data-col='macd'>MACD ({timeframe_ui})</th>"
    h += "</tr></thead><tbody>" + rows_html + "</tbody></table></div>"
    h += "<script>try{aplicarColumnas()}catch(e){}</script>"
    try:
        _det_noticias = getattr(servicio, "noticias_detalle", {}) or {}
        _noticias_ui = {}
        for _fr in filas_visualizacion:
            if _fr and _fr.get("tiene_noticia"):
                _noticias_ui[str(_fr.get("ticker"))] = list(_det_noticias.get(_fr.get("ticker"), []))[:6]
        _json_news = json.dumps(_noticias_ui).replace("</", "<\\/")
    except Exception:
        _json_news = "{}"
    h += "<div id='news-panel'></div><script>var TS_NEWS=" + _json_news + ";try{var _o=sessionStorage.getItem('tsNewsOpen');if(_o&&TS_NEWS[_o])verNoticias(_o)}catch(e){}</script>"
    _ultima_scan_txt = servicio.ultima_actualizacion.strftime("%H:%M:%S ET") if servicio.ultima_actualizacion else "aún no ejecutado"
    _error_scan_txt = str(getattr(servicio, "ultimo_error", "") or "").strip()
    if len(_error_scan_txt) > 140:
        _error_scan_txt = _error_scan_txt[:140] + "…"
    _hilo_vivo = bool(getattr(getattr(servicio, "_hilo", None), "is_alive", lambda: False)())
    _hilo_txt = "HILO OK" if _hilo_vivo else "HILO DETENIDO"
    _universo_txt = str(len(getattr(servicio, "universo", []) or []))
    h += f"<div class='footer-note'><span>Motor real · Técnico: {timeframe_ui.upper()} · {len(filas_reales)} resultado(s) · Último escaneo: {_safe_text(_ultima_scan_txt)} · {_hilo_txt} · Universo: {_universo_txt}</span><span>Estado: {_safe_text(_estado_txt)} · {_safe_text(_error_scan_txt) if _error_scan_txt else _safe_text(_hora_txt)}</span></div>"

    # Insertamos el bloque de resultados dentro del mismo main-container del panel.
    _panel_final = _h_a.rsplit("</div></body></html>", 1)[0]
    _result_body = h[len(_head_html):]
    if _result_body.startswith("<div class='main-container'>"):
        _result_body = _result_body[len("<div class='main-container'>"):]
    if _result_body.endswith("</div></body></html>"):
        _result_body = _result_body[:-len("</div></body></html>")]
    _panel_final += _result_body + "</div></body></html>"

    # Un solo iframe. La altura permite mostrar controles y tabla sin crear un
    # segundo marco blanco debajo.
    h = _panel_final

    # ── Controles de cuenta y refresh ──
    # Auth/Logout usan navegación normal de la misma página. Esto evita que
    # un callback de Streamlit intente desmontar el iframe grande del scanner
    # en el mismo rerun que abre/cierra autenticación.
    def _ts_salir():
        # Se conserva solo para compatibilidad con sesiones antiguas; el control
        # visible de SALIR usa un enlace normal y no este callback.
        st.session_state["_ts_logout_requested"] = True

    def _ts_cambiar_refresh():
        try:
            valor = max(5, int(st.session_state["ts_refresh_sel"]))
            st.session_state["_ts_refresh_canonico"] = valor
            st.query_params["refresh_sec"] = str(valor)
            _almacen = st.session_state.get("_ts_query_elegida")
            if isinstance(_almacen, dict):
                _almacen["refresh_sec"] = str(valor)
            else:
                st.session_state["_ts_query_elegida"] = {"refresh_sec": str(valor)}
            # Guardado servidor inmediato: no depende del JavaScript del iframe.
            _guardar_ultima_configuracion_servidor()
        except Exception:
            pass

    st.markdown(
        """
        <style>
        .st-key-ts_ctrl_bar [data-testid="stHorizontalBlock"]{flex-wrap:nowrap !important;justify-content:flex-end;align-items:center;gap:4px !important;}
        .st-key-ts_ctrl_bar [data-testid="stColumn"],.st-key-ts_ctrl_bar [data-testid="column"]{width:auto !important;min-width:0 !important;flex:0 0 auto !important;}
        .st-key-ts_ctrl_bar [data-testid="stSelectbox"]{width:150px;}
        .st-key-ts_ctrl_bar [data-testid="stCaptionContainer"]{max-width:190px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
        .st-key-ts_ctrl_bar button{white-space:nowrap;}
        /* Pantallas anchas: la barra se superpone dentro de la barra superior del scanner. */
        @media (min-width: 900px){
          .st-key-ts_ctrl_bar{height:0 !important;min-height:0 !important;overflow:visible !important;position:relative;z-index:60;}
          .st-key-ts_ctrl_bar > *{position:relative;top:24px;}
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    with st.container(key="ts_ctrl_bar"):
        if PUBLIC_PREVIEW:
            if st.button("📝 REGISTRO / INICIAR SESIÓN", key="ts_abrir_auth", width="stretch"):
                st.query_params["auth"] = "1"
                st.rerun()
        else:
            _n1, _n3, _n4 = st.columns([1.3, 1, 1])
            with _n1:
                st.caption(f"👤 {_email_top}" if _email_top else "👤 Administrador")
            with _n3:
                st.markdown(
                    f'<a href="{_safe_text(_ts_auth_href("abrir"))}" target="_top" '
                    'style="display:inline-block;padding:6px 10px;border:1px solid #555;border-radius:4px;'
                    'color:#fff;text-decoration:none;background:#20252c;font-size:11px;font-weight:700;">'
                    'CUENTA / REGISTRO</a>',
                    unsafe_allow_html=True,
                )
            with _n4:
                st.markdown(
                    '<a href="/?logout=1" target="_top" '
                    'style="display:inline-block;padding:6px 10px;border:1px solid #555;border-radius:4px;'
                    'color:#fff;text-decoration:none;background:#20252c;font-size:11px;font-weight:700;">'
                    'SALIR</a>',
                    unsafe_allow_html=True,
                )

    # La autenticación se resuelve mediante navegación normal (?auth=1).
    # Este bloque solo construye la página normal del scanner/robot.
    
    # Filtros nativos críticos: Precio y GAP.
    # Se dibujan como una capa compacta sobre la carátula para que sigan
    # perteneciendo visualmente al scanner, pero su estado vive en Streamlit
    # y no depende del iframe.
    def _ts_cambiar_filtro_precio():
        try:
            pmin = max(0.0, float(st.session_state["ts_f_price_min_native"]))
            pmax = max(pmin, float(st.session_state["ts_f_price_max_native"]))
            st.session_state["ts_f_price_min"] = pmin
            st.session_state["ts_f_price_max"] = pmax
            st.session_state["_ts_prev_ts_f_price_min"] = pmin
            st.session_state["_ts_prev_ts_f_price_max"] = pmax
            st.query_params["f_price_min"] = f"{pmin:g}"
            st.query_params["f_price_max"] = f"{pmax:g}"
            a = st.session_state.get("_ts_query_elegida")
            if not isinstance(a, dict):
                a = {}; st.session_state["_ts_query_elegida"] = a
            a["f_price_min"] = f"{pmin:g}"; a["f_price_max"] = f"{pmax:g}"
            _guardar_ultima_configuracion_servidor()
        except Exception:
            pass

    def _ts_cambiar_filtro_gap():
        try:
            gmin = float(st.session_state["ts_f_gap_min_native"])
            gmax = max(gmin, float(st.session_state["ts_f_gap_max_native"]))
            st.session_state["ts_f_gap_min"] = gmin
            st.session_state["ts_f_gap_max"] = gmax
            st.session_state["_ts_prev_ts_f_gap_min"] = gmin
            st.session_state["_ts_prev_ts_f_gap_max"] = gmax
            st.query_params["f_gap_min"] = f"{gmin:g}"
            st.query_params["f_gap_max"] = f"{gmax:g}"
            a = st.session_state.get("_ts_query_elegida")
            if not isinstance(a, dict):
                a = {}; st.session_state["_ts_query_elegida"] = a
            a["f_gap_min"] = f"{gmin:g}"; a["f_gap_max"] = f"{gmax:g}"
            _guardar_ultima_configuracion_servidor()
        except Exception:
            pass

    if _USAR_FILTROS_NATIVOS and not PUBLIC_PREVIEW:
        # Inicialización de los widgets nativos: SOLO una vez por sesión.
        # Después de que el usuario modifica un filtro, Streamlit conserva el
        # valor del widget y ningún rerun debe volver a copiar el valor canónico
        # anterior sobre la clave *_native.
        _pmin0 = _norm_nativo("flt", (0.0, 100000.0), st.session_state.get("ts_f_price_min", precio_min_ui))
        _pmax0 = _norm_nativo("flt", (0.0, 100000.0), st.session_state.get("ts_f_price_max", precio_max_ui))
        _gmin0 = _norm_nativo("flt", (-100.0, 10000.0), st.session_state.get("ts_f_gap_min", gap_min_ui))
        _gmax0 = _norm_nativo("flt", (-100.0, 10000.0), st.session_state.get("ts_f_gap_max", gap_max_ui))
        for _k_nat, _v_nat, _v_def in (
            ("ts_f_price_min_native", _pmin0, precio_min_ui),
            ("ts_f_price_max_native", _pmax0, precio_max_ui),
            ("ts_f_gap_min_native", _gmin0, gap_min_ui),
            ("ts_f_gap_max_native", _gmax0, gap_max_ui),
        ):
            if _k_nat not in st.session_state:
                _obj = float(_v_nat if _v_nat is not None else _v_def)
                st.session_state[_k_nat] = _obj

    # Puente nativo: el iframe no puede navegar la página superior (Streamlit no
    # da allow-top-navigation). En su lugar el JS del iframe actualiza la URL del
    # padre con history.replaceState y pulsa este botón oculto, lo que provoca un
    # rerun nativo de la MISMA sesión leyendo los nuevos query params.
    st.markdown(
        "<style>.st-key-ts_nav_bridge{display:none !important;}</style>",
        unsafe_allow_html=True,
    )
    st.button("TSNAVBRIDGE", key="ts_nav_bridge")

    # Todo el scanner se renderiza en un único iframe.
    # st.iframe es el reemplazo actual de components.v1.html y conserva
    # HTML/JavaScript inline con acceso same-origin, que este puente necesita.
    # Anti-parpadeo: si lo unico que cambio es el reloj o el "ultimo escaneo", se reutiliza el
    # mismo HTML y el iframe NO se recarga. Si cambian filtros o resultados, se actualiza normal.
    try:
        import re as _re_ifr
        _h_key = h
        for _vol in (str(fecha_hora_actual), str(_ultima_scan_txt)):
            if _vol:
                _h_key = _h_key.replace(_vol, "")
        _h_key = _re_ifr.sub(r'"_(?:u|ts)":\s*"\d+"', "", _h_key)
        # No invalidar el iframe por el paso del minuto: el reloj y el último
        # escaneo ya se excluyeron arriba. Solo un cambio real del contenido
        # debe reconstruir la carátula y su DOM/JavaScript.
        _clave_ifr = hashlib.md5(_h_key.encode("utf-8", "ignore")).hexdigest()
        if st.session_state.get("_ts_iframe_clave") == _clave_ifr and st.session_state.get("_ts_iframe_html"):
            h = st.session_state["_ts_iframe_html"]
        else:
            st.session_state["_ts_iframe_clave"] = _clave_ifr
            st.session_state["_ts_iframe_html"] = h
    except Exception:
        pass
    # El componente devuelve al servidor la configuración que el usuario acaba
    # de cambiar. Ese retorno es la fuente de verdad de la interacción: se
    # normaliza, se guarda en Session State + query params + persistencia por
    # usuario, y solo después se repinta ESTE fragmento. Así el auto-refresh
    # nunca puede resucitar una configuración anterior.
    if _TS_COMP_OK and _ts_scanner_ui is not None:
        _ts_nav_result = _ts_scanner_ui(html=h, alto=900, key="ts_scanner_ui", default=None)
        try:
            if isinstance(_ts_nav_result, dict):
                _ts_q_raw = str(_ts_nav_result.get("q", "") or "")
                if _ts_q_raw:
                    from urllib.parse import parse_qsl
                    _ts_q_new = dict(parse_qsl(_ts_q_raw, keep_blank_values=True))
                    _ts_cambios = {}
                    for _ts_k in _CONFIG_USUARIO_KEYS:
                        if _ts_k in _ts_q_new and str(_ts_q_new[_ts_k]) != "":
                            _ts_cambios[_ts_k] = str(_ts_q_new[_ts_k])
                    if _ts_cambios:
                        # Mantener siempre la sesión de autenticación actual.
                        _ts_auth_sid = str(st.query_params.get("auth_session", "") or "").strip()
                        if _ts_auth_sid:
                            _ts_cambios["auth_session"] = _ts_auth_sid
                        _ts_prev_cfg = st.session_state.get("_ts_query_elegida")
                        if not isinstance(_ts_prev_cfg, dict):
                            _ts_prev_cfg = {}
                        _ts_changed = any(
                            str(_ts_prev_cfg.get(_k, "")) != str(_ts_v)
                            for _ts_k, _ts_v in _ts_cambios.items()
                            for _ts_k in [_ts_k]
                        )
                        # Comparar también contra lo que realmente está en la URL.
                        _ts_changed = _ts_changed or any(
                            str(_qp_valor(_ts_k) or "") != str(_ts_v)
                            for _ts_k, _ts_v in _ts_cambios.items()
                            if _ts_k != "auth_session"
                        )
                        if _ts_changed:
                            _ts_prev_cfg.update(_ts_cambios)
                            st.session_state["_ts_query_elegida"] = _ts_prev_cfg
                            for _ts_k, _ts_v in _ts_cambios.items():
                                if _ts_k != "auth_session":
                                    st.query_params[_ts_k] = str(_ts_v)
                            try:
                                _guardar_ultima_configuracion_servidor()
                            except Exception:
                                pass
                            # El evento llega después de construir el HTML de esta ejecución.
                            # Forzamos UN solo rerun para que el cuadro se reconstruya
                            # usando inmediatamente el valor recién confirmado.
                            # No hay navegación ni segundo rerun en cadena.
                            st.rerun()
        except Exception as _e_ts_nav:
            print(f"⚠️ No se pudo consolidar la configuración del scanner: {_e_ts_nav}")
    else:
        # Render estable del scanner: h es HTML, no una URL. st.iframe(h) lo
        # interpreta como src y puede dejar la pantalla completamente blanca.
        try:
            import streamlit.components.v1 as _stc_fb
            _stc_fb.html(h, height=900, scrolling=True)
        except Exception as _e_ifr:
            st.error(f"No se pudo dibujar el scanner: {_e_ifr}")

    # Panel de diagnostico: cuantas acciones sobreviven en cada paso del embudo.
    # Sirve para probar pestana por pestana si un filtro realmente influye en el escaneo.
    try:
        _dg = dict(getattr(servicio, "diagnostico_filtros", {}) or {})
        with st.expander("🔎 Diagnóstico del escaneo (embudo)", expanded=False):
            if not _dg:
                st.caption("Aún no hay un ciclo de escaneo completado.")
            else:
                _etiquetas = [
                    ("radar_base", "1. Acciones en el radar base (precio/volumen del mercado)"),
                    ("enviados_tecnico", "2. Enviadas a análisis técnico"),
                    ("con_40_barras", "3. Con suficientes velas (40+) para calcular"),
                    ("ema_calculable", "4. EMA calculable"),
                    ("macd_calculable", "5. MACD calculable"),
                    ("tras_float", "6. Después del filtro de flotación"),
                    ("float_sin_dato", "   · descartadas por flotación sin dato"),
                    ("float_excede", "   · descartadas por flotación mayor al máximo"),
                    ("tras_gap_volumen", "7. Después de gap / volumen relativo"),
                    ("ema_arriba", "8. Con EMA en la condición pedida"),
                    ("macd_positivo", "9. Con MACD positivo"),
                    ("ema_y_macd", "10. Cumplen EMA y MACD a la vez"),
                    ("resultados", "RESULTADO FINAL (lo que ves en la tabla)"),
                ]
                _filas = [{"Paso": _t, "Cantidad": _dg.get(_k, "—")} for _k, _t in _etiquetas if _k in _dg]
                st.table(_filas)
                st.caption(
                    f"Temporalidad: {_dg.get('timeframe', '—')} · Sesión: {_dg.get('sesion', '—')} · "
                    f"Gap aplicado: {_dg.get('gap_min', '—')}% a {_dg.get('gap_max', '—')}%"
                )
                try:
                    _cf = list(getattr(servicio, "cache_fund", {}).items())
                    _meta_f = dict(getattr(servicio, "cache_fund", {}).get("__bulk_meta__", {}) or {})
                    _con_float = sum(1 for _k, _v in _cf if not str(_k).startswith("__") and isinstance(_v, dict) and _v.get("float") is not None)
                    _hace = (time.time() - float(_meta_f.get("ts", 0))) / 60 if _meta_f.get("ts") else None
                    _pausa = float(getattr(servicio, "fmp_pausado_hasta", 0) or 0) - time.time()
                    st.caption(
                        f"FMP (flotación): clave configurada: {'sí' if getattr(servicio, 'fmp_api_key', None) else 'NO'} · "
                        f"tickers con flotación en caché: {_con_float} · "
                        f"última carga masiva: {'nunca' if _hace is None else f'hace {_hace:.0f} min'} "
                        f"(páginas: {_meta_f.get('paginas', '—')}, encontrados: {_meta_f.get('encontrados', '—')}) · "
                        f"pausa por límite: {'sí, ' + str(int(_pausa)) + ' s' if _pausa > 0 else 'no'}"
                    )
                    _fuentes = {}
                    _solo_circ = 0
                    for _k, _v in _cf:
                        if str(_k).startswith("__") or not isinstance(_v, dict):
                            continue
                        if _v.get("float") is not None:
                            _s = str(_v.get("float_source", "FMP"))
                            _fuentes[_s] = _fuentes.get(_s, 0) + 1
                        elif _v.get("outstanding") is not None:
                            _solo_circ += 1
                    st.caption(
                        "Flotación por fuente: "
                        + (" · ".join(f"{k}: {n}" for k, n in sorted(_fuentes.items())) or "ninguna aún")
                        + f" · solo acciones en circulación (Finnhub): {_solo_circ}"
                        + f" · Yahoo: {getattr(servicio, '_yahoo_estado', 'sin usar')}"
                        + f" · Finnhub: {'clave sí' if getattr(servicio, 'finnhub_api_key', None) else 'SIN clave'}"
                        + (f" ({getattr(servicio, '_finnhub_estado', '')})" if getattr(servicio, '_finnhub_estado', '') else "")
                    )
                except Exception as _e_fmp:
                    st.caption(f"FMP: estado no disponible ({_e_fmp})")
                try:
                    _mv = servicio.estado_motor_velas()
                    _mv_symbols = _mv.get("simbolos_cargados", []) or []
                    _mv_last = _mv.get("ultimo_trade")
                    st.caption(
                        "Motor velas: "
                        + ("🟢 conectado" if _mv.get("stream_hilo_vivo") and _mv.get("stream_iniciado") else "🔴 sin conexión")
                        + f" · símbolos: {len(_mv_symbols)}/{_mv.get('limite_simbolos', 30)}"
                        + f" · trades: {_mv.get('total_trades', 0)}"
                        + (f" · último: {_mv_last}" if _mv_last else "")
                    )
                    if _mv.get("error"):
                        st.caption(f"Motor velas — último error: {_mv.get('error')}")
                except Exception:
                    pass
                _err = str(getattr(servicio, "ultimo_error", "") or "").strip()
                if _err:
                    st.warning(f"Último error del motor: {_err}")
    except Exception as _e_dg:
        print(f"⚠️ Panel de diagnóstico no disponible: {_e_dg}")


# El temporizador se mantiene FUERA del iframe.
# No navega el navegador ni modifica window.location desde el iframe.
# IMPORTANTE: refresh_sec es local a _render_scanner(), por lo que aquí no se
# puede referenciar directamente. Lo volvemos a leer de query_params de forma
# segura para que el decorador de st.fragment reciba el valor correcto.
def _tf_pendiente():
    """True mientras el motor todavía no calculó la temporalidad pedida (máx. ~20 intentos)."""
    try:
        if PUBLIC_PREVIEW or not getattr(servicio, "encendido", True):
            st.session_state["_tf_pend_n"] = 0
            return False
        valor = st.query_params.get("timeframe", "1m")
        if isinstance(valor, list):
            valor = valor[0] if valor else "1m"
        tf = str(valor).lower()
        if tf not in ("1m", "3m", "5m", "10m", "13m", "15m", "30m", "1h", "1d", "1w", "1mo"):
            tf = "1m"
        rp = getattr(servicio, "resultados_por_tf", None)
        pendiente = isinstance(rp, dict) and tf not in rp
        n = int(st.session_state.get("_tf_pend_n", 0))
        if pendiente and n < 20:
            st.session_state["_tf_pend_n"] = n + 1
            return True
        if not pendiente:
            st.session_state["_tf_pend_n"] = 0
        return False
    except Exception:
        return False


def _refresh_segundos_global():
    # Visitante: 3 minutos fijos. Usuario autenticado: conserva el refresh
    # elegido por el usuario aunque Streamlit haga un rerun completo.
    if PUBLIC_PREVIEW:
        return 180
    if _tf_pendiente():
        return 4
    try:
        canonico = st.session_state.get("_ts_refresh_canonico")
        if canonico is not None:
            return max(5, int(float(str(canonico))))
        valor = st.query_params.get("refresh_sec", "180")
        if isinstance(valor, list):
            valor = valor[0] if valor else "180"
        return max(5, int(float(str(valor))))
    except Exception:
        return 180

_CLAVES_SYNC_QUERY = tuple(_CONFIG_USUARIO_KEYS) + ("technical_timeframe",)


def _qp_valor(k):
    v = st.query_params.get(k, None)
    if isinstance(v, list):
        v = v[0] if v else None
    return None if v is None else str(v)


_TF_VALIDOS = ("1m", "3m", "5m", "10m", "13m", "15m", "30m", "1h", "1d", "1w", "1mo")

# Controles NATIVOS: (param URL, key del widget, tipo, opciones/rango, defecto)
#   tipos: sel (opciones) · num (0-25) · flt / int (rango min,max) · txt (largo máx.)
_EMA_NATIVOS = []
for _n, _cd in ((20, "Naciendo"), (50, "Ninguna"), (200, "Ninguna")):
    _EMA_NATIVOS += [
        (f"ema{_n}_estado", f"ts_ema{_n}_estado", "sel", ("Por encima", "Por debajo", "Neutro"), "Neutro"),
        (f"ema{_n}_cond", f"ts_ema{_n}_cond", "sel", OPCIONES_COND_EMA, _cd),
        (f"ema{_n}_dist", f"ts_ema{_n}_dist", "num", None, 0.5),
    ]
_FILTROS_NATIVOS = [
    ("ema20_on", "ts_ema20_on", "sel", ("OFF", "ON"), "OFF"),
    ("c_active", "ts_c_active", "sel", ("True", "False"), lambda: "True" if getattr(servicio, "encendido", True) else "False"),
    ("c_lang", "ts_c_lang", "sel", ("ESP", "ENG", "POR", "FRA", "DEU", "ITA", "CHN", "JPN"), "ESP"),
    ("c_wnd", "ts_c_wnd", "sel", ("Incrustada", "Flotante"), "Incrustada"),
    ("f_price_min", "ts_f_price_min", "flt", (0.0, 100000.0), 0.50),
    ("f_price_max", "ts_f_price_max", "flt", (0.0, 100000.0), 20.0),
    ("f_gap_min", "ts_f_gap_min", "flt", (-100.0, 10000.0), 3.0),
    ("f_gap_max", "ts_f_gap_max", "flt", (-100.0, 10000.0), 50.0),
    ("f_gap_on", "ts_f_gap_on", "sel", ("OFF", "ON"), "OFF"),
    ("f_float_on", "ts_f_float_on", "sel", ("OFF", "ON"), "OFF"),
    ("f_float_max", "ts_f_float_max", "int", (0, 10 ** 12), 20_000_000),
    ("f_vol", "ts_f_vol", "int", (0, 10 ** 12), 15_000),
    ("f_vol_on", "ts_f_vol_on", "sel", ("OFF", "ON"), "OFF"),
    ("f_mac", "ts_f_mac", "sel", ("Positivo", "Negativo", "No exigir"), "Positivo"),
    ("f_order", "ts_f_order", "sel", ("Actualizado", "Cambio %", "Volumen"), "Actualizado"),
    ("c_broker", "ts_c_broker", "sel", ("Interactive Brokers", "Tradestation", "Charles Schwab", "Otro"),
     lambda: st.session_state.get("bk_nombre", "Interactive Brokers")),
    ("c_url", "ts_c_url", "txt", 300, lambda: st.session_state.get("bk_puente", "http://localhost:8080/layout")),
]
_NATIVOS_TODOS = _EMA_NATIVOS + _FILTROS_NATIVOS
_CLAVES_NATIVAS = {"timeframe"} | {_e[0] for _e in _NATIVOS_TODOS}


def _norm_nativo(tipo, opciones, v):
    if v is None:
        return None
    if tipo == "sel":
        return v if v in opciones else None
    if tipo == "txt":
        t = str(v).strip()
        return t[: int(opciones or 300)] if t else None
    try:
        x = float(v)
    except Exception:
        return None
    if tipo == "num":
        mn, mx = 0.0, 25.0
    else:
        mn, mx = opciones
    x = max(mn, min(mx, x))
    if tipo == "int":
        return int(x)
    return round(x, 4 if tipo == "flt" else 2)


def _valor_defecto_nativo(tipo, opciones, defecto):
    d = defecto() if callable(defecto) else defecto
    val = _norm_nativo(tipo, opciones, d)
    if val is None:
        val = opciones[0] if tipo == "sel" else (0 if tipo == "int" else (0.0 if tipo in ("num", "flt") else ""))
    return val


def _sincronizar_nativos(accion_js):
    """El widget nativo manda; la URL solo se acepta al cargar por primera vez o al
    cargar una configuración guardada. Así el refresh nunca revierte lo elegido."""
    almacen = st.session_state.get("_ts_query_elegida")
    _qp_updates = {}
    for qk, wk, tipo, ops, defecto in _NATIVOS_TODOS:
        w = _norm_nativo(tipo, ops, st.session_state.get(wk))
        previo = st.session_state.get("_ts_prev_" + wk)
        u = _norm_nativo(tipo, ops, _qp_valor(qk))
        if accion_js and u is not None:
            # Cambio recién hecho por el usuario: la URL recién escrita manda.
            val = u
        elif w is not None and w != previo:
            # Cambio directo del widget nativo: capturarlo y convertirlo en estado canónico.
            val = w
        elif isinstance(almacen, dict) and qk in almacen:
            # AUTO-REFRESH: conservar el último valor confirmado por esta sesión.
            # La URL puede contener una copia antigua; no debe ganar al estado canónico.
            val = _norm_nativo(tipo, ops, almacen.get(qk))
            if val is None:
                val = u if u is not None else (previo if previo is not None else w)
        elif previo is not None:
            val = previo
        elif u is not None:
            val = u
        elif w is not None:
            val = w
        else:
            val = _valor_defecto_nativo(tipo, ops, defecto)
        st.session_state["_ts_prev_" + wk] = val
        if tipo in ("num", "flt"):
            val = float(val)
            txt = f"{val:g}"
        elif tipo == "int":
            val = int(val)
            txt = str(val)
        else:
            txt = str(val)
        if st.session_state.get(wk) != val:
            st.session_state[wk] = val
        if _qp_valor(qk) != txt:
            _qp_updates[qk] = txt
        if isinstance(almacen, dict):
            almacen[qk] = txt
    if _qp_updates:
        try:
            st.query_params.update(_qp_updates)
        except Exception:
            for _k, _v in _qp_updates.items():
                st.query_params[_k] = _v


def _sincronizar_timeframe(tf_url, accion_js):
    """La temporalidad la decide el selector nativo de Streamlit (key ts_tf_sel).

    Un widget nativo vive en st.session_state y NO depende de la URL ni del puente
    del iframe, por eso ya no rebota a 1m. La URL solo se usa al cargar por primera
    vez o cuando el usuario carga una configuración guardada.
    """
    widget = st.session_state.get("ts_tf_sel")
    previo = st.session_state.get("_ts_tf_elegido")
    almacen = st.session_state.get("_ts_query_elegida")
    if accion_js and tf_url in _TF_VALIDOS:
        tf = tf_url
    elif widget in _TF_VALIDOS and widget != previo:
        tf = widget
    elif isinstance(almacen, dict) and almacen.get("timeframe") in _TF_VALIDOS:
        tf = almacen.get("timeframe")
    elif previo in _TF_VALIDOS:
        tf = previo
    elif tf_url in _TF_VALIDOS:
        tf = tf_url
    elif widget in _TF_VALIDOS:
        tf = widget
    else:
        tf = "1m"
    st.session_state["_ts_tf_elegido"] = tf
    if st.session_state.get("ts_tf_sel") != tf:
        st.session_state["ts_tf_sel"] = tf
    if _qp_valor("timeframe") != tf:
        try:
            st.query_params.update({"timeframe": tf})
        except Exception:
            st.query_params["timeframe"] = tf
    almacen = st.session_state.get("_ts_query_elegida")
    if isinstance(almacen, dict):
        almacen["timeframe"] = tf


def _sincronizar_query_con_sesion():
    """La temporalidad y los filtros que elige el usuario NO se pierden con el refresh.

    Toda acción del usuario en la carátula (JS) añade a la URL una marca `_u`
    creciente. `_u` nueva = acción del usuario (se guarda lo de la URL); cualquier
    otro rerun (refresh, reconexión, URL vieja) = se re-impone lo último elegido.
    La temporalidad se maneja aparte (_sincronizar_timeframe).
    """
    try:
        auto = bool(st.session_state.pop("_ts_rerun_auto", False))
        tf_url = _qp_valor("timeframe")
        guardado = st.session_state.get("_ts_query_elegida")
        if not isinstance(guardado, dict):
            guardado = {}
        try:
            u_nuevo = int(float(_qp_valor("_u") or 0))
        except Exception:
            u_nuevo = 0
        u_visto = int(st.session_state.get("_ts_u_visto", 0) or 0)
        accion_js = (not auto) and u_nuevo > u_visto

        # Estado canónico por sesión:
        # - una acción del usuario captura la URL nueva;
        # - cualquier auto-refresh reutiliza ese estado, sin leer una URL antigua.
        if accion_js:
            nuevo = {}
            for k in _CLAVES_SYNC_QUERY:
                v = _qp_valor(k)
                if v is not None and v != "":
                    nuevo[k] = v
            if nuevo:
                st.session_state["_ts_query_elegida"] = nuevo
        elif guardado:
            # No dejamos que el auto-refresh sustituya los valores elegidos.
            # Preparar todo antes de tocar la URL evita una cascada de reruns
            # durante la reconstrucción de una sesión después de un refresh.
            _qp_restaurar = {}
            for k, v in guardado.items():
                if k in ("timeframe", "refresh_sec"):
                    continue
                if _qp_valor(k) != v:
                    _qp_restaurar[k] = v
            if guardado.get("timeframe") and _qp_valor("timeframe") != guardado.get("timeframe"):
                _qp_restaurar["timeframe"] = guardado["timeframe"]
            if _qp_restaurar:
                try:
                    st.query_params.update(_qp_restaurar)
                except Exception:
                    for _k, _v in _qp_restaurar.items():
                        st.query_params[_k] = _v
        else:
            # Primera carga sin estado previo: tomar la URL existente.
            nuevo = {}
            for k in _CLAVES_SYNC_QUERY:
                v = _qp_valor(k)
                if v is not None and v != "":
                    nuevo[k] = v
            if nuevo:
                st.session_state["_ts_query_elegida"] = nuevo
        # REFRESH: un refresh_sec explícito en la URL representa la selección
        # actual. Debe pasar al estado canónico antes de cualquier auto-refresh.
        try:
            _rv_q = _qp_valor("refresh_sec")
            _rv_guardado = guardado.get("refresh_sec") if isinstance(guardado, dict) else None
            _rv_canon = st.session_state.get("_ts_refresh_canonico")
            if not PUBLIC_PREVIEW:
                if _rv_q not in (None, ""):
                    _rv_num = max(5, int(float(_rv_q)))
                    # Si la URL contiene un valor válido, es el estado que
                    # acaba de llegar del selector y debe adoptarse como
                    # canónico, incluso si la sesión conserva un valor viejo.
                    st.session_state["_ts_refresh_canonico"] = _rv_num
                    if isinstance(st.session_state.get("_ts_query_elegida"), dict):
                        st.session_state["_ts_query_elegida"]["refresh_sec"] = str(_rv_num)
                elif _rv_canon not in (None, ""):
                    st.query_params["refresh_sec"] = str(max(5, int(float(_rv_canon))))
                elif _rv_guardado not in (None, ""):
                    _rv_num = max(5, int(float(_rv_guardado)))
                    st.session_state["_ts_refresh_canonico"] = _rv_num
                    st.query_params["refresh_sec"] = str(_rv_num)
        except Exception:
            pass
        st.session_state["_ts_u_visto"] = max(u_visto, u_nuevo)
        _sincronizar_timeframe(tf_url, accion_js)
        _sincronizar_nativos(accion_js)
    except Exception:
        pass


_sincronizar_query_con_sesion()
_ts_diag_add(
    "PYTHON: DESPUÉS DEL SINCRONIZADOR",
    "Así quedó el estado después de _sincronizar_query_con_sesion()",
    _ts_diag_snapshot()
)

# ESTABILIZACIÓN: el scanner usa un componente iframe (Components V1).
# No lo ejecutamos dentro de st.fragment/run_every: en esta arquitectura el
# ciclo fragmentado puede desmontar/recrear el iframe mientras el componente
# está enviando navegación a Python y dejar la página en blanco.
# El motor de mercado sigue trabajando en segundo plano; la actualización de
# configuración se produce por setComponentValue y el usuario puede refrescar
# la vista sin reconstruir un iframe activo dentro de un fragmento.


def _render_robot_long_page(servicio):
    _robot_q = {str(k): str(v) for k, v in st.query_params.items() if str(k) != "robot"}
    _robot_href = "/?" + "&".join(f"{quote(k)}={quote(v)}" for k, v in _robot_q.items()) if _robot_q else "/"
    st.markdown(f'<a href="{_robot_href}" target="_top" style="display:inline-block;margin:4px 0 12px 0;font-weight:700;text-decoration:none;">← VOLVER AL SCANNER</a>', unsafe_allow_html=True)
    # ================================================================
    # 🤖 PANEL VISIBLE DEL ROBOT LONG
    # El robot trabaja en segundo plano cada ~1 s, independiente del
    # refresh visual del scanner. Esta sección permite verlo en la misma
    # página y comprobar qué está haciendo sin exponer claves.
    # ================================================================
    try:
        _rb = servicio.estado_bot_long()
        st.markdown("### 🤖 Robot LONG — tiempo real")
        if _rb.get("hilo_vivo"):
            st.success("🟢 Robot activo y evaluando continuamente")
        elif _rb.get("disponible"):
            st.warning("🟡 Robot cargado, pero su hilo no está activo")
        else:
            st.error("🔴 Robot no disponible")
            if _rb.get("error"):
                st.caption(f"Error del robot: {_rb.get('error')}")

        # Tarjetas de estado del robot: alto contraste y lectura clara en PC/móvil.
        # Solo cambia la presentación; no modifica ninguna variable del motor.
        _ciclos = _rb.get("ciclos", 0)
        _candidatos = len(_rb.get("candidatos", []) or [])
        _posiciones = len(_rb.get("posiciones_paper", []) or [])
        _observadas = len(_rb.get("posiciones_observadas", []) or [])
        _decisiones = _rb.get("decisiones_guardadas", 0)
        st.markdown("""
        <style>
        .ts-robot-panel{background:#1b2027;border:1px solid #3b4652;border-radius:12px;padding:14px 14px 10px;margin:4px 0 14px 0;box-shadow:0 2px 10px rgba(0,0,0,.18)}
        .ts-robot-title{font-size:1.15rem;font-weight:700;color:#f2f5f8;margin-bottom:12px}
        .ts-robot-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px}
        .ts-robot-card{background:#252c35;border:1px solid #4a5663;border-radius:9px;padding:10px 8px;text-align:center;min-height:68px}
        .ts-robot-label{font-size:.76rem;color:#b9c3ce;text-transform:uppercase;letter-spacing:.03em}
        .ts-robot-value{font-size:1.45rem;font-weight:800;color:#ffffff;line-height:1.35;margin-top:2px}
        @media(max-width:800px){.ts-robot-grid{grid-template-columns:repeat(3,minmax(0,1fr));}}
        @media(max-width:520px){.ts-robot-grid{grid-template-columns:repeat(2,minmax(0,1fr));}.ts-robot-value{font-size:1.25rem;}}
        </style>
        <div class="ts-robot-panel">
          <div class="ts-robot-title">📊 Estado del robot</div>
          <div class="ts-robot-grid">
            <div class="ts-robot-card"><div class="ts-robot-label">Ciclos</div><div class="ts-robot-value">""" + str(_ciclos) + """</div></div>
            <div class="ts-robot-card"><div class="ts-robot-label">Candidatos</div><div class="ts-robot-value">""" + str(_candidatos) + """</div></div>
            <div class="ts-robot-card"><div class="ts-robot-label">Posiciones</div><div class="ts-robot-value">""" + str(_posiciones) + """</div></div>
            <div class="ts-robot-card"><div class="ts-robot-label">Observadas</div><div class="ts-robot-value">""" + str(_observadas) + """</div></div>
            <div class="ts-robot-card"><div class="ts-robot-label">Decisiones</div><div class="ts-robot-value">""" + str(_decisiones) + """</div></div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        _dec = servicio.decisiones_bot_long(20)
        if _dec:
            _filas_bot = []
            for _d in reversed(_dec):
                _filas_bot.append({
                    "Hora": datetime.fromtimestamp(float(_d.get("ts", time.time())), tz=ET).strftime("%H:%M:%S"),
                    "Ticker": _d.get("simbolo", ""),
                    "Acción": _d.get("accion", ""),
                    "Estado": _d.get("estado", ""),
                    "Motivo": _d.get("motivo", ""),
                    "Precio": _d.get("precio", ""),
                    "Stop": _d.get("stop_loss", ""),
                })
            st.dataframe(pd.DataFrame(_filas_bot), use_container_width=True, hide_index=True)
        else:
            st.info("El robot está encendido pero todavía no tiene decisiones para mostrar.")
    except Exception as _e_robot_ui:
        st.warning(f"Panel del robot temporalmente no disponible: {_e_robot_ui}")


# URL independiente del Robot LONG: mantiene su interfaz separada del Scanner.
_ROBOT_MODE = str(st.query_params.get("robot", "") or "").lower() in ("1", "true", "yes")
if _ROBOT_MODE:
    _render_robot_long_page(servicio)
    st.stop()

_render_scanner()
