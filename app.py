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

# Configuración base obligatoria de la página
st.set_page_config(page_title="Scanner Pre Market", layout="wide")

# ==========================================
# 🙈 OCULTAR BARRA SUPERIOR DE STREAMLIT Y ESTILOS
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
        .block-container { padding-left: 0.6rem !important; padding-right: 0.6rem !important; padding-top: 1rem !important; }
        h1 { font-size: 1.3rem !important; }
        h2 { font-size: 1.1rem !important; }
        h3 { font-size: 1rem !important; }
        [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
        [data-testid="stDataFrame"] { overflow-x: auto !important; }
    }
    .block-container { max-width: 100% !important; width: 100% !important; padding-left: 0.35rem !important; padding-right: 0.35rem !important; }
    [data-testid="stIFrame"], [data-testid="stIFrame"] > iframe { width: 100% !important; max-width: 100% !important; }
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
# 🔐 AUTENTICACIÓN — SUPABASE REST API
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
