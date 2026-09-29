import os
import json
import time
import hashlib
import hmac
import secrets
from urllib.parse import quote
from html import escape as html_escape
import threading
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

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
    .block-container {
        max-width: 100% !important;
        width: 100% !important;
        padding-left: 0.35rem !important;
        padding-right: 0.35rem !important;
    }
    [data-testid="stIFrame"], [data-testid="stIFrame"] > iframe {
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
OPCIONES_CRUCE_EMA = ["Vela nueva sobre EMA20 + HH/HL", "Hacia abajo", "Neutro"]
OPCIONES_MACD = ["Positivo", "Negativo", "No exigir"]

VALORES_POR_DEFECTO = {
    "precio_min": 0.5, "precio_max": 20.0,
    "gap_min": 3.0, "gap_max": 50.0,
    "flotacion_max": 20_000_000, "volumen_min": 15_000,
    "cruce_ema": "Vela nueva sobre EMA20 + HH/HL", "macd": "Positivo",
    "orden": "Actualizado", "top_n": 50, "sesion": "PRE-MARKET", "timeframe": "1m"
}

def cargar_config():
    return VALORES_POR_DEFECTO.copy()

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

def crear_prueba_usuario(user_id, email):
    if not user_id: return None
    data = _leer_licencias_simuladas()
    clave = str(user_id)
    if clave in data: return data[clave]
    inicio = datetime.now(timezone.utc)
    licencia = {
        "user_id": clave,
        "email": str(email or "").strip().lower(),
        "plan": "PRUEBA GRATIS",
        "estado": "ACTIVO",
        "inicio": inicio.isoformat(),
        "vencimiento": (inicio + timedelta(days=DIAS_PRUEBA_GRATIS)).isoformat()
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
    if licencia.get("estado") == "SUSPENDIDO": return "SUSPENDIDO", None
    try:
        venc = datetime.fromisoformat(str(licencia.get("vencimiento")).replace("Z", "+00:00"))
        if datetime.now(timezone.utc) <= venc:
            return "ACTIVO", venc
    except Exception:
        pass
    return "VENCIDO", None

def activar_plan_simulado(user_id, plan):
    data = _leer_licencias_simuladas()
    clave = str(user_id)
    actual = data.get(clave) or {"user_id": clave}
    inicio = datetime.now(timezone.utc)
    dias = 30 if plan == "MENSUAL" else 365
    actual.update({
        "plan": plan,
        "estado": "ACTIVO",
        "inicio": inicio.isoformat(),
        "vencimiento": (inicio + timedelta(days=dias)).isoformat()
    })
    data[clave] = actual
    if _guardar_licencias_simuladas(data):
        return True, "Plan simulado activado."
    return False, "Error de escritura."

# ==========================================
# 🔐 AUTENTICACIÓN — REST API SUPABASE
# ==========================================
def _supabase_config():
    url = str(st.secrets.get("SUPABASE_URL", "")).strip().rstrip("/")
    key = str(st.secrets.get("SUPABASE_ANON_KEY", "")).strip()
    return url, key

def supabase_auth_request(endpoint, payload):
    url, key = _supabase_config()
    if not url or not key:
        return None, "Faltan credenciales de Supabase en Secrets."
    try:
        respuesta = requests.post(
            f"{url}/auth/v1/{endpoint}",
            headers={"apikey": key, "Content-Type": "application/json"},
            json=payload,
            timeout=20,
        )
        data = respuesta.json() if respuesta.status_code == 200 else {}
        if respuesta.ok: return data, None
        return None, f"Error REST HTTP {respuesta.status_code}"
    except Exception as e:
        return None, str(e)

def registrar_usuario(email, password):
    return supabase_auth_request("signup", {"email": email, "password": password})

def iniciar_sesion_usuario(email, password):
    return supabase_auth_request("token?grant_type=password", {"email": email, "password": password})

@st.cache_resource
def _almacen_sesiones_persistentes(): 
    return {}
_PERSISTENT_AUTH_SESSIONS = _almacen_sesiones_persistentes()

def _admin_tokens_para_sesion():
    candidatos = []
    try:
        t = str(st.secrets.get("ADMIN_TOKEN", "")).strip()
        if t: candidatos.append(t)
    except Exception:
        pass
    return candidatos

def _crear_sesion_persistente(tipo, datos):
    sid = secrets.token_urlsafe(32)
    _PERSISTENT_AUTH_SESSIONS[sid] = {"tipo": tipo, "datos": dict(datos or {})}
    return sid

def _restaurar_sesion_persistente():
    try:
        sid = str(st.query_params.get("auth_session", "")).strip()
        if not sid: return False
        ses = _PERSISTENT_AUTH_SESSIONS.get(sid)
        if not ses: return False
        if ses.get("tipo") == "admin":
            st.session_state["token_verificado"] = ses["datos"].get("token")
            st.session_state["tipo_acceso"] = "admin"
        else:
            st.session_state["usuario_auth"] = ses["datos"]
            st.session_state["tipo_acceso"] = "usuario"
        return True
    except Exception:
        return False

def cerrar_sesion():
    try:
        sid = str(st.query_params.get("auth_session", "")).strip()
        if sid: _PERSISTENT_AUTH_SESSIONS.pop(sid, None)
    except Exception:
        pass
    for k in ["usuario_auth", "token_verificado", "tipo_acceso", "mostrar_auth"]:
        st.session_state.pop(k, None)

def _guardar_usuario_auth(data):
    usuario = data.get("user") or {}
    st.session_state["usuario_auth"] = {
        "user_id": usuario.get("id", ""),
        "email": usuario.get("email", ""),
        "access_token": data.get("access_token", ""),
    }
    st.session_state["tipo_acceso"] = "usuario"

# ==========================================
# 🎨 CARÁTULA NATIVA DE AUTENTICACIÓN
# ==========================================
def pantalla_autenticacion():
    st.markdown("""
    <style>
    .stApp { background: #030303 !important; }
    .auth-card { max-width: 480px; margin: 40px auto; background: #0d1118; border: 1px solid #2a3348; border-radius: 12px; padding: 20px; text-align: center; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="auth-card"><div style="color:#d4af37; font-weight:800; font-size:22px;">TRADE SCANNER INSTITUTIONAL</div></div>', unsafe_allow_html=True)
    tab_l, tab_r, tab_a = st.tabs(["🔐 Login", "📝 Registro", "👑 Admin"])
    
    with tab_l:
        with st.form("form_l"):
            em = st.text_input("Correo")
            pw = st.text_input("Contraseña", type="password")
            btn = st.form_submit_button("ACCEDER")
        if btn:
            d, err = iniciar_sesion_usuario(em, pw)
            if err: st.error(err)
            else:
                _guardar_usuario_auth(d)
                st.session_state["mostrar_auth"] = False
                st.query_params["auth_session"] = _crear_sesion_persistente("usuario", d)
                st.rerun()

    with tab_r:
        with st.form("form_r"):
            em = st.text_input("Nuevo Correo")
            pw = st.text_input("Contraseña (Min 8)", type="password")
            btn = st.form_submit_button("REGISTRAR CUENTA")
        if btn:
            d, err = registrar_usuario(em, pw)
            if err: st.error(err)
            else: st.success("✅ Registrado. Ya puedes iniciar sesión.")

    with tab_a:
        with st.form("form_a"):
            tk = st.text_input("Token Maestro", type="password")
            btn = st.form_submit_button("VALIDAR")
        if btn:
            if tk.strip() in _admin_tokens_para_sesion():
                st.session_state["token_verificado"] = tk.strip()
