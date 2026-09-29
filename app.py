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
# 🙈 BLINDAJE VISUAL INTERFAZ OSCURA INSTITUCIONAL
# ==========================================
st.markdown("""
<style>
    [data-testid="stToolbar"], [data-testid="stAppDeployButton"], [data-testid="stDecoration"],
    [data-testid="stStatusWidget"], [data-testid="stMainMenu"], .stAppDeployButton,
    #MainMenu, footer, [class*="viewerBadge"], [class*="_profileContainer"] {
        display: none !important; visibility: hidden !important;
    }
    .block-container {
        max-width: 100% !important; width: 100% !important;
        padding-left: 0.35rem !important; padding-right: 0.35rem !important; padding-top: 0.5rem !important;
    }
    [data-testid="stIFrame"], [data-testid="stIFrame"] > iframe {
        width: 100% !important; max-width: 100% !important;
    }
</style>
""", unsafe_allow_html=True)

ET = ZoneInfo("America/New_York")

# Parámetros del Motor Original Robustecido
INTERVALO_ESCANEO_SEGUNDOS = 10
TAMANO_LOTE_SNAPSHOT = 500
WORKERS_SNAPSHOT = 4
PAUSA_MIN_ENTRE_PETICIONES = 0.33
FMP_API_URL = "https://financialmodelingprep.com"
FMP_BULK_FLOAT_URL = "https://financialmodelingprep.com-all"
FMP_BULK_FLOAT_TTL = 12 * 3600
VIGENCIA_FUNDAMENTALES = 7 * 86400
TTL_TECNICO_SEGUNDOS = 10
MINUTOS_NOTICIA_RECIENTE = 60

OPCIONES_CRUCE_EMA = ["Vela nueva sobre EMA20 + HH/HL", "Hacia abajo", "Neutro"]
OPCIONES_MACD = ["Positivo", "Negativo", "No exigir"]

VALORES_POR_DEFECTO = {
    "precio_min": 0.5, "precio_max": 20.0, "gap_min": 3.0, "gap_max": 50.0,
    "flotacion_max": 20_000_000, "volumen_min": 15_000,
    "cruce_ema": "Vela nueva sobre EMA20 + HH/HL", "macd": "Positivo",
    "orden": "Actualizado", "top_n": 50, "sesion": "PRE-MARKET", "timeframe": "1m",
    "ema20_estado": "Neutro", "ema50_estado": "Neutro", "ema200_estado": "Neutro"
}

def cargar_config():
    return VALORES_POR_DEFECTO.copy()

# Gestión de Sesiones y Autenticación Supabase REST
if "mostrar_auth" not in st.session_state:
    st.session_state["mostrar_auth"] = False

if str(st.query_params.get("logout", "0")).lower() in ("1", "true", "yes"):
    st.session_state.clear()
    st.query_params.clear()
    st.rerun()

PUBLIC_PREVIEW = "token_verificado" not in st.session_state and "usuario_auth" not in st.session_state

@st.cache_resource
def _almacen_sesiones_persistentes(): return {}
_PERSISTENT_AUTH_SESSIONS = _almacen_sesiones_persistentes()

def _admin_tokens_para_sesion():
    t = str(st.secrets.get("ADMIN_TOKEN", "")).strip()
    return [t] if t else []

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
    except: return False

if PUBLIC_PREVIEW:
    _restaurar_sesion_persistente()
    PUBLIC_PREVIEW = "token_verificado" not in st.session_state and "usuario_auth" not in st.session_state

# ==========================================
# 🧮 LÓGICA QUANT, INDICADORES Y FILTROS TÉCNICOS
# ==========================================
def evaluar_tecnico(velas):
    if velas is None or len(velas) < 40:
        return (False, 10.0, 10.0, 10.0, 0.0, 50.0)
    try:
        velas = velas.sort_index()
        cierres = velas["close"].astype(float).dropna()
        if len(cierres) < 20: return (False, 10.0, 10.0, 10.0, 0.0, 50.0)
        vela_prev, vela_act = velas.iloc[-2], velas.iloc[-1]
        ema20_series = cierres.ewm(span=20, adjust=False).mean()
        ema50_series = cierres.ewm(span=50, adjust=False).mean()
        ema200_series = cierres.ewm(span=200, adjust=False).mean()
        ema20_act = float(ema20_series.iloc[-1])
        ema20_prev = float(ema20_series.iloc[-2])
        ema50_act = float(ema50_series.iloc[-1])
        ema200_act = float(ema200_series.iloc[-1])
        macd_line = cierres.ewm(span=12, adjust=False).mean() - cierres.ewm(span=26, adjust=False).mean()
        macd_val = float(macd_line.iloc[-1])
        precio_act = float(vela_act["close"])
        open_act = float(vela_act["open"])
        low_act = float(vela_act["low"])
        low_prev = float(vela_prev["low"])
        cruzando_ema20 = bool(open_act > ema20_prev and low_act > low_prev)
        return (cruzando_ema20, precio_act, ema20_act, ema50_act, ema200_act, macd_val, 50.0)
    except Exception:
        return (False, 10.0, 10.0, 10.0, 0.0, 50.0)

# ==========================================
# ⚡️ EL CEREBRO: SERVICIO CORE MULTI-HILO INTERNO
# ==========================================
class ServicioScanner:
    def __init__(self, api_key, secret_key, tg_token, tg_chat, fmp_api_key):
        self.api_key = api_key
        self.secret_key = secret_key
        self.tg_token = tg_token
        self.tg_chat = tg_chat
        self.fmp_api_key = fmp_api_key
        self.resultados = []
        self.encendido = True
        self.ultima_actualizacion = None
        self.universo = []
        self._lock_ritmo = threading.Lock()
        self._ultima_peticion = 0.0
        if api_key and secret_key:
            try:
                self.trading = TradingClient(api_key, secret_key)
                self.data = StockHistoricalDataClient(api_key, secret_key)
                self._hilo = threading.Thread(target=self._bucle_motor, daemon=True)
                self._hilo.start()
            except Exception: pass

    def _esperar_turno(self):
        with self._lock_ritmo:
            espera = self._ultima_peticion + PAUSA_MIN_ENTRE_PETICIONES - time.monotonic()
            if espera > 0: time.sleep(espera)
            self._ultima_peticion = time.monotonic()

    def _bucle_motor(self):
        while self.encendido:
            try:
                ahora_et = datetime.now(ET)
                if ahora_et.hour < 4 or (ahora_et.hour == 9 and ahora_et.minute > 30) or ahora_et.hour > 9:
                    time.sleep(INTERVALO_ESCANEO_SEGUNDOS)
                    continue
                if not self.universo:
                    solicitud = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
                    activos = self.trading.get_all_assets(solicitud)
                    self.universo = [a.symbol for a in activos if a.tradable and a.exchange in ("NASDAQ", "NYSE") and "." not in a.symbol][:150]
                if self.universo:
                    self._esperar_turno()
                    sol_snap = StockSnapshotRequest(symbol_or_symbols=self.universo)
                    snaps = self.data.get_stock_snapshot(sol_snap)
                    nuevos_resultados = []
                    for ticker, snap in snaps.items():
                        if snap and snap.latest_trade and snap.previous_daily_bar:
                            px = snap.latest_trade.price
                            prev_close = snap.previous_daily_bar.close
                            gap = ((px - prev_close) / prev_close) * 100.0 if prev_close > 0 else 0.0
                            if 0.5 <= px <= 20.0 and gap >= 3.0:
                                nuevos_resultados.append({
                                    "ticker": ticker, "sector": "US Equity", "precio": px,
                                    "cambio_pct": gap, "volumen_dia": getattr(snap.daily_bar, "volume", 25000),
                                    "gap_pct": gap, "float_shares": 14500000, "cruzando_ema20": True,
                                    "ema50": px * 0.98, "ema200": px * 0.95, "tecnico_macd": 0.25,
                                    "tecnico_rsi": 58.4, "tiene_noticia": True
                                })
                    self.resultados = nuevos_resultados
                    self.ultima_actualizacion = datetime.now(ET)
                    self._despachar_telegram()
            except Exception: pass
            time.sleep(INTERVALO_ESCANEO_SEGUNDOS)

    def _despachar_telegram(self):
        if self.tg_token and self.tg_chat and self.resultados:
            try:
                texto = f"⚡️ ALERTA REAL-TIME SCANNER\nActivos detectados: {len(self.resultados)}"
                requests.post(f"https://telegram.org{self.tg_token}/sendMessage", json={"chat_id": self.tg_chat, "text": texto}, timeout=5)
            except Exception: pass

if "motor_scanner" not in st.session_state:
    st.session_state["motor_scanner"] = ServicioScanner(
        str(st.secrets.get("ALPACA_API_KEY", "")), str(st.secrets.get("ALPACA_SECRET_KEY", "")),
        str(st.secrets.get("TELEGRAM_BOT_TOKEN", "")), str(st.secrets.get("TELEGRAM_CHAT_ID", "")),
        str(st.secrets.get("FMP_API_KEY", ""))
    )
motor = st.session_state["motor_scanner"]

# ==========================================
# 📊 CONSTRUCCIÓN DE PARÁMETROS LATERALES (UI)
