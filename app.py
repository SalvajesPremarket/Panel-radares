from datetime import datetime, timedelta, timezone
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import yfinance as yf
import os
import json
import time
import requests
import pandas as pd
import pandas_ta_classic as ta
import plotly.express as px
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest
from openai import OpenAI
from threading import Thread

st.set_page_config(page_title="Scanner Pre Market", layout="wide")

print("⚙️ Iniciando el Sistema de Radar Definitivo...")

# ==========================================
# 💾 PERSISTENCIA DE FILTROS
# ==========================================
RUTA_CONFIG = os.path.join(os.getcwd(), "config_filtros.json")

VALORES_POR_DEFECTO = {
    "precio_min": 2.0,
    "precio_max": 20.0,
    "gap_min": 7.0,
    "gap_max": 500.0,
    "flotacion_max": 10_000_000,
    "vol_rel_min": 1.3,
    "intervalo_refresco": 15,
    "direccion_cruce": "Hacia arriba",
    "macd_signo": "Positivo",
    "top_n": 10,
}


def cargar_config():
    config = VALORES_POR_DEFECTO.copy()
    try:
        with open(RUTA_CONFIG, "r") as f:
            guardado = json.load(f)
            config.update(guardado)
    except Exception:
        pass
    return config


def guardar_config(config):
    try:
        with open(RUTA_CONFIG, "w") as f:
            json.dump(config, f)
    except Exception as e:
        print(f"⚠️ No se pudo guardar la configuración: {e}")


if "config_filtros" not in st.session_state:
    st.session_state.config_filtros = cargar_config()

cfg = st.session_state.config_filtros

# ==========================================
# 🎨 ESTILO OSCURO TIPO FINVIZ
# ==========================================
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
        color: #e6
