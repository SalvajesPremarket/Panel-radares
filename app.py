import streamlit as st
import pandas as pd
import numpy as np
import datetime
import requests  # Para el envío de señales HTTP al Broker

# 1. CONFIGURACIÓN ESTRICTA ESTILO FINVIZ (Ancho completo y limpio)
st.set_page_config(
    page_title="TradeScanner - Stock Screener",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# COLOR, TIPOGRAFÍA Y FORMATO PLANO IDÉNTICO A FINVIZ
st.markdown("""
<style>
    /* Fondo general gris de Finviz y tipografía densa */
    html, body, [data-testid="stAppViewContainer"] {
        background-color: #f3f3f3 !important;
        font-family: Verdana, Arial, Tahoma, sans-serif !important;
        font-size: 11px !important;
        color: #000000 !important;
    }

    /* Contenedor principal sin márgenes excesivos */
    .block-container {
        padding-top: 5px !important;
        padding-bottom: 5px !important;
        max-width: 100% !important;
    }

    /* FILTROS PLANOS: Eliminación de bordes 3D/redondeados modernos */
    div[data-testid="stSelectbox"] > div {
        border-radius: 0px !important;
        border: 1px solid #a0a0a0 !important;
        background-color: #ffffff !important;
        box-shadow: none !important;
    }

    div[data-testid="stNumberInput"] > div {
        border-radius: 0px !important;
        border: 1px solid #a0a0a0 !important;
        background-color: #ffffff !important;
        box-shadow: none !important;
    }

    /* Botones planos */
    button {
        border-radius: 0px !important;
        border: 1px solid #a0a0a0 !important;
        background-color: #f0f0f0 !important;
        font-size: 11px !important;
        box-shadow: none !important;
    }
</style>
""", unsafe_allow_html=True)

# LÓGICA DE ENVÍO HTTP AL ENGRANAJE DEL BROKER
def enviar_senal_broker(ticker, layout_canal):
    """
    Envía una petición HTTP POST para sincronizar el gráfico del Broker.
    Ajusta la URL ('http://localhost:8080/layout') al puerto real de tu
    puente o terminal.
    """
    url_puente = "http://localhost:8080/layout"
    payload = {
        "ticker": ticker,
        "layout_color": layout_canal,
        "timestamp": str(datetime.datetime.now())
    }
    try:
        requests.post(url_puente, json=payload, timeout=0.1)
    except Exception:
        # Se ignora silenciosamente si el puente local no está escuchando aún.
        pass

# 2. BASE DE DATOS DEL SCANNER (Valores adaptados a tus rangos exactos)
@st.cache_data
def generar_datos_finviz():
    tickers = [
        "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META",
        "TSLA", "AMD", "NFLX", "BABA", "PLTR", "SOUN"
    ]
    sectores = [
        "Tecnología", "Tecnología", "Tecnología", "Consumo",
        "Tecnología", "Comunicación", "Automotriz", "Tecnología",
        "Entretenimiento", "Consumo", "Software", "Inteligencia Artificial"
    ]

    np.random.seed(42)
    datos = {
        "Ticker": tickers,
        "Sector": sectores,
        "Precio ($)": np.round(np.random.uniform(5, 500, len(tickers)), 2),
        "Cambio %": np.round(np.random.uniform(-6.0, 6.0, len(tickers)), 2),
        # El código original tenía "Volumen":, incompleto.
        # Se generan valores simulados para mantener el ejemplo funcional.
        "Volumen": np.random.randint(100_000, 10_000_000, len(tickers)),
        "Gap %": [1.5, 4.2, 8.5, -2.1, 0.5, 5.1, 9.2, -0.8, 3.5, 7.1, 1.2, 4.8],
        "Flotación (M)": [12.5, 14.2, 18.0, 19.5, 22.0, 35.0, 48.0, 11.0, 15.5, 24.1, 41.3, 17.2],
        "EMA20 (1 min)": np.random.choice(
            ["1ra Vela 1min por encima", "1ra Vela 1min por debajo", "Sin patrón"],
            len(tickers)
        ),
        "MACD": np.random.choice(["Positivo", "Negativo", "Neutro"], len(tickers)),
    }
    return pd.DataFrame(datos)


df_activos = generar_datos_finviz()

# 3. BARRA SUPERIOR NEGRA COMPACTA (Estilo Finviz con Logotipo, Registro e Idioma)
st.markdown("""
<div style="background-color: #111111; padding: 6px 12px; display: flex; justify-content: space-between; align-items: center; border-bottom: 3px solid #b39212; margin-bottom: 10px;">
    <div style="display: flex; align-items: center;">
        <span style="font-size: 24px; margin-right: 10px;">🐂⏳🐻</span>
        <div>
            <h2 style="color: #f4d03f; margin: 0; font-family: Impact, Arial, sans-serif; font-size: 20px; letter-spacing: 0.5px;">TradeScanner</h2>
        </div>
    </div>
    <div style="color: #ffffff; font-size: 11px;">
        <span style="color: #a0a0a0;">Language:</span> <b style="color:#fff;">Español</b> |
        <span style="color: #a0a0a0;">Status:</span> <b style="color:#00ff00;">Live Connection</b>
    </div>
</div>
""", unsafe_allow_html=True)

# CASILLA DE REGISTRO CON CORREO ELECTRÓNICO (Panel Superior Integrado)
col_reg1, col_reg2, col_reg3 = st.columns([2, 1, 3])
with col_reg1:
    email_usuario = st.text_input(
        "🔑 Registro de Usuario (Ingrese su Correo Electrónico):",
        placeholder="usuario@correo.com"
    )
with col_reg2:
    st.write("")
    st.write("")
    if st.button("Crear Cuenta / Login"):
        if email_usuario:
            st.success("Registrado correctamente.")
        else:
            st.error("Escriba un correo.")

st.markdown("<br/>", unsafe_allow_html=True)

# 4. PANEL DE CONTROL OPERATIVO PLANO (Encendido, Horarios y Broker)
st.markdown("<b style='font-size:12px; color:#333;'>🎛️ CONTROL DEL MOTOR</b>", unsafe_allow_html=True)
col_ctrl1, col_ctrl2, col_ctrl3 = st.columns(3)

with col_ctrl1:
    estado_scanner = st.toggle("⚡ Alternar Scanner (On / Off)", value=True)

with col_ctrl2:
    expander_horario = st.expander("📅 Horario de Ejecución")
    with expander_horario:
        hora_inicio = st.time_input("Inicio", datetime.time(9, 30), label_visibility="collapsed")
        hora_cierre = st.time_input("Cierre", datetime.time(16, 0), label_visibility="collapsed")

with col_ctrl3:
    expander_broker = st.expander("🔌 API Enlace Broker")
    with expander_broker:
        broker_target = st.selectbox(
            "Broker Target:",
            ["Interactive Brokers", "TradeStation", "Custom Bridge"]
        )

st.markdown("<hr style='border: 0; border-top: 1px solid #d0d0d0; margin: 8px 0;'/>", unsafe_allow_html=True)

# 5. MATRIZ DE PESTAÑAS DE FILTRADO COMPLETAMENTE ALINEADAS Y PLANAS
st.markdown("<b style='font-size:12px; color:#333;'>🔎 FILTROS DEL SCREENER</b>", unsafe_allow_html=True)

col_f1, col_f2, col_f3, col_f4 = st.columns(4)

with col_f1:
    f_volumen_min = st.number_input(
        "Volumen Mínimo (Monto exacto):",
        min_value=0,
        value=0,
        step=100000
    )
    f_precio = st.selectbox(
        "Precio ($):",
        ["Cualquiera", "< $10", "$10 - $50", "$50 - $200", "> $200"]
    )

with col_f2:
    f_gap = st.selectbox(
        "Gap %:",
        ["Cualquiera", "De 0% a 3%", "De 4% a 6%", "De 7% a 10%"]
    )
    f_sector = st.selectbox(
        "Sector Activo:",
        ["Todos"] + list(df_activos["Sector"].unique())
    )

with col_f3:
    f_flotacion = st.selectbox(
        "Flotación (Float):",
        ["Cualquiera", "De 10M a 15M", "De 16M a 20M", "De 21M a 50M"]
    )
    f_ema20 = st.selectbox(
        "EMA 20 Patrón:",
        ["Cualquiera", "1ra Vela 1min por encima", "1ra Vela 1min por debajo"]
    )

with col_f4:
    f_macd = st.selectbox(
        "MACD Estado:",
        ["Cualquiera", "Positivo", "Negativo", "Neutro"]
    )

# --- PROCESAMIENTO MATEMÁTICO DE FILTROS ---
df_filtrado = df_activos.copy()

if f_volumen_min > 0:
    df_filtrado = df_filtrado[df_filtrado["Volumen"] >= f_volumen_min]

if f_precio == "< $10":
    df_filtrado = df_filtrado[df_filtrado["Precio ($)"] < 10]
elif f_precio == "$10 - $50":
    df_filtrado = df_filtrado[(df_filtrado["Precio ($)"] >= 10) & (df_filtrado["Precio ($)"] <= 50)]
elif f_precio == "$50 - $200":
    df_filtrado = df_filtrado[(df_filtrado["Precio ($)"] >= 50) & (df_filtrado["Precio ($)"] <= 200)]
elif f_precio == "> $200":
    df_filtrado = df_filtrado[df_filtrado["Precio ($)"] > 200]

if f_gap == "De 0% a 3%":
    df_filtrado = df_filtrado[(df_filtrado["Gap %"] >= 0) & (df_filtrado["Gap %"] <= 3)]
elif f_gap == "De 4% a 6%":
    df_filtrado = df_filtrado[(df_filtrado["Gap %"] >= 4) & (df_filtrado["Gap %"] <= 6)]
elif f_gap == "De 7% a 10%":
    df_filtrado = df_filtrado[(df_filtrado["Gap %"] >= 7) & (df_filtrado["Gap %"] <= 10)]

if f_sector != "Todos":
    df_filtrado = df_filtrado[df_filtrado["Sector"] == f_sector]

if f_flotacion == "De 10M a 15M":
    df_filtrado = df_filtrado[(df_filtrado["Flotación (M)"] >= 10) & (df_filtrado["Flotación (M)"] <= 15)]
elif f_flotacion == "De 16M a 20M":
    df_filtrado = df_filtrado[(df_filtrado["Flotación (M)"] >= 16) & (df_filtrado["Flotación (M)"] <= 20)]
elif f_flotacion == "De 21M a 50M":
    df_filtrado = df_filtrado[(df_filtrado["Flotación (M)"] >= 21) & (df_filtrado["Flotación (M)"] <= 50)]

if f_ema20 != "Cualquiera":
    df_filtrado = df_filtrado[df_filtrado["EMA20 (1 min)"] == f_ema20]

if f_macd != "Cualquiera":
    df_filtrado = df_filtrado[df_filtrado["MACD"] == f_macd]

st.markdown("<br/>", unsafe_allow_html=True)

# 6. TABLA DE RESULTADOS DENSA CON COLUMNA DE ENGRANAJE EMISORA HTTP
st.markdown("<b style='font-size:12px; color:#333;'>📊 TABLA DE RESULTADOS DE ACTIVOS</b>", unsafe_allow_html=True)

if not estado_scanner:
    st.warning("Scanner Apagado.")
elif df_filtrado.empty:
    st.info("Ningún activo cumple los criterios.")
else:
    # Definición de los 10 Canales / Layouts del Broker para enlazar
    opciones_layout = [
        "⚙️ L1 (Rojo)", "⚙️ L2 (Azul)", "⚙️ L3 (Verde)", "⚙️ L4 (Amarillo)",
        "⚙️ L5 (Morado)", "⚙️ L6 (Naranja)", "⚙️ L7 (Blanco)", "⚙️ L8 (Negro)",
        "⚙️ L9 (Cian)", "⚙️ L10 (Rosa)"
    ]

    # Renderizado de Cabeceras con Ancho Fijo y Fuente de Finviz
    header_cols = st.columns([1.5, 0.8, 1.2, 1, 1, 1, 1, 1.8, 1])
    headers = [
        "LINK LAYOUT (API)", "TICKER", "SECTOR", "PRECIO", "CAMBIO %",
        "VOLUMEN", "GAP %", "EMA20 (1 MIN)", "MACD"
    ]

    st.markdown(
        "<div style='background-color:#d3d3d3; height:1px; margin-bottom:4px;'></div>",
        unsafe_allow_html=True
    )
    for col, h in zip(header_cols, headers):
        col.markdown(
            f"<span style='font-weight:bold; font-size:10px; color:#444444;'>{h}</span>",
            unsafe_allow_html=True
        )
    st.markdown(
        "<div style='background-color:#a0a0a0; height:2px; margin-top:4px; margin-bottom:6px;'></div>",
        unsafe_allow_html=True
    )

    # Impresión Fila por Fila
    for idx, row in df_filtrado.iterrows():
        cols = st.columns([1.5, 0.8, 1.2, 1, 1, 1, 1, 1.8, 1])

        layout_idx = idx % len(opciones_layout)
        layout_canal = opciones_layout[layout_idx]

        with cols[0]:
            if st.button(layout_canal, key=f"layout_{idx}"):
                enviar_senal_broker(row["Ticker"], layout_canal)
                st.toast(f"Señal enviada: {row['Ticker']} → {layout_canal}")

        with cols[1]:
            st.markdown(f"<b>{row['Ticker']}</b>", unsafe_allow_html=True)

        with cols[2]:
            st.markdown(str(row["Sector"]))

        with cols[3]:
            st.markdown(f"${row['Precio ($)']:.2f}")

        with cols[4]:
            cambio = row["Cambio %"]
            st.markdown(f"{cambio:+.2f}%")

        with cols[5]:
            st.markdown(f"{int(row['Volumen']):,}")

        with cols[6]:
            gap = row["Gap %"]
            st.markdown(f"{gap:+.2f}%")

        with cols[7]:
            st.markdown(str(row["EMA20 (1 min)"]))

        with cols[8]:
            st.markdown(str(row["MACD"]))

        st.markdown(
            "<div style='background-color:#d0d0d0; height:1px; margin:2px 0;'></div>",
            unsafe_allow_html=True
        )

st.markdown("<hr style='border:0; border-top:1px solid #b0b0b0; margin-top:10px;'>", unsafe_allow_html=True)
st.caption("TradeScanner - Stock Screener")
