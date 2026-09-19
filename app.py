import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ---------------------------------------------------------
# 1. CONTROL DE ACCESO (STREAMLIT SECRETS)
# ---------------------------------------------------------
st.set_page_config(page_title="Radar Pre-Market Multihilo", layout="wide")

def validar_token(token_usuario):
    if "tokens_autorizados" not in st.secrets:
        st.error("Error crítico: No se encontraron tokens configurados en Streamlit Secrets.")
        return False
    
    tokens = st.secrets["tokens_autorizados"]
    
    if token_usuario in tokens:
        fecha_expiracion_str = tokens[token_usuario]
        try:
            fecha_expiracion = datetime.strptime(fecha_expiracion_str, "%Y-%m-%d").date()
            if datetime.now().date() <= fecha_expiracion:
                return True
            else:
                st.error("El token ingresado ha expirado.")
                return False
        except ValueError:
            st.error("Error en el formato de fecha del token en la configuración (debe ser AAAA-MM-DD).")
            return False
    return False

# Interfaz de Login
st.sidebar.title("🔑 Seguridad")
token_ingresado = st.sidebar.text_input("Introduce tu Token de Acceso:", type="password")

if not token_ingresado:
    st.warning("Por favor, introduce un token de acceso en la barra lateral para desbloquear el sistema.")
    st.stop()

if not validar_token(token_ingresado):
    st.stop()

st.success("🔓 Acceso concedido correctamente.")

# ---------------------------------------------------------
# 2. CARGA DE CONFIGURACIÓN DE FILTROS DESDE GITHUB
# ---------------------------------------------------------
# REEMPLAZA ESTAS VARIABLES CON TUS DATOS REALES DE GITHUB
GITHUB_USER = "SalvajesPremarket"
GITHUB_REPO = "Panel-radares"
GITHUB_BRANCH = "main"

URL_FILTROS = f"https://githubusercontent.com{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}/config_filtros_radar.json"

@st.cache_data(ttl=600)
def cargar_filtros_github():
    try:
        response = requests.get(URL_FILTROS)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"No se pudo descargar config_filtros_radar.json. Estado: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Error al conectar con GitHub: {e}")
        return None

config_filtros = cargar_filtros_github()

if not config_filtros:
    st.warning("Usando filtros por defecto debido a un fallo en la carga externa.")
    # Filtros por defecto de respaldo para evitar crasheos de comillas
    config_filtros = {
        "precio_minimo": 1.0,
        "precio_maximo": 20.0,
        "volumen_minimo": 50000,
        "cambio_minimo_porcentaje": 4.0
    }

# ---------------------------------------------------------
# 3. PROCESAMIENTO MULTIHILO Y ESCANEO (EJEMPLO DE SIMULACIÓN API)
# ---------------------------------------------------------
# Esta función procesa un ticker individual (Reemplazar con tu lógica de API real)
def escanear_ticker(ticker, filtros):
    try:
        # Ejemplo simulado de respuesta de API de Mercado
        # Aquí consumirías tu API de datos financieros (Polygon, FinancialModelingPrep, etc.)
        url = f"https://ejemplo.com{ticker}" 
        # data = requests.get(url).json()
        
        # Simulación de datos limpios para evitar errores de comillas
        precio = 5.50 
        volumen = 120000
        cambio_pct = 8.5
        
        # Aplicación estricta de filtros cargados
        if (filtros["precio_minimo"] <= precio <= filtros["precio_maximo"] and 
            volumen >= filtros["volumen_minimo"] and 
            cambio_pct >= filtros["cambio_minimo_porcentaje"]):
            
            return {
                "Ticker": ticker,
                "Precio ($)": precio,
                "Volumen": volumen,
                "Cambio (%)": cambio_pct
            }
    except Exception:
        pass
    return None

# Interfaz Principal del Radar
st.title("📈 Radar Pre-Market Multihilo Optimizado")

# Lista de prueba de tickers a escanear
tickers_a_escanear = ["AAPL", "TSLA", "NVDA", "AMD", "MSFT", "AMZN", "META", "BABA", "NIO", "XPEV"]

if st.button("🚀 Iniciar Escaneo en Paralelo"):
    progreso = st.progress(0)
    resultados = []
    
    st.write(f"Escaneando {len(tickers_a_escanear)} activos usando ThreadPoolExecutor...")
    
    # Ejecución multihilo optimizada
    with ThreadPoolExecutor(max_workers=10) as executor:
        futuros = [executor.submit(escanear_ticker, ticker, config_filtros) for ticker in tickers_a_escanear]
        
        for i, futuro in enumerate(futuros):
            resultado = futuro.result()
            if resultado:
                resultados.append(resultado)
            progreso.progress((i + 1) / len(tickers_a_escanear))
            
    # Mostrar resultados en componente nativo st.dataframe
    if resultados:
        df_resultados = pd.DataFrame(resultados)
        st.subheader("📊 Resultados del Filtro Pre-Market")
        st.dataframe(df_resultados, use_container_width=True)
    else:
        st.info("Ningún activo cumple con los criterios de los filtros actuales en este momento.")
