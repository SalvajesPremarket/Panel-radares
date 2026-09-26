import os
import json
import time
import hashlib
from urllib.parse import quote
from html import escape as html_escape
import threading
from datetime import date, datetime, timedelta, timezone, time as dt_time
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
import streamlit as st
try:
    import websocket
except ImportError:
    websocket = None
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.trading.requests import GetAssetsRequest, GetCalendarRequest

st.set_page_config(page_title="Scanner Pre Market", layout="wide")

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
</style>
""", unsafe_allow_html=True)


ET = ZoneInfo("America/New_York")

# ==========================================
# ⚙️ PARÁMETROS DEL MOTOR
# ==========================================
INTERVALO_ESCANEO_SEGUNDOS = 10        # cada cuánto el motor recorre el mercado (una sola vez para todos los usuarios)
TAMANO_LOTE_SNAPSHOT = 500             # tickers por petición de snapshot
WORKERS_SNAPSHOT = 4                   # peticiones de snapshot en paralelo
PAUSA_MIN_ENTRE_PETICIONES = 0.33      # ~180 peticiones/min a Alpaca (límite: 200/min)

# Streaming real de trades Alpaca (Basic = IEX y hasta 30 símbolos suscritos).
STREAM_URL = "wss://stream.data.alpaca.markets/v2/iex"
STREAM_MAX_SYMBOLS = 30
STREAM_RECONNECT_SEGUNDOS = 5

# Radar base: rango AMPLIO que el motor enriquece. Cada usuario filtra su vista dentro de este rango.
BASE_PRECIO_MIN = 0.5
BASE_PRECIO_MAX = 20.0
BASE_GAP_MIN = 3.0
BASE_GAP_MAX = 1000.0
BASE_FLOTACION_MAX = 50_000_000

# 🧪 ETAPA DE DEPURACIÓN DE FILTROS
# 1 = solo precio + EMA20 + MACD. Telegram queda APAGADO.
# Luego podremos pasar a 2, 3, 4... agregando un filtro por vez.
ETAPA_PRUEBA_FILTROS = 3

MAX_ENRIQUECER = 500                   # PRUEBA 3: ampliar temporalmente la muestra técnica; no es un filtro de trading

# Float: FMP es la fuente principal; volumen y velas técnicas se obtienen con Alpaca.
FMP_API_URL = "https://financialmodelingprep.com/stable/shares-float"
MAX_FUNDAMENTALES_POR_CICLO = 1        # FMP: una consulta de float por ciclo para evitar HTTP 429
WORKERS_FUNDAMENTALES = 1               # FMP no se consulta en paralelo
VIGENCIA_FUNDAMENTALES = 7 * 86400
REINTENTO_FUNDAMENTALES = 300
PAUSA_FMP_429_SEGUNDOS = 900            # tras HTTP 429, pausa FMP durante 15 min
FMP_MIN_INTERVAL_SEGUNDOS = 30          # máximo 2 consultas/minuto para no golpear el límite de FMP

# Horario automático: 04:00–16:00 ET, solo días de mercado según Alpaca.
HORA_AUTO_INICIO_ET = 4
HORA_AUTO_FIN_ET = 16
TTL_CALENDARIO_MERCADO = 12 * 3600

TTL_TECNICO_SEGUNDOS = 30              # no recalcular EMA/MACD de un ticker más seguido que esto
VENTANA_CRUCE_EMA_MINUTOS = 1
MARGEN_PROXIMIDAD_EMA = 0.05
MINUTOS_NOTICIA_RECIENTE = 60

# --- Cuadro "Eventos en vivo" (parte de abajo de la interfaz) ---
MAX_EVENTOS = 500                      # eventos que guarda el motor en memoria
MAX_HISTORIAL_CICLOS = 10               # ciclos recientes conservados para depuración
EVENTOS_MOSTRAR = 40                   # filas visibles en el cuadro
EVENTOS_ALTO_PX = 430                  # alto del cuadro (con scroll)

# --- Botón encender/apagar del scanner ---
MOSTRAR_BOTON_ENCENDIDO_A_TODOS = True  # True: lo ve cualquier usuario con licencia. False: solo el administrador

# --- Opciones de los filtros técnicos (la primera es la que viene por defecto) ---
OPCIONES_CRUCE_EMA = ["Hacia arriba", "Hacia abajo", "Neutro"]
OPCIONES_MACD = ["Positivo", "Negativo", "No exigir"]

NOMBRE_ARCHIVO_HTML = "radar.html"
RUTA_CACHE_FUNDAMENTALES = os.path.join(os.getcwd(), "cache_fundamentales.json")
RUTA_CONFIG = os.path.join(os.getcwd(), "config_filtros.json")

VALORES_POR_DEFECTO = {
    "precio_min": 0.5,
    "precio_max": 20.0,
    "gap_min": 5.0,
    "gap_max": 500.0,
    "flotacion_max": 15_000_000,
    "volumen_min": 20_000,
    "intervalo_refresco": 5,
    # Valores técnicos usados por el motor compartido/diagnóstico.
    # Antes faltaban aquí y filtrar_resultados() podía lanzar KeyError
    # con self.filtros_dueno, abortando el ciclo antes de publicar el diagnóstico.
    "cruce_ema": "Hacia arriba",
    "macd": "Positivo",
    "orden": "Actualizado",
    "top_n": 50,
}


def cargar_config():
    """Filtros por defecto (los del dueño). Solo lectura: cada usuario ajusta su propia vista."""
    return VALORES_POR_DEFECTO.copy()


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
        with open(RUTA_CONFIG, "w", encoding="utf-8") as f:
            json.dump({"hora_inicio_auto_min": int(inicio_min), "hora_fin_auto_min": int(fin_min)}, f)
    except Exception:
        pass


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
    """Valida el acceso administrativo/legacy por token."""
    admin_token = str(st.secrets.get("ADMIN_TOKEN", "")).strip()
    if admin_token and token_usuario == admin_token:
        return True, "2099-01-01"

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
            or "No se pudo completar la operación."
        )
        return None, str(mensaje)

    except Exception as e:
        return None, f"Error de conexión con el servicio de autenticación: {e}"


def registrar_usuario(email, password):
    """Crea una cuenta de usuario mediante Supabase Auth."""
    email = str(email).strip().lower()

    if not email or "@" not in email:
        return None, "Introduce un correo electrónico válido."

    if len(password) < 8:
        return None, "La contraseña debe tener al menos 8 caracteres."

    # Supabase debe enviar el enlace de confirmación de vuelta a la
    # aplicación pública del scanner, no a share.streamlit.io.
    redirect_url = "https://jd6gih.streamlit.app"

    data, error = supabase_auth_request(
        f"signup?redirect_to={quote(redirect_url, safe='')}",
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

def cerrar_sesion():
    """Limpia la sesión local de Streamlit."""
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
    for _i in range(len(COLORES_LAYOUT_DEFECTO)):
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
            max-width: 520px;
            margin: 45px auto 20px auto;
            background: #0d1118;
            border: 1px solid #2a3348;
            border-radius: 16px;
            padding: 30px;
            box-shadow: 0 18px 50px rgba(0,0,0,.35);
        }
        .auth-title {
            color: #d4af37;
            font-family: sans-serif;
            font-weight: 800;
            text-align: center;
            margin-bottom: 4px;
        }
        .auth-subtitle {
            color: #8e96a3;
            text-align: center;
            font-size: 11px;
            letter-spacing: 2px;
            margin-bottom: 20px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="auth-card">
            <div class="auth-title">TRADE SCANNER INSTITUTIONAL</div>
            <div class="auth-subtitle">SCANNER</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Una sola ventana para todos.
    # El acceso de administrador está dentro de la misma pantalla y
    # requiere el token secreto configurado en Streamlit Secrets.
    # No se utiliza una segunda URL ni un parámetro especial de administrador.
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
                _guardar_usuario_auth(data, tipo="usuario")
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
                        _guardar_usuario_auth(data, tipo="usuario")
                        st.success("Cuenta creada correctamente.")
                        st.rerun()
                    else:
                        st.success(
                            "✅ Cuenta creada. Revisa tu correo para confirmar "
                            "la cuenta y después inicia sesión."
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
                    st.session_state["token_verificado"] = token_limpio
                    st.session_state["fecha_vencimiento"] = estado
                    st.session_state["tipo_acceso"] = "admin"
                    st.rerun()
                elif estado == "EXPIRADO":
                    st.error("🔒 Token expirado.")
                elif estado == "FORMATO":
                    st.error("❌ Error de configuración del token.")
                else:
                    st.error("❌ Token no válido. Acceso denegado.")

    st.stop()


# Si no existe ninguna sesión, mostramos login/registro.
if (
    "token_verificado" not in st.session_state
    and "usuario_auth" not in st.session_state
):
    pantalla_autenticacion()


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


# Barra discreta de sesión.
with st.sidebar:
    st.markdown("### 👤 Sesión")

    if ES_ADMIN:
        st.success("Administrador")
    else:
        _email_ui = st.session_state.get("usuario_auth", {}).get(
            "email", "Usuario"
        )
        st.info(_email_ui)

    if st.button(
        "🚪 CERRAR SESIÓN",
        key="cerrar_sesion_global",
        width="stretch",
    ):
        cerrar_sesion()
        st.rerun()


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


def evaluar_tecnico(cierres, entrada_actual=None):
    """Calcula EMA20, MACD y Bollinger sobre cierres de 1 minuto.

    La señal EMA20 de entrada se evalúa en el nacimiento de la vela actual,
    no al cierre: apertura_actual > EMA20 y apertura_actual > mínimo_de_la_vela_anterior.
    El máximo de la vela actual nunca participa en esta condición.
    """
    if cierres is None or len(cierres) < 40:
        return (False, False, False, False, None, None, None, 0,
                None, None, None, None, None, None)
    barras_count = int(len(cierres))
    ema20 = cierres.ewm(span=20, adjust=False).mean()
    macd_line = cierres.ewm(span=12, adjust=False).mean() - cierres.ewm(span=26, adjust=False).mean()
    precio_act = float(cierres.iloc[-1]); precio_prev = float(cierres.iloc[-2])
    ema_act = float(ema20.iloc[-1]); ema_prev = float(ema20.iloc[-2])
    macd_actual = macd_line.iloc[-1]
    macd_val = float(macd_actual) if not pd.isna(macd_actual) else None
    bb_mid = cierres.rolling(20).mean(); bb_std = cierres.rolling(20).std()
    bb_upper = bb_mid.iloc[-1] + 2 * bb_std.iloc[-1]
    bb_upper_val = float(bb_upper) if not pd.isna(bb_upper) else None
    if pd.isna(ema_act) or ema_act <= 0:
        return (False, False, False, False, precio_act, None, macd_val, barras_count,
                precio_prev, ema_prev, precio_act, ema_act, bb_upper_val, None)
    # ENTRADA EMA20: se decide al nacer la vela actual.
    # No se usa el máximo de la vela actual porque todavía no terminó.
    if entrada_actual is not None:
        apertura_actual = entrada_actual.get("open")
        minimo_anterior = entrada_actual.get("low_anterior")
        ema_entrada = entrada_actual.get("ema20", ema_act)
        cruzo_arriba = bool(
            apertura_actual is not None
            and minimo_anterior is not None
            and ema_entrada is not None
            and apertura_actual > ema_entrada
            and apertura_actual > minimo_anterior
        )
    elif ETAPA_PRUEBA_FILTROS == 1:
        cruzo_arriba = precio_act > ema_act
    else:
        # Respaldo para llamadas antiguas: cruce por cierre.
        cruzo_arriba = bool(precio_prev <= ema_prev and precio_act > ema_act)
    cruzo_abajo = bool(precio_prev >= ema_prev and precio_act < ema_act)
    macd_positivo = bool(macd_val is not None and macd_val > 0)
    macd_negativo = bool(macd_val is not None and macd_val < 0)
    bb_dist_pct = ((bb_upper_val - precio_act) / precio_act * 100.0) if bb_upper_val is not None and precio_act > 0 else None
    return (cruzo_arriba, cruzo_abajo, macd_positivo, macd_negativo,
            precio_act, ema_act, macd_val, barras_count, precio_prev, ema_prev,
            precio_act, ema_act, bb_upper_val, bb_dist_pct)


def descargar_cierres(data_client, tickers):
    """Velas de 1 minuto de Alpaca para EMA20/MACD, sin depender de Yahoo Finance."""
    salida = {}
    if not tickers:
        return salida

    for i in range(0, len(tickers), 50):
        lote = tickers[i:i + 50]
        try:
            # En el plan Basic de Alpaca, las consultas históricas que llegan
            # hasta el presente quedan limitadas por el acceso en tiempo real.
            # Pedimos el histórico hasta 20 minutos atrás para que Alpaca
            # entregue las velas históricas completas disponibles.
            inicio = datetime.now(timezone.utc) - timedelta(days=2)
            fin = datetime.now(timezone.utc) - timedelta(minutes=20)
            solicitud = StockBarsRequest(
                symbol_or_symbols=lote,
                timeframe=TimeFrame.Minute,
                start=inicio,
                end=fin,
                limit=10000,
            )
            barras = data_client.get_stock_bars(solicitud)
            datos = getattr(barras, "df", None)
        except Exception as e:
            self_error = str(e)
            print(f"⚠️ Error descargando velas de Alpaca (lote {len(lote)}): {self_error}")
            continue

        if datos is None or datos.empty:
            continue

        try:
            if isinstance(datos.index, pd.MultiIndex):
                for ticker in lote:
                    try:
                        serie = datos.xs(ticker, level=0)["close"].dropna()
                    except Exception:
                        continue
                    if len(serie) >= 40:
                        salida[ticker] = serie
            else:
                # Caso excepcional de un solo ticker.
                if "close" in datos.columns and len(lote) == 1:
                    serie = datos["close"].dropna()
                    if len(serie) >= 40:
                        salida[lote[0]] = serie
        except Exception:
            continue
    return salida


def filtrar_resultados(filas, p):
    resultado = []
    for c in filas:
        if not (p["precio_min"] <= c["precio"] <= p["precio_max"]):
            continue
        # Etapa 1: dejamos fuera gap, float y volumen para localizar
        # exactamente qué filtro está provocando la caída a cero.
        if ETAPA_PRUEBA_FILTROS >= 4:
            if not (p["gap_min"] <= c["cambio_pct"] <= p["gap_max"]):
                continue
        if ETAPA_PRUEBA_FILTROS >= 3:
            if c["float_shares"] is not None and c["float_shares"] >= p["flotacion_max"]:
                continue
        if ETAPA_PRUEBA_FILTROS >= 2:
            if c.get("volumen_dia", 0) < p.get("volumen_min", 20_000):
                continue
        if ETAPA_PRUEBA_FILTROS == 1:
            # PRUEBA 1 real: EMA20 = precio por encima de EMA20.
            if not c.get("cruzando_ema20", False):
                continue
            macd = "Positivo"
        else:
            cruce = p.get("cruce_ema", "Neutro")
            if cruce == "Hacia arriba" and not c["cruzando_ema20"]:
                continue
            if cruce == "Hacia abajo" and not c["cruzando_ema20_abajo"]:
                continue
            macd = p.get("macd", "No exigir")
        if macd == "Positivo" and not c["macd_positivo"]:
            continue
        if macd == "Negativo" and not c["macd_negativo"]:
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
        if not (p["gap_min"] <= e["cambio_pct"] <= p["gap_max"]):
            continue
        if e["float_shares"] is not None and e["float_shares"] >= p["flotacion_max"]:
            continue
        if e.get("volumen_dia", 0) < p.get("volumen_min", 20_000):
            continue
        salida.append(e)
    return salida


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

        self.trading = TradingClient(api_key, secret_key)
        self.data = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)

        self.encendido = True
        # Control manual del administrador: si se apaga, el horario automático NO lo vuelve a encender.
        # El horario se carga desde disco si el administrador ya lo guardó antes;
        # si no hay nada guardado, usa los valores por defecto del código.
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
            "tras_vol_rel": 0,
            "ema_arriba": 0,
            "macd_positivo": 0,
            "ema_y_macd": 0,
            "resultados": 0,
            "raw_tickers": [],
            "final_tickers_mismo_ciclo": [],
            "eliminados_post_ema_macd": [],
            "eliminados_post_ema_macd_count": 0,
            "gap_aplicado": ETAPA_PRUEBA_FILTROS >= 4,
            "gap_min": self.filtros_dueno.get("gap_min", BASE_GAP_MIN),
            "gap_max": self.filtros_dueno.get("gap_max", BASE_GAP_MAX),
        }

        self.tg_msg_id = None
        self.tg_ultimo_hash = None

        self.eventos = []                    # cuadro "Eventos en vivo" (el más nuevo primero)
        self._ultimo_precio_evento = {}
        self.historial_ciclos = []            # últimos ciclos: permite ver cuándo entran/salen candidatos
        self._raw_tickers_ciclo_anterior = set()

        self.cache_tecnico = {}
        self.cache_series_tecnico = {}  # ticker -> (timestamp, serie de cierres históricos)
        # Estado intraminuto para detectar el nacimiento de la vela de entrada:
        # ticker -> minuto actual, apertura actual y mínimo acumulado de esa vela.
        self._velas_intraminuto = {}

        # WebSocket real: los trades alimentan la vela actual sin esperar al snapshot.
        self._stream_lock = threading.RLock()
        self._stream_stop = threading.Event()
        self._stream_ws = None
        self._stream_thread = None
        self._stream_tickers_deseados = set()
        self._stream_tickers_activos = set()
        self._stream_trades = {}
        self._stream_estado = "iniciando"
        self._stream_autenticado = False
        self._iniciar_stream()

        self.cache_fund = self._leer_cache_fundamentales()
        # Control específico de FMP para no martillar la API cuando devuelve HTTP 429.
        self.fmp_pausado_hasta = 0.0
        self._ultima_peticion_fmp = 0.0

        self._lock_ritmo = threading.Lock()
        self._ultima_peticion = 0.0

        # Control del hilo para permitir un reinicio limpio desde el panel de administrador.
        self._detener_hilo = threading.Event()
        self._lock_reinicio = threading.Lock()
        self._hilo = threading.Thread(target=self._bucle, daemon=True)
        self._hilo.start()

    # ---------- streaming real Alpaca ----------
    def _iniciar_stream(self):
        """Inicia un WebSocket persistente para trades IEX.
        El plan Basic permite streaming en tiempo real IEX y hasta 30 símbolos.
        """
        if websocket is None:
            self._stream_estado = "websocket-client no instalado"
            return
        if self._stream_thread is not None and self._stream_thread.is_alive():
            return
        self._stream_stop.clear()
        self._stream_thread = threading.Thread(target=self._stream_loop, daemon=True, name="alpaca-trades-stream")
        self._stream_thread.start()

    def _stream_on_open(self, ws):
        with self._stream_lock:
            self._stream_ws = ws
            self._stream_autenticado = False
            self._stream_estado = "autenticando"
        try:
            ws.send(json.dumps({"action": "auth", "key": self.api_key, "secret": self.secret_key}))
        except Exception as e:
            self._stream_estado = f"error autenticando: {e}"

    def _stream_on_message(self, ws, raw):
        try:
            mensajes = json.loads(raw)
            if isinstance(mensajes, dict):
                mensajes = [mensajes]
            for msg in mensajes if isinstance(mensajes, list) else []:
                if not isinstance(msg, dict):
                    continue
                if msg.get("T") == "error":
                    self._stream_estado = f"Alpaca stream: {msg.get('msg', msg)}"
                    continue
                if msg.get("T") == "success" and msg.get("msg") == "authenticated":
                    with self._stream_lock:
                        self._stream_autenticado = True
                        self._stream_estado = "autenticado"
                        tickers = sorted(self._stream_tickers_deseados)[:STREAM_MAX_SYMBOLS]
                        ws_actual = self._stream_ws
                    if ws_actual is not None and tickers:
                        ws_actual.send(json.dumps({"action": "subscribe", "trades": tickers}))
                        with self._stream_lock:
                            self._stream_tickers_activos = set(tickers)
                    continue
                if msg.get("T") != "t":
                    continue
                ticker = msg.get("S")
                precio = msg.get("p")
                ts_raw = msg.get("t")
                if not ticker or precio is None or not ts_raw:
                    continue
                precio = float(precio)
                ts = pd.Timestamp(ts_raw).to_pydatetime()
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                minuto = ts.replace(second=0, microsecond=0)
                with self._stream_lock:
                    self._stream_trades[ticker] = {"price": precio, "timestamp": ts}
                    estado = self._velas_intraminuto.get(ticker)
                    if estado is None or estado.get("minuto") != minuto:
                        low_anterior = estado.get("low") if estado else None
                        self._velas_intraminuto[ticker] = {
                            "minuto": minuto,
                            "open": precio,
                            "high": precio,
                            "low": precio,
                            "low_anterior": low_anterior,
                        }
                    else:
                        estado["high"] = max(float(estado.get("high", precio)), precio)
                        estado["low"] = min(float(estado.get("low", precio)), precio)
        except Exception as e:
            print(f"⚠️ Error procesando trade WebSocket: {e}")

    def _stream_on_error(self, ws, error):
        self._stream_estado = f"stream error: {error}"
        print(f"⚠️ Alpaca WebSocket: {error}")

    def _stream_on_close(self, ws, close_status_code, close_msg):
        with self._stream_lock:
            self._stream_ws = None
            self._stream_autenticado = False
            self._stream_tickers_activos = set()
        if not self._stream_stop.is_set():
            self._stream_estado = "desconectado; reconectando"

    def _stream_loop(self):
        while not self._stream_stop.is_set():
            try:
                ws = websocket.WebSocketApp(
                    STREAM_URL,
                    on_open=self._stream_on_open,
                    on_message=self._stream_on_message,
                    on_error=self._stream_on_error,
                    on_close=self._stream_on_close,
                )
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                self._stream_estado = f"stream detenido: {e}"
                print(f"⚠️ Error WebSocket Alpaca: {e}")
            if not self._stream_stop.is_set():
                self._stream_stop.wait(STREAM_RECONNECT_SEGUNDOS)

    def _actualizar_stream_tickers(self, tickers):
        """Mantiene suscritos los 30 candidatos con mayor volumen del ciclo."""
        if websocket is None:
            return
        deseados = set(tickers[:STREAM_MAX_SYMBOLS])
        with self._stream_lock:
            self._stream_tickers_deseados = deseados
            ws = self._stream_ws
            autenticado = self._stream_autenticado
            activos = set(self._stream_tickers_activos)
        if ws is None or not autenticado:
            return
        agregar = sorted(deseados - activos)
        quitar = sorted(activos - deseados)
        try:
            if agregar:
                ws.send(json.dumps({"action": "subscribe", "trades": agregar}))
            if quitar:
                ws.send(json.dumps({"action": "unsubscribe", "trades": quitar}))
            with self._stream_lock:
                self._stream_tickers_activos = deseados
        except Exception as e:
            self._stream_estado = f"error actualizando stream: {e}"

    def detener_stream(self):
        self._stream_stop.set()
        with self._stream_lock:
            ws = self._stream_ws
            self._stream_ws = None
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

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
        except Exception as e:
            self.ultimo_error = f"Calendario Alpaca: {e}"
            print(f"⚠️ Error consultando calendario de Alpaca: {e}")

    def _esta_en_horario_automatico(self):
        """True solo de 04:00 a 16:00 ET en un día de mercado según Alpaca."""
        ahora_et = datetime.now(ET)
        self._actualizar_calendario(ahora_et)
        es_dia_mercado = ahora_et.date() in self.dias_mercado_cache
        minuto_actual = ahora_et.hour * 60 + ahora_et.minute + ahora_et.second / 60
        inicio = self.hora_inicio_auto_min
        fin = self.hora_fin_auto_min
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

    def configurar_horario(self, inicio, fin):
        """Actualiza el horario automático compartido por todo el scanner y lo guarda en disco
        para que sobreviva a reinicios de la app (redeploy, inactividad, etc.)."""
        self.hora_inicio_auto_min = inicio.hour * 60 + inicio.minute
        self.hora_fin_auto_min = fin.hour * 60 + fin.minute
        guardar_horario_en_disco(self.hora_inicio_auto_min, self.hora_fin_auto_min)
        self.auto_motivo = "Horario automático actualizado; esperando el próximo ciclo"

    def reiniciar_scanner(self):
        """Reinicia de forma segura el motor compartido del scanner.

        Detiene el hilo anterior, limpia el estado de resultados/cachés de trabajo
        y crea un único hilo nuevo. No modifica las credenciales ni el horario.
        """
        with self._lock_reinicio:
            hilo_anterior = self._hilo
            self._detener_hilo.set()
            self.detener_stream()

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
                "gap_aplicado": ETAPA_PRUEBA_FILTROS >= 4,
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
            self.finales_ema_macd_actual = []
            self.cache_tecnico = {}
            self.cache_series_tecnico = {}
            self._velas_intraminuto = {}
            with self._stream_lock:
                self._stream_tickers_deseados = set()
                self._stream_tickers_activos = set()
                self._stream_trades = {}
            self._stream_stop.clear()
            self._iniciar_stream()
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
            print(f"🌐 Universo cargado: {len(self.universo)} tickers.")
        except Exception as e:
            self.ultimo_error = f"Universo: {e}"
            print(f"⚠️ Error cargando universo: {e}")

    # ---------- snapshots en paralelo ----------
    def _descargar_snapshots(self):
        lotes = [self.universo[i:i + TAMANO_LOTE_SNAPSHOT] for i in range(0, len(self.universo), TAMANO_LOTE_SNAPSHOT)]

        def pedir(lote):
            self._esperar_turno()
            try:
                return self.data.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=lote)) or {}
            except Exception as e:
                self.ultimo_error = f"Snapshot: {e}"
                return {}

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
            ahora = time.time()
            espera_fmp = self._ultima_peticion_fmp + FMP_MIN_INTERVAL_SEGUNDOS - ahora
            if espera_fmp > 0:
                # El scanner nunca debe quedar dormido esperando FMP.
                # Se intentará en un ciclo posterior cuando venza el intervalo.
                return None
            self._ultima_peticion_fmp = time.time()
            respuesta = requests.get(
                FMP_API_URL,
                params={"symbol": ticker, "apikey": self.fmp_api_key},
                timeout=8,
            )
            if respuesta.status_code == 429:
                self.fmp_pausado_hasta = time.time() + PAUSA_FMP_429_SEGUNDOS
                self.ultimo_error = (
                    f"FMP devolvió HTTP 429 para {ticker}. "
                    "Se pausaron las consultas de float durante 15 minutos para evitar más bloqueos."
                )
                print(f"⚠️ FMP HTTP 429 para {ticker}; pausa de {PAUSA_FMP_429_SEGUNDOS}s")
                return None
            if respuesta.status_code in (401, 403):
                self.ultimo_error = (
                    f"FMP rechazó la API para {ticker} (HTTP {respuesta.status_code}). "
                    "Revisa que FMP_API_KEY sea válida y tenga acceso a shares-float."
                )
                return None
            if respuesta.status_code != 200:
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

    def _asegurar_fundamentales(self, tickers):
        ahora = time.time()
        faltan = []
        for t in tickers:
            e = self.cache_fund.get(t)
            if e is None:
                faltan.append(t)
                continue
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
            # FMP es la única fuente de FLOAT.
            float_fmp = self._float_fmp(t)
            float_final = float_fmp
            try:
                if float_final is not None:
                    float_final = float(float_final)
            except (TypeError, ValueError):
                float_final = None

            if float_fmp is not None:
                estado, fuente = "ok", "FMP"
            else:
                estado, fuente = "no_data", "FMP"

            return t, {
                "float": float_final,
                "float_source": fuente,
                "float_status": estado,
                "ts": time.time(),
            }

        with ThreadPoolExecutor(max_workers=WORKERS_FUNDAMENTALES) as ex:
            for t, entrada in ex.map(pedir, faltan):
                self.cache_fund[t] = entrada
        self._guardar_cache_fundamentales()

    # ---------- EMA20 / MACD ----------
    def _asegurar_tecnico(self, tickers, snapshots=None):
        """Motor técnico híbrido y no bloqueante.

        1) El histórico de cierres para EMA20/MACD se cachea y solo se renueva
           cuando vence TTL_TECNICO_SEGUNDOS.
        2) La vela intraminuto se evalúa en CADA ciclo usando el snapshot actual,
           sin esperar a que termine la vela.
        3) Así, una consulta histórica lenta no impide detectar el nacimiento
           de una vela de entrada. Esta separación permite migrar después a
           WebSocket sin cambiar la lógica del motor.
        """
        ahora = time.time()
        snapshots = snapshots or {}

        # Histórico: solo descargamos lo que realmente venció.
        pendientes_hist = []
        for t in tickers:
            e = self.cache_series_tecnico.get(t)
            if e is None or ahora - float(e.get("ts", 0)) > TTL_TECNICO_SEGUNDOS:
                pendientes_hist.append(t)

        series_nuevas = descargar_cierres(self.data, pendientes_hist) if pendientes_hist else {}
        for t, serie in series_nuevas.items():
            self.cache_series_tecnico[t] = {"ts": ahora, "serie": serie}

        for t in tickers:
            cache_serie = self.cache_series_tecnico.get(t, {})
            serie = cache_serie.get("serie")
            entrada_actual = None

            # Primero usamos el trade real del WebSocket. El snapshot queda como
            # respaldo para los símbolos que todavía no tengan tick recibido.
            with self._stream_lock:
                stream_trade = dict(self._stream_trades.get(t, {}))
                estado_stream = dict(self._velas_intraminuto.get(t, {}))
            snap = snapshots.get(t)
            trade = getattr(snap, "latest_trade", None) if snap is not None else None
            if stream_trade.get("price") is not None and stream_trade.get("timestamp") is not None:
                precio = float(stream_trade["price"])
                estado = estado_stream
                entrada_actual = {
                    "open": estado.get("open"),
                    "low_anterior": estado.get("low_anterior"),
                    "ema20": None,
                }
            elif trade is not None and getattr(trade, "price", None) is not None and getattr(trade, "timestamp", None) is not None:
                try:
                    precio = float(trade.price)
                    ts = trade.timestamp
                    minuto = ts.replace(second=0, microsecond=0)
                    estado = self._velas_intraminuto.get(t)

                    if estado is None or estado.get("minuto") != minuto:
                        # Fallback: snapshot. El WebSocket será la fuente principal.
                        low_anterior = estado.get("low") if estado else None
                        self._velas_intraminuto[t] = {
                            "minuto": minuto, "open": precio, "high": precio,
                            "low": precio, "low_anterior": low_anterior,
                        }
                    else:
                        estado["high"] = max(float(estado.get("high", precio)), precio)
                        estado["low"] = min(float(estado.get("low", precio)), precio)

                    estado = self._velas_intraminuto[t]
                    entrada_actual = {
                        "open": estado.get("open"),
                        "low_anterior": estado.get("low_anterior"),
                        "ema20": None,
                    }
                    if serie is not None and len(serie) >= 40:
                        ema20 = serie.ewm(span=20, adjust=False).mean()
                        entrada_actual["ema20"] = float(ema20.iloc[-1]) if not pd.isna(ema20.iloc[-1]) else None
                except Exception as ex:
                    print(f"⚠️ Error formando vela intraminuto {t}: {ex}")

            (cruz_arriba, cruz_abajo, macd_pos, macd_neg, precio_act, ema_act,
             macd_val, barras_count, precio_prev, ema_prev, precio_actual,
             ema_actual, bb_upper, bb_dist_pct) = evaluar_tecnico(serie, entrada_actual)

            self.cache_tecnico[t] = (
                ahora, cruz_arriba, cruz_abajo, macd_pos, macd_neg,
                precio_act, ema_act, macd_val, barras_count,
                precio_prev, ema_prev, precio_actual, ema_actual,
                bb_upper, bb_dist_pct
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
                for n in respuesta.json().get("news", []):
                    con_noticia.update(n.get("symbols", []))
                return con_noticia & set(tickers)
        except Exception:
            pass
        return set()

    # ---------- Telegram ----------
    def _enviar_telegram(self, texto_tabla):
        if not self.tg_token or not self.tg_chat:
            return
        hash_actual = hashlib.md5(texto_tabla.encode("utf-8")).hexdigest()
        if hash_actual == self.tg_ultimo_hash:
            return
        try:
            payload = {
                "chat_id": self.tg_chat,
                "text": f"⚡️ <b>SCANNER</b>\n<pre>{texto_tabla}</pre>",
                "parse_mode": "HTML",
            }
            cabeceras = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            if self.tg_msg_id is None:
                url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
                r = requests.post(url, json=payload, headers=cabeceras, timeout=15)
                if r.status_code == 200:
                    self.tg_msg_id = r.json()["result"]["message_id"]
                    self.tg_ultimo_hash = hash_actual
                else:
                    print(f"❌ Telegram rechazó el mensaje: {r.status_code} - {r.text}")
            else:
                url = f"https://api.telegram.org/bot{self.tg_token}/editMessageText"
                payload["message_id"] = self.tg_msg_id
                r = requests.post(url, json=payload, headers=cabeceras, timeout=15)
                if r.status_code == 200 or "message is not modified" in r.text:
                    self.tg_ultimo_hash = hash_actual
                elif "not found" in r.text:
                    self.tg_msg_id = None  # el mensaje fue borrado: se envía uno nuevo en el próximo ciclo
                else:
                    print(f"❌ Error al editar en Telegram: {r.status_code} - {r.text}")
        except Exception as e:
            print(f"⚠️ Error de red con Telegram: {e}")

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

    # ---------- ciclo principal ----------
    def _ciclo(self):
        inicio = time.monotonic()
        self.ultimo_error = None

        if not self.universo or time.time() - self.universo_ts > 6 * 3600:
            self._cargar_universo()
        if not self.universo:
            return

        snapshots = self._descargar_snapshots()

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
            if ETAPA_PRUEBA_FILTROS >= 4 and not (BASE_GAP_MIN <= cambio <= BASE_GAP_MAX):
                continue
            base.append({
                "ticker": ticker,
                "precio": precio,
                "cambio_pct": cambio,
                "volumen_dia": snap.daily_bar.volume or 0,
                "actualizado": snap.latest_trade.timestamp,
            })

        # Conservar el total real del radar para la interfaz/diagnóstico.
        # Después se limita el enriquecimiento a MAX_ENRIQUECER para no saturar
        # las APIs, pero eso no debe convertir 532 candidatos en "0" ni en 120.
        radar_base_total = len(base)
        self.n_radar_base = radar_base_total
        base.sort(key=lambda c: c["volumen_dia"], reverse=True)
        base = base[:MAX_ENRIQUECER]

        if ETAPA_PRUEBA_FILTROS >= 3:
            self._asegurar_fundamentales([c["ticker"] for c in base])
        else:
            # Etapa 1: no consultar FMP/float. Así aislamos EMA + MACD
            # y evitamos que el límite HTTP 429 contamine la prueba.
            self.ultimo_error = None

        # PRUEBA 2: separamos el filtro de float y el de volumen en dos pasadas
        # para poder medir, en el diagnóstico, cuánto recorta CADA UNO por
        # separado (antes ambos se aplicaban en el mismo bucle y el panel
        # mostraba el mismo número para "tras_float" y "tras_vol_rel").
        tras_float = []
        for c in base:
            entrada = self.cache_fund.get(c["ticker"], {})
            float_shares = entrada.get("float")
            # En la etapa 1 no usamos float ni volumen como filtros.
            # Tampoco consultamos float en esta etapa para evitar HTTP 429 de FMP.
            # PRUEBA 3: se usa el mismo umbral que el resto de la app
            # (self.filtros_dueno["flotacion_max"], 15,000,000 por defecto)
            # en vez de BASE_FLOTACION_MAX (50,000,000), para que este
            # conteo de diagnóstico coincida con el filtro que de verdad
            # determina el resultado final en filtrar_resultados().
            if ETAPA_PRUEBA_FILTROS >= 3 and float_shares is not None and float_shares >= self.filtros_dueno.get("flotacion_max", 15_000_000):
                continue
            c["float_shares"] = float_shares
            c["float_status"] = entrada.get(
                "float_status",
                "pending" if not entrada else "no_data"
            )
            c["float_source"] = entrada.get("float_source", "")
            tras_float.append(c)
        self.n_tras_float = len(tras_float)

        enriquecidos = []
        for c in tras_float:
            if ETAPA_PRUEBA_FILTROS >= 2 and c.get("volumen_dia", 0) < self.filtros_dueno.get("volumen_min", 20_000):
                continue
            # El porcentaje de subida se representa directamente con cambio_pct.
            c["volumen_relativo"] = c["cambio_pct"]
            enriquecidos.append(c)

        # El snapshot encuentra candidatos; el WebSocket los sigue tick a tick.
        enriquecidos.sort(key=lambda c: c.get("volumen_dia", 0), reverse=True)
        self._actualizar_stream_tickers([c["ticker"] for c in enriquecidos])

        tickers_enr = [c["ticker"] for c in enriquecidos]
        self._asegurar_tecnico(tickers_enr, snapshots)
        con_noticia = self._noticias_recientes(tickers_enr)

        for c in enriquecidos:
            tech = self.cache_tecnico.get(c["ticker"], (0, False, False, False, False, None, None, None, 0, None, None, None, None, None, None))
            _, cruz_arriba, cruz_abajo, macd_pos, macd_neg, precio_tec, ema_tec, macd_tec, barras_tec, precio_prev_tec, ema_prev_tec, precio_actual_tec, ema_actual_tec, bb_upper_tec, bb_dist_tec = tech
            c["cruzando_ema20"] = cruz_arriba
            c["cruzando_ema20_abajo"] = cruz_abajo
            c["macd_positivo"] = macd_pos
            c["macd_negativo"] = macd_neg
            c["tecnico_precio"] = precio_tec
            c["tecnico_ema20"] = ema_tec
            c["tecnico_macd"] = macd_tec
            c["tecnico_barras"] = barras_tec
            c["tecnico_precio_anterior"] = precio_prev_tec
            c["tecnico_ema20_anterior"] = ema_prev_tec
            c["tecnico_precio_actual"] = precio_actual_tec
            c["tecnico_ema20_actual"] = ema_actual_tec
            c["bb_upper"] = bb_upper_tec
            c["bb_dist_pct"] = bb_dist_tec
            c["cruce_ema20_confirmado"] = bool(cruz_arriba and precio_prev_tec is not None and ema_prev_tec is not None)
            c["tiene_noticia"] = c["ticker"] in con_noticia

        # Diagnóstico del embudo: no cambia ningún filtro ni el resultado del scanner.
        ema_arriba_count = sum(1 for c in enriquecidos if c.get("cruzando_ema20"))
        macd_positivo_count = sum(1 for c in enriquecidos if c.get("macd_positivo"))
        ema_y_macd_count = sum(
            1 for c in enriquecidos
            if c.get("cruzando_ema20") and c.get("macd_positivo")
        )
        # El filtro de volumen ya se aplicó al construir 'enriquecidos', así
        # que ese conteo ES el resultado "tras volumen". El paso previo
        # (antes de aplicar volumen) queda guardado en self.n_tras_float.
        tras_vol_rel_count = len(enriquecidos)
        tecnicos_validos = sum(1 for c in enriquecidos if c.get("tecnico_barras", 0) >= 40)
        ema_calculable = sum(1 for c in enriquecidos if c.get("tecnico_ema20") is not None)
        macd_calculable = sum(1 for c in enriquecidos if c.get("tecnico_macd") is not None)
        tickers_enr_unicos = len({c.get("ticker") for c in enriquecidos})
        # Conteo bruto que cumple EMA20 + MACD antes del límite de presentación.
        # En PRUEBA 4 top_n=50, por lo que el resultado final podrá mostrar hasta 50.
        candidatos_ema_macd_brutos = sum(
            1 for c in enriquecidos
            if c.get("cruzando_ema20") and c.get("macd_positivo")
        )
        self.diagnostico_filtros = {
            "radar_base": radar_base_total,
            "enviados_tecnico": len(enriquecidos),
            "con_40_barras": tecnicos_validos,
            "ema_calculable": ema_calculable,
            "macd_calculable": macd_calculable,
            "tras_float": getattr(self, "n_tras_float", len(enriquecidos)),
            "tras_vol_rel": tras_vol_rel_count,
            "ema_arriba": ema_arriba_count,
            "macd_positivo": macd_positivo_count,
            "ema_y_macd": ema_y_macd_count,
            "candidatos_ema_macd_brutos": candidatos_ema_macd_brutos,
            "tickers_unicos": tickers_enr_unicos,
            "duplicados": len(enriquecidos) - tickers_enr_unicos,
            "resultados": len(filtrar_resultados(enriquecidos, self.filtros_dueno)),
            "gap_aplicado": ETAPA_PRUEBA_FILTROS >= 4,
            "gap_min": self.filtros_dueno.get("gap_min", BASE_GAP_MIN),
            "gap_max": self.filtros_dueno.get("gap_max", BASE_GAP_MAX),
        }

        # Guardamos una fotografía del resultado REAL de este ciclo antes de publicar
        # la lista nueva. Esto evita perder candidatos cuando desaparecen en el siguiente ciclo.
        p_hist = dict(self.filtros_dueno)
        p_hist.update({"cruce_ema": "Hacia arriba", "macd": "Positivo", "top_n": 50, "orden": "Actualizado"})
        resultados_finales_hist = filtrar_resultados(enriquecidos, p_hist)

        # PRUEBA 4B: conservar las dos listas del MISMO ciclo.
        candidatos_raw_actual = [c for c in enriquecidos if c.get("cruzando_ema20") and c.get("macd_positivo")]
        self.candidatos_ema_macd_actual = list(candidatos_raw_actual)
        self.finales_ema_macd_actual = list(resultados_finales_hist)
        raw_tickers = {c.get("ticker") for c in candidatos_raw_actual}
        final_tickers = {c.get("ticker") for c in resultados_finales_hist}
        eliminados_mismo_ciclo = sorted(raw_tickers - final_tickers)

        # PRUEBA 4C: comparar candidatos EMA20+MACD con el ciclo inmediatamente anterior.
        # Esto solo diagnostica entradas/salidas naturales entre ciclos; no cambia filtros.
        raw_anterior = set(getattr(self, "_raw_tickers_ciclo_anterior", set()))
        mantenidos_entre_ciclos = sorted(raw_tickers & raw_anterior)
        entraron_este_ciclo = sorted(raw_tickers - raw_anterior)
        salieron_este_ciclo = sorted(raw_anterior - raw_tickers)
        self._raw_tickers_ciclo_anterior = set(raw_tickers)

        self.diagnostico_filtros.update({
            "raw_tickers": sorted(x for x in raw_tickers if x),
            "raw_tickers_anterior": sorted(x for x in raw_anterior if x),
            "mantenidos_entre_ciclos": mantenidos_entre_ciclos,
            "entraron_este_ciclo": entraron_este_ciclo,
            "salieron_este_ciclo": salieron_este_ciclo,
            "final_tickers_mismo_ciclo": sorted(x for x in final_tickers if x),
            "eliminados_post_ema_macd": eliminados_mismo_ciclo,
            "eliminados_post_ema_macd_count": len(eliminados_mismo_ciclo),
        })
        self._registrar_historial_ciclo(enriquecidos, resultados_finales_hist)

        self.resultados = enriquecidos
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
        self._registrar_eventos(enriquecidos)

        # 🛑 TELEGRAM APAGADO DURANTE LA DEPURACIÓN.
        # No se envía nada al grupo mientras comprobamos los filtros.
        p = dict(self.filtros_dueno)
        p.update({"cruce_ema": "Hacia arriba", "macd": "Positivo", "top_n": 50, "orden": "Actualizado"})
        top = filtrar_resultados(enriquecidos, p)
        if top:
            tabla = f"{'TICK':<5}|{'PRE':>5}|{'CHG%':>4}|{'VOL':>5}|{'FLT':>5}\n" + "-" * 28 + "\n"
            for c in top:
                nombre = f"🔥{c['ticker']}" if c["tiene_noticia"] else c["ticker"]
                tabla += (f"{nombre:<5}|{c['precio']:>5.2f}|{c['cambio_pct']:>3.0f}%|"
                          f"{formatear_numero_grande(c['volumen_dia']):>5}|{formatear_numero_grande(c['float_shares']):>5}\n")
            # Telegram permanece desactivado en las pruebas.
            # self._enviar_telegram(tabla)
            self._escribir_html(tabla)

    def _bucle(self):
        while not self._detener_hilo.is_set():
            inicio = time.monotonic()
            try:
                en_horario = self._esta_en_horario_automatico()
                if self.encendido and en_horario:
                    self._ciclo()
            except Exception as e:
                self.ultimo_error = f"Ciclo: {e}"
                print(f"⚠️ Error en escaneo: {e}")
            espera = max(1.0, INTERVALO_ESCANEO_SEGUNDOS - (time.monotonic() - inicio))
            # Event.wait permite interrumpir el descanso inmediatamente al reiniciar.
            self._detener_hilo.wait(timeout=espera)


@st.cache_resource
def obtener_servicio(api_key, secret_key, tg_token, tg_chat, fmp_api_key):
    print("⚙️ Iniciando el motor del scanner (una sola vez para todos los usuarios)...")
    return ServicioScanner(api_key, secret_key, tg_token, tg_chat, fmp_api_key, cargar_config())


servicio = obtener_servicio(
    st.secrets["ALPACA_API_KEY"],
    st.secrets["ALPACA_SECRET_KEY"],
    st.secrets.get("TELEGRAM_BOT_TOKEN", None),
    st.secrets.get("TELEGRAM_CHAT_ID", "-1004440734539"),
    st.secrets.get("FMP_API_KEY", None),
)

# ==========================================
# 🎨 ESTILO OSCURO
# ==========================================
st.markdown("""
<style>
    :root {
        --ts-bg: #030303;
        --ts-panel: #090909;
        --ts-panel-2: #0d0d0d;
        --ts-gold: #d4af37;
        --ts-gold-bright: #f2d675;
        --ts-gold-dark: #7d641c;
        --ts-text: #f3f3f3;
        --ts-muted: #9a9a9a;
        --ts-red: #d64545;
        --ts-green: #37c77a;
    }

    .stApp {
        background:
            radial-gradient(circle at 50% -10%, rgba(212,175,55,.09), transparent 34%),
            linear-gradient(180deg, #080808 0%, #030303 55%, #000000 100%) !important;
        color: var(--ts-text) !important;
    }
    [data-testid="stHeader"], [data-testid="stSidebar"] {
        background: #030303 !important;
    }
    .block-container {
        max-width: 1500px;
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
        background:#090909 !important;
        color:#f4f4f4 !important;
        border-color:rgba(212,175,55,.42) !important;
    }
    div[data-baseweb="select"] * { color:#f1f1f1 !important; }
    .stButton button {
        border-radius:6px !important;
        font-weight:800 !important;
        border:1px solid rgba(212,175,55,.55) !important;
        background:linear-gradient(180deg,#17130a,#0d0b07) !important;
        color:var(--ts-gold-bright) !important;
    }
    .stButton button:hover {
        border-color:var(--ts-gold-bright) !important;
        box-shadow:0 0 14px rgba(212,175,55,.12) !important;
    }
    [data-testid="stMetricValue"] { color:var(--ts-gold-bright) !important; }

    /* Header / logo */
    .dash-header {
        width:100% !important;
        height:122px !important;
        box-sizing:border-box !important;
        padding:0 !important;
        margin:0 0 8px !important;
        border:1px solid rgba(212,175,55,.55) !important;
        border-radius:8px !important;
        overflow:hidden !important;
        background:#000 !important;
        box-shadow:0 0 28px rgba(212,175,55,.07), inset 0 0 28px rgba(255,255,255,.015) !important;
    }
    .dash-header .logo-image {
        display:block !important;
        width:100% !important;
        height:100% !important;
        object-fit:cover !important;
        object-position:center !important;
    }

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

    @media (max-width: 900px) {
        .dash-header { height:96px !important; }
    }
    @media (max-width: 640px) {
        .block-container {
            max-width:100% !important;
            padding-left:.25rem !important;
            padding-right:.25rem !important;
        }
        .dash-header {
            height:70px !important;
            border-radius:5px !important;
        }
        .simple-title { font-size:13px !important; }
        .small-note { font-size:10px !important; }
        div[data-testid="stHorizontalBlock"] { gap:.35rem !important; }
        div[data-testid="stNumberInput"] input,
        div[data-testid="stTextInput"] input { font-size:12px !important; }
    }
</style>
""", unsafe_allow_html=True)

IMG_LOGO_B64 = "UklGRvD1AABXRUJQVlA4IOT1AACQLwSdASp8CNQCPjEYi0QiIaERWXSQIAMEsrd7T3vJLQn5O9MLv/icwv+vWO/eNk7uFk7DNmLvNkMGATT3/K1k3Ssv+p/wn7pd//GvjH7t/f/8b/l/7/+3nzK8V9Tnhz7l/kP8V/cf/f/rPvQ/Sf8Lu794/3//c+6r3/vLf0v/Rf3v/Nf9T/H////5fcr/df7b/bf33/xfKL9H/7v/Pfur+//4A/xP+Zf5P+0f5H/qf4n////v8Qf83/x/5j3g/4f/k/8H9uf/b8hf6V/b/+j/i/3t+Zn/N/8j/D/vT8pf8D/n/+z/kP8n/9/oB/qH94/5P7X//H43PYm/dr/8+4L/Sv8h/2fzm+MD/4f7T/bf///p/aD/Tv9R/7P9L/uv///1PsY/nv9z/7H7S//z/ZfQB/3v//71X8A/5X//9gD9//bX5e+lv49/Q/6D8rf7B/7fan8g+5/yv+A/zn+w/vP/l/3/3U44/f/8zzR/mP4T/Q/3n/Mf7n/JfuB7bHjD83P8r/Jfkp8hf45/Lv8D/dP21/wX7XfX5+P4jnC/8r/0+oX7c/U/9N/hf8v/x/8f8If4P/T/xHrX/B/6z/l/m59AX9S/uH+l/v/7nf3z////L8o/6Xin/k//J+2vwDfzT+0/8f/Pflj9Nv9v/5P8//p/3Q94v5x/m/+3/mf9d8hv8p/rv+6/v/+g/9P+k////0+9f/1e7z9vP/3+9nyxftF/+BSNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpfR+ZizOKvQYpgEkyQy8bailUxtqKVTG2opVMbailUxtqKVTG2opVMbailUxtqKVTG2opVMbailUxtqKVTG2opVMbailUxtE8dg6+1z/vhiEUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKdCvA4HSK8cIyglxMEoPc6P5DU0kmSGXjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmMs2DewGer+UjLqE7FGoV2VBaSJeUlXbXJRfdsJ/oZeNtRSqY21FKpjbUUqjSGLpCgsqqZuJvlas21FKpjbUUqmNtRSqY21FKpjbUUpuMECHBGfmT4R+H2X3DcM1jwcSBWEUsGWE/0MvG2opVMbailUxtqJsHbs5TGl1Y44+ZgReL0DyHaKqq8VCAp0D/GuKKeuo1ikl7stKtMGGuYSPgW/0MvG2opVMbailUxtqKVS/LHDPyKMBRsxseYuoLI5LsoIisJ/oZeNtRSqY21FKpjbUUp0HNhiTPEVcj0o7F5OYNY8FXgZQwGYSLMfdpiRhQLmnXC8UgWfJnvI6y1wrzKifH+HOAV1r9/ASTJDLxtqKVTG2opVMbPSPOAihbf7el2nWYjG/zI4J0KA2HpxlxDH0nOfA8jB07u2js844cwpf2trknwbnXARb/Qy8bailUxtqKVTG2opVMY8miDEktG1F6WllwvxdGTSO8Y0dOjUv/Qy8bailUxtqKVTG2opTd//8PqOH2ZFefg+2e7f5JF8ppTDFACJyneHuGBQSVejIRn09KIIpbi8H1E3K9fjoXVMFdJ7Hb9C+ehtL8bailUxtqKVTG2opVMbZxmZEsiCYzvdqefb2hzxHMlty/g/Mnbj2cWAvdnhTgXuanzYvvx3KbuR2rTrSqY21FKpjbUUqmNtRSH0RSy3LLpFLBEun7FIM1FdHdENKwAtnQID4zDkTyEGna7nChitvOcCQNKmzQqcSBCpbiSTJDLxtqKVTG2oo/nd7n9/dKMuQExOvmu5WvYgP/vv77S5OFVCU4uQqW7yaN4tSZKyAMfaJoyHX67Lygysh9z6i7VwyBvEG66CKVRp9U9DmdmZiE7oA5pPHgRXyW5ZdEpDqFqL6TsRBF9J2IKw1ezOA1zhyLOjJJGWfU9s5dxqen+uE5yV+nJfWHU/G7NmsMoSASgwZVG4wGoTxEjcfozRIa1Wh83C7GZa71uIyiBIEmjRQXVmWT6Vhrg0nf/5Adf6K7B198aoVCoT+nYvzSWcORZpLOHIs0o6s+BhIPig6JmmAdDkekvuNjO9Qwo0EOnWJJ2IgjDsexV3qaEjH40og76SLsvzfweltI/JdGALHbKVX3Cfr363KYa7BE6YmegU/kOUNbnSWBDGqYbgv66LcsfdDVjm6dsowJz9SoNlUeit4SplHNP8cMIV4giTwrJ0/DIZeNtRSqY21FKo0pt3ZFCVk4b8ZKP//9fGQG4DXOJBz5SNVY0ItOu8TUb3ondYEhCBF+iD9NyO6M4jS30qxJo4K0aGFn3gzLzt5U9n8beoN0RCIQ/SwiQT58qi/JQbtGit6VKakqDZx7wUnazTHjLFVoEtVmqGTzasMjgGvxmrdIit5LYrIrb3Gg5iS+qmxrc9+c2fVxxEDYc9n/V32unWJPjS4GJiHyFXVCDPE9debKInE4nE4DTlCDeCzl5wF8QgMT/Nc41nw544OdoiAOAQrgEivZ4qBS/HhkkYC+0VO4QpF+ScjFnQedcW5EtqUqhW0WkfBjUv4pXqjivJnPcfyZ3CdGS7rwAIId03B3TxCSeaOIpqMWCRmEFK2ivXFJ4djZu1OqED9L/o+TFN/RPsS2ApOOkabmxBoGKiW3956vGcQvPa1exPUKtOiEqkCf6GXjbUUql8zKBWd2PpAa96raaiT6ST10dHwY3ycVO51V5fvztlZ/lk869d3h4VParmsMb7YrNuKzrphtzDUiHgwhZUkF2RaRwRhb+u9FPY3qhG2opVMbailUxtqKVRaJt2czz4YZY1GhdEPOTqE8UNofy39SMg7onW1FKpjbUUqmNtRSqYyxG5BIiuQg+slL/D3hbf3wNsoucsiyq9Rn9kXe62m2AD7xekmIQ2lviYra/5ukaW+5nII9wKRAfhlXfGoGBBralMHYbTV/6KFuAqC+we5D56nt6+u+jHIrUbzNLo/YfgTZP/LCQT4i0q+K8oKdxgEkyQy8bZs9mJhTW5sXN1Den/IBmb0le0LbSWgOPsjB06N4LGafKsXC7tXeNBgloWNXbU99tr/jwhgv5EbQMqB0B3Zxi+R/QHOIv99ngurDeMTrLS3r+N8Ub9tmr0v528WFKG+cvph4JJkhl421FrQy8bafamTbs5dmASIirHDIpVMbailUxtqKVTG2ok4ZwCjfW8+R/+kcX/Jx0JIi9yCq4YznSBxzXatRCqG9421GzUv51BtPMPAfSi4m+ox2Tw9gAVIGGa+740y2md/DM/Z6BKW1GheK6bmAfsCDa/WPEq75HLpJbQ+fD8CLT9weF0hmgN4ezEGZhP9DLxtqJEoYm+MIlGUO7+ipPxRu0YxB5vc2xI73R/GylrN6KPGfRNQkDRhUWVcwNKhQZxhEV8X5aLX7gwQsStkaiZtgYq0mhh26t6Th2IreYNDLJmInv4rNlSQJ40uAbTr46OOte9c5q6keQYSZdaWWOCSZIZeAJeyC7HG59W4nvIL77Bo+IX/iMuTdghGZRtptGkyQy8bailUxtqKVTG2opzxPZmG++No4lZZBgBTDbT80CR3cAleYEisYaxRv5Tiv8D0tVetGF3ZgTe7z4KLPzLbSRfMitbNqYDbZwLCrtrBaLUuTc5s/ne+h9U5ac/cjO8xsR/m6ZQr+hl421FKsxyh2UtIde8X/oVrG9jCWHtQRFs+vL/UXSkJQpuFpVZlQz2HrkcdLjdcY7csUOdkZ8YwdVa9IZtWkOJRRdwW4hQ3TD4aAT83xkWGOmWL7Y6jCe7LvVW3GpGYZSDeAwrpoXIT+HwoKSUOuMMU7FR26PWmG5HQWmfG2opVMKtBYCUZ95MAA+eaJdyXlhykAsRNstGMP3DZ6CLbPJP9DLxtqKVTG2opVMbailUvngG/GaskKkUQE26EcFY7BHZOB1H86AOL4FscsnwlFc5lhIZWInerKlowVWf6JUEqfncKzcjOFtBXAIvMLBopGnpv4VDYZSGsgM/RTLO2px6WWlUxtqKVRRaMf9lH6W02NvJPs7i5BhVIbEdBq26GKqSqrqatNxem5ILt5Mf1MIub0w1ZRgt1kGV3bnRENuPCts6jlbFqRosfbADAM2Zxcom9J6pq2I/GiMgNUh9sC+MZ8+dBTLbWmg9jEUSysgwHeHhrGysjAyojBNxj3+mJh/bvdF+RybMeHFHrvenoY0vDxZhEo2Y3+bgMczf/F2JexNAStCvm2GqgerTetW0cOjl468hbVr3g30fHJkw9Hk83VDxKLri+302NvlCTk4hfJwaL0tttTGLDSBLq9RiSWur5Vp5Jm9yGcmcYPkgmZuIyLRvlJRIuuD85IVrEaxkk9NEYZoR0ZVsHoPnvpR2wOwv8X98z/7nGpjq/5//JvkYu3aid/VhtzLbHImzbvF1cklejwvGbpj9gUW6IImMhQRl+PxpoH+mc9C10/0YJ9rgn2+KEZuQlwwy8cJekmRn9HemB1FOSjR5qgj/uDuyqr3XtbVh9yGjE5ByDkNVIQUzmhIuYpKTL7oIJJkhl42bzZAbPwPOLSrWAQaE61RowMEotC7nMrhDZRWDTeG8N3NkheI1VF5hKhOcdpFEbq5Mi33QLG9BY6bIV8UV+P/lwvDYJ9rp1UQl0ANcLmWvIFSzZj7YUV9+khecRnpqKqW6SMgkbqrqyGYy3xPi9CYYAXM21vb2RVNLrV0pn7P4om7gv+R7ZsLNWLT1omi6/W0jgq4BJw5rTiFPonXeMXi/CBul8oRk2NCoIFmQZ9zS/8+uerzPXJucp35kc09CBwzUks1ymTsh8AJWPxnruqy04zpj/t0l6JhX5wLS4U8w4WkajbOEbT75PdzpuJYeq2Zt/N6BQ2S0C6oXpI0sOP+CgTXza3gzYcWE74mKgP5TUz7zsQG8RxjnHSkC6PaV/efsx9Iudq2SjsAIPvEE9Tz3rNLSOFLeLmsYRPDrFpMnIZOyfmzwQRUNI65BspjCoP/x7JSnugmervmknIH/9TqLxtRupofrxuIugTg2WpKgO7MBKfSAI3EkIBW++jNwNlYO+jTGh+SZN8UpSTgKwZ+BcQp/Sw3qKwwHtf0fyrFbEXVuW1xFeQVJDujYjAfnR/RVN+m1/oTdFWh/97BpAvQ+Jasp47E59URyMXlfrOX/Qy8bailOl8XCR8sujfKcIUm06oYAQeWzoDE/dcFICRv4aOqWbIuuV7fvSzF45xU8ANOUgi6RThT0/oML5XD2Rv73kVJXv7f1BlAjOVx5jfGgp93QdB9ooFpNGGuyqPcVsyG3CImAzE9AYSSq4+5ZtxkUqa1Mx0XJQ3/6O3IoWF5O1ulLc4UnMoL7o+FfZh3EOLfO3oiaOCEJOm02phMVGPJYhEsK68T3zkS8MbVaGi3D69awhTBERGrlaQx7sPukgil7xpivjSIPTOuV9exF4kEQi/+MXerAxXakNHkiS4u6Nz8mWAH/lkOmGqeLgynTpYEsKQs0m3evOCBVBb42LKn05rvcndnU/PUaUv4FlsOl7KfdIGy5BptiaXL6XIA8fuBTjwSY3Vo8V45OgoWGWwM6zn/EXgXnGivTIODk/GeKbXz1gfow8DSIFJC0IVUThOkKO1uQIW5LztDJYimaWjndN1mzEtgoVs2j14R8umEegZtE6jxBAVWPTZMsLal8ptsPHPrK+wyAQZbb/kPiHnRiGQoDEd3NoHfmriuI1vklV8SvAQ/AexpfrPFtA5bcVJ7KeSHxohNtRSqY208izqfF2Hi6gIwHcTTygIh1L9Z9+rn3ps8hGLWTND7TBDAGaKJyKH+biAQSTF7uH1U6nhz3CxrCz3btd3cVdcus4dYpOmw+BwO4s0ZMvQOMhI6U68AewB8HRdpDyW/oT2Z753kEezAhwl7n8U51hJpjeGRqvHDIgkBTZ+yiqmiv5rFXAy12fvMFxl35GmACFgOo2T+G0fkuB9tbMEoU5AYzpVWm9NWGn4M80fAcaxwiALra5UC4TlaVcy+OBky3DLJBOL2jtJ4hL2hrB/h/CPIX+OsIK6q+gQFfwpLPsi4jePZZn4uHmUpnYq2j2Y680x48kjNC8OgGHZwuX/N/7R6pxaEhC1klH7NYkNf3Zwbepw5yteylErIBNsSHrB1iHbzCXiaxVVXpwct8IiikDUq4ivnUvEhTgkd9fhR9/eL6kp4Vo1CPzc99djL2kEu/szi29JUo60IrKTzEADpDK67/ppk3J87gh0/DSFxPnFrNWHahT91pVMbaikNKWg+zFDlysGDQv2UhCUE7ah8zRYxnQ/rkd4vBd/oZeDD1mNxk9Y4N8VsaWeqXPk2yiNXdq32NyO8uMnER8L1p2s1MVdVvv/B/hQqddoRVwWsM/jwv9P2utqZbiTz5OPgONqybD9DdNlqW7nnktcaDfcMFmVcumWV/nZTPhOTkqt49jTPErZeadC0Ee+GwU47MpCIETXLS2ccXYNWdbA2nCsSqtcQEy+jCRxC6kdU5M9kQ/cY82aMGpD3R0SKT/cNK3sFmReuThpaOUjckCPHVANFjSB1xsqNpa8SkkLqXKPsltgvFv8Gy5nHTFwI7yD8efBR2EMOq+Ny8+8c0Ek5Tpi014coZoMjT7LScS+sKBlwnEgOy/tQYBr4NPwxeNGXJhY1SaLAp4zWoxif8agSn2lDlfdkU0lOguq2FEbkF2hvBaU7D9/NuDf5ELl8+3+rJikoZYVAxXmj/omNObUUqmNtRSnZ+iwmm9r3CX6dDOOdmanMr30CCRkaN4q6d0eBzL254JtqJ//96zvUa132wk09CJ538oxyf9wCxtz8CIFa71q2nlnF0xGXuio3tS9QJUehwDU+7eLeEeKliqC2rJM/N47ksISR6GnAmaM+T7XywvTOJGVQYXMCdX3RI8eiJVUegtPyTVqDZCfmHQhgYokmkXUbVhxzOn1a9K5DAIuGSQ03nAu1xnWn423bvcU91yk16SaA90K0WJWr+Jg2sn3bDmLiaB9VpmV5WO+UI+Mw8L5d6gqdNAXdSeoy1Bpai77+IJRuTM1e40ruJqpROE6ruwqDR5PK4CTiFollrmjWvyZDZPEilGt/VwVc6huyn2sXQRU8SQysEoEbisVIspWCDE0KoE7eObVXXOV9kfYAc5Z2XItFUVNbxFoKoS2iMxhPI4K+tOtKpeyHA3mTcbW0v7n75rxOHecDAS4ZrrTHHJJJMkMvG2oo9ZZhO9LHLqB6yudALLUMm22pZYiM6Ctq9P2lG7SCWhO0eJRJ+HT62tiXrSfGVskQzuGWBCZ+MDe2K3s5uRI3RrWcvQrpbF/M+Vd4mNhxsxz3X5gHnMewzBJFftZLjzGeO/m4OEjjQf25VrtRA4T4HgCJ0ndHHJQY7lnwxD3HNY0W3rD2cS1O9TpIIiNQMVO4xwlApn2KA5DkTJCl/EyEGeW/Zf5uPm4Ug1Rk8KxAavMY5wnPpuG/+VQ0vZvEcbjPGnNCm9xQW12ewL0CQmbSC6oNIhIp/kn7sM/5NiL/9LUh3c/6Ie1aE5fhctWTzUiBJR3KMfq8LB8aGYu8AOMjFSy3oMIA3Txr4GtvTGK6d/AtML5Pu8HR/uAXynHEjxxe3HoRSQ6nUMUjJYZq98Xf6BcKQaxMkFhNGsEmkRoBjQbk/Mgb83o32j1/VQ9b7pPE7a646yX+DeheozaRiw+s6gfxRx16HOmDxK1tcbo/G/eozVJkhl421FHqQK9BO562DlPnnLVLfhkqrKoOXWfUzcu5ouTDORWQaN4KsQdB9EecI4Ff6WsihffbPyt7hs+p/j2qVdUArzS6PcYel0ZvROp8DFKDNLxKcOKqFUmaCSTJDLxtqKVTG2opVMbd+E/5BlalF2s+xSSTJDLyW2IM1STAJKOJ0PHKu4rPdRtOxgtsl1gog9U2t+r6relBPqGlTGzuq/RYDofbgoRs1fxrEmELoJBRSd+RLtma/MqoNri/oZeNtRSqYx9tQ5RknbuG1Go/p3TcYwM/CsUcxgnfr301KjBSyTknuRoJzAAKPPffhtQ3NO7hWl7LwknRGuKCjKXAvm8OhAC/87qfL3vH3ECSZIZeNtRSqY21FInInKpjbUUqmNtRSqY21FKpjbUUqmMhoUyKfWx5gIKJuFwSu846TRJDT2qdOdfNT68Lo+hxS1fD9Ksi6CZZ/BqMQzJ/ru9GBUHXRTy5nQlp+D2op8BiH0V/xkKnVnRUZeGxC2U4KY21FKpjbUUtUxYUb9JJhyWCuomEG7wSTopqrZ9lnheyw9mZ2mDhWRwQSBaAhJdPHcxXkpqdu/JjQ66LTyaXsbailUvmEm3g5fqjAryplIW8ss3bksIdck8sF8ObNiEsFEbKx67/aTrHVebMqiDqHBPGUllPNirpCIpK0zWB1TdG8aQMn9fVs/KfpIZeNtRSqJHfzi8EpCB7sTjETc7+E+YzwF8pMnfZFndQnwhlPlEMZNXQ9V2SrdxDS2cf694bBajrVUCEsomXScZV7Vc8OgJ/iWGrhpqYXCmO6+ucNFe95ZBIZeNtRSqY21FHoVKql/AngDniUXATZpCr5WezSIm24DlZsq8gsQPOvbEePyg0Q6afcCCuDUr15UTFQk5K/QziWSvcqvDCWyOCECZOFQ2L+C88UmuPiIcmMmTVmd5/KZ47akJg2+NtS0QP7KeZEtpvsW4emnapmGf5KrWzS9TYZo5hqyZgTyWNec75cuxsx1XuulUaYAhzUJZET88DLubRh9RgepSCqfoQvpsAp+2m1M5nUuc6sK9JEXA9bY8OjSUGLA0Vi3SlFtxGYQBaTdPThE+y2EgtNCT9Dm2jJiXIrWw3iWqMREqOqSvkJTwih2UZWGwmZTR1r98x5C57kYFCtSFH37lTvDUCPHzYcPAjSbWr9VTMIArhR9ZyruTMbrg4KHuYyZDAtuoK7fnjm9faWPciAHoZeNtRSqY21FLV8pzVRax0w84TPHbHPm22tQ+1MLUXMzKAhUVkC707sY4SkQDvvqaFh0Ny2UmKyd4heF2SvqlKeEos4GckfH7VYqbMVeutIxO1Tvt60xftms9iNO1M+/dGRtqc8e7W4D/Jp7d4nWwSc47NItIK7YFgHgGfHKBBKFyZxoAZDJCitqS/NJyAt/4Ur8GDkTs8UmQCARbL+UEamuI8Sdi45fTXt2uw9ZR00b1a5YZi+yYqpIV9qAcQQCX2Wm8dHnF3ASUVhDQjVXaExbpyTOU0WBA8r5CkeqSSNtkPFqL017+j2gBl+kfQeAQ+aqJVXo7BAKi8bVgv+7Ybl+urLbdjwfIhxcfzAeYrO/oO/6tl6nGTxIcZFE8aohOju+qd34Mzpx8cR+FX2I3eQBfwCakvZgEkyQy8bailUxwoQpzuoZCnxZzLXGuuhz/EeZecHCOthaoKnyyMg756/3XHWlUxtqJP4ws9wUWKcdWlB+0h5TvBXpTdlpJiAaLf546DdQn/y2aAhzETv6KNi1NdTADDaw62yYzc+8DHPGSzD4dLcPEADZKYtYS1hNoAn/cTsvfAzy6eulVXCefHsx5wf6ru/AsQE1ZDxT0/gX507bAN420asatoarKOzkaKmS1+je1MZy46uxDm99ND6+2xzPFZgNWyTSGqBC3oQFQSTJDLxtqKrKzbUUghXziwJ6VhQreAnkMPz8Q+lUxtqKVTG2opVMbaiRZ5V+OqeiSND53omE6owoUkPjbUUqmNtRSqY21FKpjbXDeTG2opVMbailUxtqKVTG2opVMbailUxtqKVTkoah5NEYYHSfAAZqkyQy8bailUxtqKVTG2opVMbailUxtqKVTG2opVMbailUxtqKVTG2opVMbailUxtqKVTG2opVMbailUxtp7rHTAmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbUUqmNtRSqY21FKpjbMgAP7/A+gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1w3QLkhoIUHgMMMMFA8QoAAYwVB2QAAAAG7s+//CsLZ8H7vvX0k4xT+N8ZPlaMb9ltot3XQUL3+a0uhVXuUH+BbMk7F20C2Rr7F7AIItk6UAAAAAAAN4cDcV/BafvzsIF7l049f8wdT1oewOJMQMOc6Os4uevJyPDKZC8KqTWvF0jI0wlr19cHqusbthe1SofrxlPNiYj50SobnIz/JcV0YMmlngJAAAAAAAnVHBWilE50Jq0+96pDH46XffQSp8dM8LqsRpgT14A7Z3Uo4fsNykJr7ZcxIcEEIdtTrj102P1h6aiUo+yXZb72vY5q7dfeUE34AG2DeSovdR43Cvil+PdoGfmS/B1D8AOt/QMGV9Dv3AQQzm7fsblJIrL3HW3pSAUhuEQdhoXGUFfhJiu5u9wAAExZAAADC6xJsGA+uws03va+tmVwP7LxhJyMFu9jDROwNhMLHUBblUQp7orGjZzbfo60zmurmWc3MJqa/bZmP+PtERo2OF0Wgwx9KsD+jpUTzmsYNImUxKy9F+kn7SZx3D4SfjsX8c1EVzOZmbsPGXfMKR+k/8CVWnP9KtsQNEyJCJZlLOG52ufKWXKt/qDenTLev8LB394B4u5NvcgAsNT+BWvVulTM4mevkctLd9RwqSwOWrqU1ilajt45gwpCwUNg/D4nAfMyjvdhdTYargb39g5Ipg8d4SjwpxoAgV/3LiYG3oqIIAkl/Pq7gn9tXhfv9hyV/zvc63PpDBNvv2hG1UUZv2LNuBaJyiJTXlCAs/BHPTQIDWWZuKir+xxnRotFl9lWT8oU3nf/WH4kr2XhswkcA6zIrSQOVCx8Grki4Y+UPCH8/3hXkPFtWkT8MZDGPSg+OPuP2LvYmFRC3vEPvP57MFivshNabY5PSsR49e61tvVq3dVorp9ndkaVlRDkAADdzzWpqK4G5PxDascjNip4g58rb9DK+fsoDx2wSHlTm9JQ0IuSscKyJRmP09fwByEGWOE7uC3MX/Dbi+xAwhyxBWwLs5ByDVIeatOLSppEOAiOb/orGIR/3zZMnXMAaiHJzRYyNR8crApYo9ozTGGMSfZ26p/8jdgUjz1aM1KJHzijf4jROybTR4i4jz9im3QAGkHY38ecovgRb81s86rVh3M2TsYqpKQPS2e6678HwfQfqAfXJo3Qnq1XbJhLdjnrYLG4LydoeyFU0e0aJiPh1dIx0bQ1Ha/Bz6JeuRhSODLPwF302MdS2S4TZjN4UivrUeNKpXCaAG5ExAaUtW4LDKwkD/eAr2w70/v7Y8wYpHWn9E0WZz5NYZblB3vKcuZ1DapzDQCj3zBPPsR5b+k/UaxDXlBMFuocNe8fXa3kQ5LQnxXDAEJlwm0jdF4okdlit+/maJfYhVpCfn5D2/W2zGgOShvXGKM97YFI1wALu7GpsOW/QRgHrBljYGgFH3fejk11BFXs/gcjdj9jgfT3dZl6y1oH6E17larRxY5JdFf+IqBY17UfDCmkW5AFc05e3Kw7a6hgLiJIfpeNki8NXULIHS02v4G72/+PHty9xSMvThGTJBBiGkAiwqgYQpElipJ0+EVgXYPUKtFKbGqFK9VaYKRYcClOka6LRqW0Cj5X3ZRFyFvlt5ecj1h7lxoLjEDc3s2R2FC+2zV6AzYZM+WlKiouGRrUxap+CHAKskaEeDl1ZV1bBhsp7N39GNp3TsoTphAwtACm3j7itGo9oheXm8DW6R1wI+hVD+yXLXCSzyw+egvmdWQ4cH8YeVPVkV7XgqtVqW6zCtDxEPv/ZzubakQOs1ZPINqrrjk3ql2Q9fxmbk6gSHCCE0pvyeDwWPfLcqb/Cm/HfGZNJgGqVLrINTP4jwJaACVdpinXTXMbVuUMkzOBRORYFY/TFoVGmUgmfpkNYjPRtX00j3UeLt+eY7Z8M8/ZrDdtWujVub8R/3lii8wnhKZDSRS0oB8yIY9IerkPxFz80PRiWtgn2Bymlj9e6ZE69n22iSaciNcUFM8qqBOWbNc9LM24fOK3tlg9q6TVKhw4pGfynnOdkDtQDdsaqNjD5W7Q3ybhv7XMt37yNzEbX7ukDoi1kgH0ptxVS6Ct+kD66mhGsqvp7OcmhuQsPdusuP/XUMvl4zxirXRHtLo8K675QWbPynH7kVkykO0ULBP8UI5aJyCuzhjeaJV0yyDCxvDXzY30BOaIcGiE+tzR1t2J7IHT2GDzl+NVerjl4q8uSTQmt8H+qcr5DEuNbNN27QpB+shEBkfj9AA91Uux5K6MwSOPTzVG43zOgDysxC6BRjuHp8Do+lWdR+G9b0voFQqfy4AASflTO/lImnVcmH5adDZiaGH5C38cIGyUwJTjzX77h+QE5NhDGya63W64UCUIaFYSorVZswOMUfWrT5TalC4NxjSZrBXWZTnhrXo5Kl0VYu1gmCAtdeUE4cSAVYEwmnv+36BBpMopFEjLFCUFHa7ZoHdbG2DO4GuXhaIhBkKkbGwhbIxomHvO8FovQdyyEHIIzw2TusDa5bamLwZTEVrOn4LpRZdRLmsApwoDu01zAxDyjQzo1vvtz4Dj0IkCVpqizKV5P7iXzD2llBA7AMRS9IYXltSiEiLbqOtT1yLSDGofzYqvUyDYBO6OhQ47K6nd/8PaRGbeDy4CU/av7p+Qx+wgaoXX1071V6SYHeeGgD39xhIFPI3Ihg/rOrX+8OJ5I0vYzrGEAhAglwyk0E6K9l1LPcSU/WM7Xt9aVMEVEi33FFaih//a4j0nQGafCwesQWQVDCL5kJ0KZK4H9AEh2QrFKG3lSvXefMV2N2tddt2WBNrKEQpRJjNQQ7Gin6vblPZpDJ5WsA9jaPTxMzwUTTWKytLpcboQmGseqCvp3AOmY82iKoXCHzpReq1y7J5iKlpBfHWDoNEtDauunXBcNyGEeNmW1zDewOU+o8PU0jBUhsvfBrxMxM5QaGnmGGCcfqkKJ0y7RwBFO/1qtSk99LjEvb/1B8uWxG9Yk/Ter4AGh2iZkiyj/33HsQ0pomd72hBv12M9BvAyA9rh3CtmPQVTyq73IjFAJwoA0twKAMWM95jYr24J0UWzx0AyaFSXPsnG1SXZwYskJwX7CYN1YO+A/+HOSxxAFBkmwIV9HAb1w2DYu0a/VDA6hUFItBi8rhnmG3rBho1p6BdMbYuG0Jp58MdrvYaAvLtiXPjwl7QJtbUFuBajZWh8omHbIsAdRIl7sw8+dNOLNpm14491jK/ALLELZwLDNr3ngJkELQZ5lAfyUqOdjkXb/EmhlgCiOSy8z7zoxRFkCWx8PpDzsrUnu2JWwCUlKyZsVKVedgrwgH6Q1Lmtf8yCph0IJkQUgBNXJyklI6wUM/1D0yctnpDIrNNP0fMCfegmbnS9OJBpbZkfeHJedTfEPTM8YhhO5OOYIXL2psGfZlalbHry72T3LohrgSqjlLoeVtYGNNpafexLo3TJ/jFmkcvDbhS3w2XBkxDAAG/42U0Y2JoBh2crl/IZx+pE3/WxqhZK+5ASd+K81fSk5RDvJ+aRzvIgFUgg295DZ9f9dPjksqNEAvxWYTavnfJQshIrDXFDunYpbLGLwvJ3mKp0/XQU7CX+CvrG7b2IX2Q0sPOdl8c+YVu+0W0CskVcYykUSTS+jR66n3Yx8KssFRVd185r579ZPatp3sluatQOcajJoqzXLTwLsJkxCPBzmn/BR9zxJgFNjfYLo5Q8KDWPzVpNCjn0Ryjn4oYAb+kvcrZ23BWaqg3Eco5syTi/EZdcRaDMssHX2jK25IhXSMGc550JbdU7t/sXRauVeLFMXNN6Rh1eS4NSQsmaUxiuITcPoDI8MeKZ5f5SlAYQAghJ97jYyqxBOxds4hyrfcyyPb7IhFrLP3/gSuJsAIUNr3UaQzN1d2EvIkQjbwtxkHyJEqpCf6HlrPQcP0S1O4kc/RZ96PZ9fu7z94HZL73QmiQBAnVCpYWDb77IfH4CieuLC1gCwEqiygxNlYZerBS83upbjOHewnvmswlHFLuURcr0r2QJHwdhNMiWo+6h5aJ6jOm1ZC8MCc7TNx814HLqphkudATM/4qMUAdnaJxlE8C0zCyvzEMTm9xj/X9O/vtBXITe43kEthu8/DUdfF2NE6DpMElZd44U7/937di+tllJ/q+I5nbiQzZP9YApuqN93MSlym4ackizZrOmdXUFEeNTDczhxIx6Y1cfTAHgXC17KjYZYIj3tl31xCFSubkbvfTObVSloop2uilfZDjvKtL9vJbBdWO7OIzFQXlNx+rv/VjjmZzQASXlEQQPGSPk5GomX5FdF0/eYMfvhXJIpJiSIpF29dfgj7ZxkBUwOYM7Qob0BHjxC6AeiYRC9u9pGaepLvkG0p6dIG+3Qthllr08isdUmUN5qNO6ox5rRjScUzXQNSive+VbMagZkNL/zfCTJK2ifqyeM6M5+pqnvD5Q1Lv9Tj8wuJcWr1HvJzg93tHxFpgpKyQ+4Dn55bb+M1x7/7xMX9ZIdNRtrxwhOJhtcftbokeHsS1XS2z2CpZ4/8QnE8hmqMlpNTxc8lZVSLf0D05E8eJBOIF6c5T8v1vmw4L7QA1skrEE91iTKsxKnRsh5/Y8Tu62qQI4R/WoY73z2qwzyf7SQsKFdc8gVXV4kLMpWso/Dr5SoAeodDf5QdDQ34u5lzP/Gne2rjtUF4P1yPQK2XNloFi1HqoiPcQ+Os73Ml8hxSks2bulpMKEORKcCQh02SIn4c4QO/v0Wi6A7I5ThoVW2XmxKiCNvCO/9a7Q0ZbFyZJXccj1BUsxn4QrB8j0KVXY/LbY5w7EJKEs7H0GiffVPOtxp85jO3DT/8xjIBwQ2vrlGu98XSRnFqCt6pw27KUTWmb4oKI53C3VlJpt4G9lUFz2UPiMlIKwtOTLiVv82zBiov7UgEp7MljxTgAToPM78gMLKhpEAd9xoEwzo8Cc6wAG+9swnd/eAAaCrm5PGpyoIlbDnbOBJ8PINW9qFscfqwwNt1thoaavlV05vHefimAzoRfZeCcr/xctTiKYxPWya8gPXuTsNwhKGq3nKGibjMRzatiw7PY3gRfL7C5kxEVwV+ZbQNWdwyRJow26ZVdIYF7RRhVwfKDIOvu3af9VrNH7q7KvOjWNQqxBhqc+kdNSo4vn2L2G8cc36Jz8ScZSk1ZAcx0V2BzZslmE0bfmlsVILd9d/XF4TfLLN3nfa3Edyj0Ftd3WZM4dupC49qkWba8H6aJL0X5h5HkzZVPp7lccH+YTraI20TmqMuWiPWIJrvAACekzlkY9Ka5PY+xZCf4OucjyGy5hDL0EG4qCJgAEs8Rd9w+hIIC+iNadZBIndaNQnIsorMjnWSpNDkCgvNd3yND4N7ygtsAvJrpEF3SgeY7FGnkja01yC4xTgCF7Y11J5DTuWjHucaTcmO47nS+wCaFxsymH8GHWe5OyLpKdt3pIqyLaYwAooHLgpasc8tyOAsDdatvVaeZo3cUmGIYvjHUR1FrCC9/GdczPyuX69TqzaTi0nVSakfxv2vcc/YXa9ylNnS4SVCE9/BbK84zKeG0s7Z8LgsnZ0HpyzQFCWEg4kyGu66Ja5GF0eiFkO9nRNUIZ2i3Y8LtaKBxbSOuF/NXkKl7cTYLFY6Kax7DvTWe4qfPUjjrgrtRtF7bku/yisa8651nFHYSpPonOWFu3213tMBfkmzrD9zmrI0jeYpDOETyp126O0EAR8gtN3bKPhC+OLbTw4FvMK7YNhahZSA+EK7gGmaWPMY9cB18AeyORWauSpJ5fiCyIdelLZWwQcvt+xVrpd1ho65ww9F+RmJS/hbnG/E1Tmvhb3fSF+N+RPKkRdGuRiqT1dxYxlxYcmIecuhQfNBtJwnT32YQL7IoDLDBr2FTG5X6f9jLpVnVcaPMAae9qBPoGs04ELRMLt04f7NTqlFxgsWs+h+9kCwZsWCEH9/HuLpXhO3HEy04/uRC7056qd/EGMqndgJ4Zl9F7pRV6T/V6DjJtmesef5DZwXSll+UEVXI/eltrw/z/0Y2Kdf/a0Ybeg3SozYJH84T4s1Jjb5OarptdLRhFl0FoXA+aXScLrUfV2VHZ8q7zz0uQ08Z8s8f82kGXI+PXJ1IbiX/k2dkserfg505R7MS4F3nU5m+OdhsejehlsHJd+wZmCzpjHCdjCwAlSNxuPj23FwqB4gLoInNpw4dsWvP/qmbw0Uk9u5Yyz32Z+gnmW9x8DZpGzKFVAAsosmBaz29qzFkO1PZpEhh/1UNqH0NVaSmb8P+hfnzLo45jMv0OD2YzaGshaDepUlGXRr51eBruj9C6bPU2Rq0U/nfkeFgC4Yn6d7ij2xgkUmBgB2aqilBGZLjuK2f6qmpLncxlvIaTPlPf3PvdYFYjmVL4LFvOdGt4eB/hjdD9o2PuSF6Vg5/zt3TrunuEnoxzO+tKhQdmMVXw189gDvE8tnQ7wo3Tc5s5Jbkd5IhooxQcP9f7iLh5S1rPfeFHXtS213KX4DTUP+jhsF2//s4x2+UdeBdE+R2RCDbcYvliGiyP/FI46pPBIKYbJ2ZGE5oTt5X7hLjzBxNqxVI4pehuJW/iGbwsFoBWF0qfKe0UHeQ+oVsBMnFheq8UaNCdVo9d5W1ysmXvUXil0pstLRQUXFS0AImESiVPcwfXiheoh/t/8snepshXL789YPsJUdNc3Sys9BAkzVd97EayvVJeVN7xp9rt4sAkacLK6wEABCFB+UywfAawoipeG0WWXp2DNpaPo78WVwoFe4Ce8Kj9gEGT1Sgb5CVsCLyf0a0vHp6ErxjJNdcGDWvW29AoIQKYF79tliTrb8WWrHWzo8ZuhlkA8t5kyxWhqNBCwa1h/M7iRneNFypqO3g/fgZmowfhfhHJ1tEZpn21z7/KRfOqAlFd34kPve3a5gMioe2SEihwUhEsHbTRVU6IcisQHJoUBqWlONZpkXEYMhNWvo4QLpOLmt19t9bZ1yYnmqLaJsJcl56oKdz82v/uuAiFV6hrMOPwZ0BqhFl0gtyr5auVhwafzsRqjv4auPAEGGBUUrlUz+Ik7NhCx9RapeZDZ66sJY+JfTRFyUuDNf0DdgFDX/DyjZPuwaZ08TuFyug4zVFKE7tomk1fB6GYxQT85QKi7RL1+pXHNabCxsYB9BEGsJ6Gt9LyzwL1WPyAD5703Z/lx6Al+6L4lubWjWq9NqAHTAvUZdus1Nkw+eCCkjKGaNo5GpEftnBEjvOmMAWTwu348q+sNLTypXJaynZ1N6nXFoJLWbYLZURyrELybceUJjx8JjNJmFxtCBhtweZRaUYbhnitvaqiBdSCbtP5Cdecd+8J+trD89x3j9SKoDDN6pEjF4h4IgLHRapqv9H4YT0i87xU6Qpse3Uei9/vyCJcd+mBKUVh3oSJdQ7iBJJtqB9DTLwmuBYaOc2AZKGBHyTikbmWjZA3+2fso/KkMJ/YtbQqmw2inPKPHca/xa79s1Iqn6R4jI1F/HIe8SyYRGSZ+SK8dWrJ4ghIVO+xqKlayJp+Bdo/97dIYi6lhrd/ZJkIAHamIIvJANlkEUmxax9ykAVB2jdyoEq9fCdVTN31Mvdlt9PXIBzA7dHoN7cM+ApyZypq/khZuQEoAynb0l0wpJHnImTPH7MIXOz7o3NHpGbqYnNGI4gqgM1N1Lq3VvRhUGFI6B7XiYmiYELLgOjCD6BuLPUiUE4L14IEnr2Buml6uYMvVr0CAbQl+TZN+GQKU6wtRH28fnVNBOr7DevbgZR9CEx1QR0C27wuanV8qwOSkLUkp9wFdZ4ehpa90EDLjACp6QKpoWN1ApG1mdm3FkOskpzBhGcE8aTG08t4o8t4cYS3X/FXJMBx0y4Nv+U6YAY0EFYSzayN7wngFGhHcnSRl1tH4M6fdMzfTBXIMK7oLOz5VquLliH7EdKBtuPYLuULYALLAkxD9fka3l8kaGprGAP7nEexPc8o16SUzLXoPyWeoDoiw1vAOg1F8+lYES1bGI2Mu668pi7pxU/Cky+9K1SYjYZdVNHlzKAAc6oQciHZrbRD5IDCHSWeYP2b20iwaMwSKv4CXXrDWSxmnnl7HPT7z4V3By/M6JmJhXXsWYrH/riiwGz4nyMG72xKo+Ws6YsKkZ8sPWJ2+M1utIvKAjG4HuReX7AfHOnH+pI/7lQuvM8hJB2KRQthBSE+2kHSs37RiRVUV8tS7LWHOI0QuOHa5GKSgSMZOQtqRZ/jjXE95Kri3f52rT7E1K3rDXku/2FZNFnHxFk93JyjY98rleCmLJL5edpx66NLiq+nfHQJpK4hXbol5f3N1OZ2Oj5Va1Kx8wtZD+gi6G0gcmcKL9p4DXCVFd7uLS4ZXKHlta3DErt1R+fb5Uay4GILkhYwTf4wWFXD4ZoFTEOdpG7fYCJnTYzxoBfzswMyUDF9p7ZuApy5zul5bAcTwpeenZ2zxQD0iFCqbwJgx8lJT54DOPqEtVxMZmIBqMv+uiGLumJGoZaJIqMoS7dUYrEVmLamiqvC8cNbjfzj9BTzfTsAVDa2ykL6eQ+hN9DYtd7Y0dVM9rTvEZCIVLhwmvj5BaVTeFN9Yf5dO5RiiSMB5GjWPJCEKGwNRKpqK7lXOAP52kFhkMzDqmBxqy9CYKOii1HIxVoz2PeMFkCOsDbSC15w+SPV/UpVThNCWwN8thjoGJFRG8FinU4pM6/4DWHSIA0hzYtxH7pTt0NN6IDkFHfwcM9gsaJ0a09s8Y5BF/wU++uFxb25IC4k8jytv4SaqCeW4RZqQTPcfZEvW0J1wjTu8C7aEZ+emwSKirOsSu54c8Tp52msNJ2RB1zn1+z/wqwldh9aaflrt/vp0oJ+kk99mNNyembLQv9BOHPGB278Eq1URDTJtpM8q4cl6OLBWX21uDvdv/RNtH3981XA7CTOANhen6qkK/OL6eg09YqC3GidV+hsRfZ6MpbxrqeCbCB++KrFIDQ3HiPQu/I6ZlTyjmapl21U8ARV0kSkzs/s2SDEbrtoWXFlXB+/Y/gmGtfvLj0lL+tSjV2b1cfEZU+RWfq2sO61p6NAqUEeXwim8MqOQ9RxmtaavwtrUH5buviwyyiBRuaWAkVVT7BDE4MEgdpLvhPdCxkp7S2CH3tIeNHPnGboHW3x0DkZFKIuIcr+i8jO5eOWD4nFSYBmBdvk1gSQn0BkNAZ+5KUzer6s49Cmqd3muzvkX00QpMFvTvX4cIIn+o/MG3Y7v9ayPPERanFz+d9LXw66Ia/wc3IULejTTHz444OycZkAjRasmgNu6mRPXtVnEh99xzHWZZggGbzZPHCGopJ9r/YK76oot0gfTTY9XXPflAmCl6keCCsYW2Ws+4Ss6OD9EPdCcsYqzRQPKZ5zOtNku27jv8rofABnMSt7s9agOs04e7ZYX5/kBJxzg9hgCsZIEx7j+Xm13oo4Mq26D0tCUNzI+UclNZpsN+Bqo39YsBzIeSUHA7KmZPuQV2oi0a8jrLmsOb7jiku4MkkMUd3Va6Gl5zobjNlBwnLTtShdvJvmnPoWP+DwGAuV6IuKD7QgB26JpCPEbC46S3PAHD7YcMEYzlsONHg3xZdZp+ElBB9d3SV+vtnbx2LIPmFlzdmfqNFPYAAy7u/5TTef/Tl/P4tFOmVNGAwUMqjetnnxg1TEUPPWXRohb2oPptD+6cUsm+IkSrJf9OtGnLqssK2dUV2eYfl4ipVapsoBKdFQOKP/zeH13E6SPVpQlGeWcE0wuD6iasGtYqEFSp4+/RNK16A7OXBZ7KMQzCqy+juIazjGtxfarBxlFk/hv/mfWM9blZaIMLg41+YzYQmT/gol8rpA9b/lFRRiKI9/K5pIaWbWU9xfsQa6Vqsb1wxqxvlLJG4Ry2tR2wwA/RppWUJ6tYt71K4+5pHFccZjNlKdKOqfDpd+rgp7ezoa2PxcImPoVH9Qni0KJVsaZtDqVZIwV2/ZpNs8Yzh2ndEgXt7lwDzCSbQG4DVnD8gNVuCOTDUvjsa3Xpq7TmzsVmRB42qqR14NUpXLfur3jjPjJhIf+OEIFIS0dnCC1z5nN75MA1R6EZk3Im2Y0h8fGp46XOgK7MwWkzieP6qcTUWmbExX6WsUVR3DGdxTpH2rKjTR8mGi7Wi0B72x9JrPA4vuKmfc1IpnV4hUiv1nxJH7WDZtukTebM0NmVy34PPmEi5m77zxUlmG/W6EHjfxppK4uifBPhKsC3US9/OWtdfoGT53a/OqtiR+60VYg7JcCRP9vESlH/cZXqpZ34qOJkOjMBgepE8JroXGcniDH7xH3eUkU39zFkTBJXxvnKmgl7MihXrKYBV9fOD7XxJg/yYvjF7CaMQW+m6S5/6XVTmLXtqOTjLNCyNHEIXgXQSv1dlkoI4l8oev2H8DXnLs20s8vUPD96yJAYUBW+qRRpzBt6GewgcIm4OJnW8CfW4RVGza3vNlwF1Gjbag36yk4xqSvapwygEaEYyRqaMIl+wiLF4pH+eKG4gbW/xcjz2ziKuy9f0Tlv6bL7Xcfj+4xwtnAmYxxF+Xsr6lar6qVmMKE7tqR2LgDZ/Hutj+jvmLfvSi/JZfI/6vSvqaNb4DQeh/iIDf/CH9NboQ4svmY21FA8Qo6vDR0EnaXDcrXQUDkLn1L9rj9oXX36RVaDkQJjUpsJePYzM+/eTpSPJWXLp6HQy3RRAjtpDs66rKpiaPucXDmlRawnh7tSi/VJz0XAEEidvQew8N4xYjvBwpXv/Z+VAA6nnZIEFR0vrVW/7R54ScdIOcAmpDfqnKu4EzgrhSNFkFoO7pkrCYGYGPxWPtQJIIkxFgflp9XCSSBYTZ8yR+oMLr4M1BcPB4gCWU8CZ9PYtOYqWs20TUyMBwhQMFzM/rwEYXX88aV21pmHYb/6nbCLsya0zpamSlssNXDaZavGJhGnmCnWSIKMUscKerrwX4M/ZjzrYfJ8AfRK0W34iCmgdAU6tLpdk+7ZrOenlfJVaJXUO5TRSTeW/65vvgwLDYbPYEKHwBwqSwY2o/ArIZGOzzrjMl6z2TN/B//e+vVclsxJ8ikI1phJl4nJSdLeetyNMp0DKu0Uxv4rbb6ctPGp/0W1fJnKK9Bjb7h+8+tL6Ybvp3Q8f4kv9+HcEAV1Ou9uz5nPueYDF8120VDGF79YayGDpejpF9PJl2uAofutil/iYwlDkIk1L2XEYm77IEEfCjv1EzTw0wYTDOmr02xIvB60oQ/uKqdk0AU92rwP+nRNSwehjzu1+aRK1Fg5AAKcIqRi/AC75tp2411OtTJVOKMON7Yt77BulAEVaYAhiYgdhlE5HZYa8NHmzY0BACX3WWAZzqdjV3acB6MlC2x5Zo87BOShIWszsIzXYgprgrvxeJQioWCKd58ZQQ3Jz4Z8DfoYXIp0C4G1ABwebPvvXkqcDVvpeCdOL+usvbiCshcFemuLWkBIasEBkkNMjMlhr7piYSYY+HE08twSItj6Ztio0CE1h5gOn8WMjQTClL6TCUNXmU3u/AWJboodBDMB6/RUWj4HG+J4ScPvXq91E+aUP60w3GMSoaQexDRXibwad0K3INtp1IQJ4lJyNMQ74I6Wf2TEoxpytAUgEsNsoWquY0DobFVVvcd0ZDq9EWXTONuxsKrmF8OhsLUSYfeoNNOUmg5Pg/JUyvfnjOML73W9OARE2P961uggnjWLUDTxigWYT8XbPBYkrkV3P4R/E6+htmtcbjvCYgeq70lL//Ui4nbOJMStvjPcKl3F+NrXGOsVcATeA41LwIiAIytsbLUh0r7it7awwxgL1iMRSyuaXOHwBENo6kurovWsDrNzX06aBiTvGOdVtWcmvui7cYV0ydivZC3PW/7eTZFLvOCe0NSPwijC+xnn1vj8iqWlEfiS/Cr65uqY4JeSamQY+QNgpFHIgPHPteFCIBX9FPC7ESR+RfEFlzifPIjqac19x1F4sX2kJCJtj3KYdnuvvv9+WVT6Iqv62f1m6N7Y2cNvDn32mK8x1v63WFUKnMPL8eYmgP3QyfoWKO6+o4mawSMZEURIo+w974UnROwBuQG8YpDFM5Y/uHxwpB2Ftb+FuA0Rsjy3f0xzYY8N4by6V5X1RBFDQWyiN7WtR10qxiQeU60v4MplAxiJgBrRDJKvqQaf95ZiKwOA2lcprP3Lejzq3OIMtgWKVmI+d6YwHPPJQWiu5+z4J3C/4PyRz6+4oBjCYNSmEMbfetmaR0XlHWv/D2FwEIgXsTppF0sg8BLbLgH6y0nPzhagPC9HcF1uzN4G4BwhqGg/JT2Pk7KHgPy9zvv6jvvhfWxatVadNOWMF9NVYYg0iRANonSTVlCxmRAO5MNPkuDVKjNt+jAmgASh8ufq/fEwi/6SrLbACnG+wXmdksJBIyoJTXbGLco053NYGVcp5PnQPXcYUnXxo4MBifAO+z5TMU5xt/lKpXG00Ik/YNfCbDf4TfE9G8BNwK59j5q1dL4JBeOCkS3jcohdLEPxX9A6ftcITJH5StySvxqXNjReG0k0T2WNKdi8UTTmPPbQxUtff2ZAZzmM7FAkMIg1FNzzYPOFwNyozdSJHVJvPTqJzlx8HgvV7x9TsMO/3vaepcwBYrz+6KXhEXvbQA/AsDVP00CQ6wD+zrVWlT7DXlfKbWVt82LYRpz5zWtE71fDxZ2Ph+qBD7ixv+uSrjJ6N0Sdd/7eEjvDNmKsEOYlFzTl41+5gJEg2nRP8/SZpyWFwy7ReyfOsddBQoiNFIV1INT63Uyh+3untsOMGw7sdU0C/VqKv1t9DdRyMbRABVW5p0WK1wPsjq3kZFeKp5eGtcQyPNz0T+uhQYDnKqJRh7Zo37J9CCge6oVLhSNpz33YYZAthB7f43P9ldAUfbxMuLgRaPsL/OC35GFu3u8plMFmxtCGcjHCvmhw329nCEhW4wM1AbiavH4AtoGgKL99eJwqmNj3RKHdzzXkK3r1L4w/UG4lMANegjnOGp68zS+/dHUHfFEOlzkWHYqwgGTkssPFGZCrwcml/HmyPUajoWnK5hse8Z3pHBd9M0RemD0lLMrtTYRf4mVwn9NphqexifpL1t/eFOOwHJunBhPouTr/CWAMaHsjLy0WGKLLd1OWj0Hhgg3j7o1GCN8FoTkVzCqhzYZgacFvhapP84rr+hVgV87mOVOfcqFZs6e/pbZIDoNUbMUf3nrVyDWajShsTsqx9EVGNoCUyK465mp63fSxY0iCuKy5S4+1HZh19ssPIcgIsLEvOuhOghkxzcshuYjhG1Swcj1AJXHThfKF7kVDEocfxLmtvLTdMQIYNiOwaltggzbBAazObfoFofsQuhXjKacZTfg6NXyc1jjLrx7RHEMjH6J4xvMuQnGtRBEchI4Utt/PxVaF4VJ6FexhN/9/ypVDEp/VKBBm+XAhE+gWrVQCPlNykas7cE6YQUQhAVjAY17GLPgNQKmuYfHy+q3FrBSK0p0uhShJ3MIc/p99XWVmNalClHGeE4HTdLBK/qhGNFj0M4BfZbzRcyIl0Qee/wjnFRQGSKwu0rnMr+hBMR0KUiXYBQpQySyGnSA0Q9usQRMgsfC6zinMF4FIz373RcYjq1yFRJPucehzLa8OJxsH8/oPAvSub3b38gNWZVF2oNZcBwmXmuqNM3fhPJv1C96p/2DNG3K8CYuV8GbnkJek/D7wajZqxxbLFkvAFQev0VGA2WSj0oyGOm2Wh/Ilm5fq3l6e34uoymOZVXvVLFpWQITWRJ7bCFUnNwC8Yg7iRMVev4NHZZ61IzJyQyn+fM4MlNMnrMSZpZLAJ3qQ/wzskmXl/lWxUqYcBWPWscK0qr/yXKNLFCCLyxQ3etPkqH9MwXT2c4spq0pxVYwmgdp1VEs7snbvEWDvThGFpy0z4dWyk8ah6Ug3szW3QP+s1pb5/yI7D3gqK4hWeZauQF1zMkiJZpX4DU7PUKZT8x0gDMT2B77A1rOZlmX9cAPw8Pe1BtXInujkqlefYiFKyNhkGkvIfTsGsmGVc4FhVkxTN2LxXXuvPgHzZ8LuYLC1U2T1MQ9oxtbA1FF5bmYjye7zKVLI2a3daxDqb7DYCHeAQVkOemxz504wOXpSyRWVPc+Uo09wChMsiZE65Cfl44S2iiBQygroQqrg+go6ZEXbf4pBP/s7icocZUsxeu94jDIdVxP4vNtIN6Lx5wWpHe5vmW5oOsmonUnavOKyU2X9kJ/ku6QNbETKCTsaV9PfcfhyEqUwPPT/12IRoZLAPSRjkoeRuVB6jwchNiT9y5E+c9UItxT2i4s/PjBfcVD3hco3dK1mZHUsRPSULK7tpPCbOyBTUalo4C7xqKYU7xdBN5ffo2VDH2ufBQVgrX+I/xQrrxBSdMtJacyoPsXT+WTLeR9dQl0J5Lxx0Uizie9jbTmv9xVccdAyhvlpZn0V353VvuXpuXqaYh4Q+YfjvldeSKZHYhJ1N0OIs/z5u5V1d/nVV5MlLFs6povaHWe6lhUlPWACzuub6DDND9D/nfYUZjymHjfRq4pgva9WUEN3hiouviaaXm8nPtlSe2vdNG2uihM7hhfwE65A2va2VlOTFcN+FXPtlE9hsyEyEMZPTlzOUWyALOllhcDjCFvj435+KXnQG5sZrQUkUgggt9jFEjoCC/SaOmN63zizlE3aMhY3bgIPWon5GlSTlIYWpgWcKCfp0yadyPW9HwRJK0yueuL2le7812fK7cmrHsHNXIRwvGB/RofTBwMEyHRcYASZGfx1AEZOeeSc1jWqKGrqQagMU1Let2sTImqOy7vTzpjJAVa5Tol31DoWyWuo4rdhYBsFd0Gs1NfX9v7X8wB4RaQ9KbjRqco3XwumHsGEc80Fpxi+GYxSQFM+X+UNGupLjyuWDwZCJ6CijYr20ynzi/CGQNCKjTVqbcWnb8UKsK3qTllWiNoDcF5NX15mTKS1y/qP2MwoR326tu9y33FNDcu2Ph5U51BcOi71pzjMLkp4czlnBRrZ/qbEzR7C3v48N+GcpVUTgnpy+m51A7BH5IcAQx8rUKwe/RpL+nGWUkYS6piTZhNlbVpokdcAgNE7Iom/kTmh5j2p2Gs9uyZVl1UKbprE5syCkWB7gqgi10p0nlSVeUGnHhNoV4WPOFY2/EvahSuw6pkF0HxOzmcpf9EzjmxZejK5ZnmUR31c8K54gQQDIbLFXxJt2CFGHuRduMoGIS5g4VdZRNJmgAACI5bdkn7JEEvvJDv3of5IrYYopTco05k9OA7TgvCzTH/2a5+aqPKpgMNjmNYJDYtBGI0AbLomQzNeKQ25jyAU0kejZqum35zHCZo3v0zE9ojoIXzHjM4mhigOsW/ZZjPj6loN1+emiMlvmGibP/u+iOK62YHht6hkzj/QQrx9XAgNnFvqR65BhXVZK4YfQOn2EX3NlonRlmW1Bm9wrn1iTnWJpfMIfIKonxuh9ebukL0YVIEQ0JT71UibG5CIthZ7VxwD7Pj1CClKIvA0hGehSXlEjlHcs/mJzTHEMgEcKya4DQimAQnrYQWBz85fcJJ64/WiNDNGmbZodEySrJa6pe2+cn4eJjr2cr1fzw7xeqC1qZ8MS69p80XU44IY7J6Z1BSJ0sBY+mTQY9sHIHEiutDA3bC5RrA2krVt3bytf9eIUPmAByo4MgXAttgqaXZpptF3EmCReGt1wXrdDCfNxpYuCWCVaPLlnVqepXTpni99RG/5kkd9qN7WhU0i4zyammIZrIrLjQJQD1tr5lG9i/QFQ81nNWUu6U9r5uGMB/VSX4gzs8ryKXzHqdIaf5N2JKL3jCwHavbZ39BXkCKTP8b7RDxSwJQY6/5IlzdFfFIt37to20glBPe7wrMDO24wqQ8pot4wVekBbBiQPwagucSyJP6JSjhkt0sv81L/AnjZ5U3FmlJQQ8yjMxbqUXpNjtgBH7wGv3MuntYSBwfk1/zp8+iok6YGjAuwTbcF+uy3M8U1LR7f52770qKqylrfJjJIGkXbo3WJFscc+ek0KT5LuPYWGQct6UpTpF8apKdDI+FYxhJx1/9j2sHNe7p1xDiW7YLNqiDDGZpTnPOsOujw4myMLqTAPArKOuCh23QgC3FMBEjb/NVkhwN418SblultKYgrRgtk3qhzugcz/42lgxxCpIOFKYxsz0xBfa1o5TbEejoDekvWTCs5s52uUbRJRBhgrCWsD8RBCy6YboX43NN3wvO07h89366Fbvq4q4IQipJgPGJUAJjUlWnG3+Vz0vWt2/fiezYC4iCNNtPMJkhTcs301m5z5HTNYcPI2hsNtg4qx1pxlpD9+UmFZ2fHfufamcnNHCbhPuO+x+B6DNjauZDBb9QNagadWId0dpbD1NmUJsd9nW/oY+SHuamZPR7D+TYJXEV9vXkD/JgdCI2bLG0xu6hWwHUDIKvUJo5mVCdAyL0SHnL5SxyMaFjZfcyw3lhcLRkTHoMezt0nxNQQ+HwUOTS6GIL2WhDuDuUnunYJ5T4sl0Wqk3RgZhM0/E543u+KENyWUVNnTCm7us8BzsG+pXyLsP+ulcuiPwnWsMWvkEIISHoBHHNNFEHywoWzIrozI0cy5eZp2lTGBoakCocM6QJqJtfduom0gTgSYSUtpJe+Zdo3up50wWbRAVUxroERUsV/nNIKG5VW9rb912FnFk4N3onwjalLNdyJLkL/HiE2I1APrulkqDazHI16ni54uTXY7FAT/tn1d93Q8Y01uANcLSMG1YgnTJ4NBjJC5fZqFvksuhy7bdNiPcep039kCAMbiTqWczE4P9ZBJoGf4ZHmCFnDMW/cvGF0VdIbJacikVehRyaNvAQjjWUWKTjdJHFHEBnC1ESecV7KFNTV2D2vnvDtC8Zhu35AvSVnyq4jJHvJkqpExM+16mjGwJEabTFPzu2CrjOIAW/2TaeRIRwqXxUl3JeF792wh+YNoUR1aWZ80kHhjJDJWeZKaXPSavEOswOrMYosj+h5U//nu1pSwwbVvovC3XAAjL/0dMtvMZfQwyQC3G8qpVOaK/ISNtMqV40pTPXPvo9OVN9ToiBR+7cwokKJQVo//L/vkBxf4YAaqCsra4IN7qSjFz/zlTH62yUwM92LJnKARYbCI9F3vs48TfRCQaZXlZjd1C+PXBppmfAPx7f9xZ+uxzcoZDo/Z1uUkJBRAcMGvaTQxuBJ3hL8krOQu15mY/kTFnbEtG2lFcPFZSbtv2pD9fGV8bjtyHKAPHp7htXcLNMozQTU4azYTYg0AdgpLnfGrWb7ud1a+ox+zrMy9Ep4eL4AM2Jlid1WRocVP2tVmjx4grhSXFI3Ajis31t3LbwYv2WZ13xJK/8p271yOX13Tasp749K5ZBa4R167/YDppz8vIuqCQaFr9uEx26BQMDTTEtVTAQTrrnNneaMBPMzNi/iskawCdwxnDPxOVqJdL2rHmdpN6u2UwhgOtM/5lxtTswI3VPLcZF+qD8Q64FkDuy5X3hhSuxm39ym+e0lOi9WQAP71OQO/UIVcJa2L3ij2xAZtS75j7dU2fjK/9leVUhGMzlk3Fbu/JmvWSj028jaGajQyYWDyetHfQJGpQBb6HJQl7pummEaVEeoly+b8vMqo4G+p7lCV2y2aYtjxrQvPQzBobofZf9VTroY22wHcLaT47UCVtzb6bYu+9MMUzmdJt2mUrTVj20gzDG6plF2j38iR76plpJLE6iyhN/BHGzpU+sMQql9OKwTIvwB+yvhHSm6J8BMezpT9sC9lPT0QXkzXN/qmpR5cbs+XlIi89lFcOfo3gWmCKVFcsw3UvQhRrIeN7VGXkfKVzN1XUYL2qv0Kbli4p6glInxGlnnSEUyBqZhmSjOyuSTniQSvubwll/5TmY7zMmmIkJerKyDJ/OamtcuHfVe6VekOAcuGLBWVJBlLZNCDKZScYwNjc1Eg3Tpz1B5t0aqcv7lSyR5frlDvyriiEt84NidqRoKr+tuXc7/LQS9OF7fQYBQAOmrzlRk3aOQ/7Lnv0KlqRgnbwT6IHcCzj3R9JPM5PBaqEukFmlVvvgctr+ivDx0Wvf6j+dfuTCcbQiPOu3ixBIVqL7+Se8ZUak/mBn+5dO11pZL33bZFdg6pi9S0OJTECwXOfbg1fFB1U86TDOyJ9hr/P7C+v2LjRc75j/lVFykNa3hXEaZB/RTHw9xH0sLl6ELUnmQKxAD9kVBN1bnN+H1Q6+UyW4iNijSqvIw/WoCjAAXC3n1J4Y3CimbzRgIY6dZQpjBzh84qEAaA3LIpA7ML3h6vKy/LGMbterLXF8oI8ucnsMzhdhWC6yuX7Cl9yoLYWUwb480bWqqKEBDPZRbVztWGBkY7X8Ki680liHtmfOz6JkvOCqbcw019RbKnecXfcy1ZO/jL5U4jMF6mbuZETq94iGjFdBhsU4XHBUkjvbroUptzLa1lYdGdFVRQWyI+Q0vEC/e/7ppUOIUFOBH+38IB01weNcRBwpwIp7ccnkYShG0B9dCZ+l6V8mkmjCAUeAQdsGjf09W5g9krTcEwrmuioFdc+snl1aXX/C28VzCW7gQYHIqSxgB6GM20RnzXtqSqfLG5VtYK5pc7MfSBPjSL9WIzSblsGU+ly093o25fQJeFfTFj5i1sOsQr6LVnF0u7QaMLBwiOT4V8RtJoji3V58MWWMQDU29iWzK5klotZgEtjan09mvFnvM65GZrMINkvdKyg1iwzgVXvhISTf40/Zex26Ge8mmXSDxdw0T3Ll/oEMLuGv9io6wumjpwUJOFXtNFJ5Llg+CXmqyuQyBK3zGQ3ZbQpiPp+ruB+2L46vPSCrzq5qjiwU+HlPJzwMGZSn6AWledSRacZik8ZWK4qlgyX+jyyVyKBQZcbKUk7ELltkVYNjqThtGxDjkJUPeGMenRC+Gic168+XoK5rpUgwL53iETGoEayG0olD6l/cbnKl5ICPesQoOnAzk7r8tFCL5+yLxsMxi09NVA6TQVsM0PLaU9um97W5CSoLJTrFqsmrPArsKTsFvFTI00ljn9IprbUITBloN33ORPVF86rz9icdQnei+hLhEG2E0IBFVL/+GFXu3rYyq5O7GNYSeSYnAZT9Nb/Dr1bDDu2tLfRfzSczftCLDLwDUCsWxc9WMXKmqvjO/X33C3pTLoYa5pKObGRoa4w+EGzbyT0sv06QapPNe4HInjnGuXtp8RmCMgVWLO0NOMpqMEK9FuuBIeL8x/X5Esm58sm8eZ0gs558HeRjJmY0+LrP74JdonOKK01HeTYGkVXkFWrnDN87EiK3up8/M7sdKTA07cG1iE1+m4Vz3T4ZyJpMxbldPBU4iMLs7XDjwu81kkL6uTXaje37RV8QmebwMAcD2/Sj1Jv4ILlRHZJlwR69zyCR7mcjOeIMYzjCK8tYdicaqhZtkEDa8UMUCo4ckoAKNMpnltx4htjP4rGA/Kqgrm+CbddbsYB2hI+51ADwe46bDha1TitiF4JDxc3V6iisJ9fkPqgTPYcXArjysv7XkdW2ClH2rMZY5H7PSm/ZAdVBX7aHcJPmq06CuqGglAcYnxtdhB8t0DD83CVfyvCdXwTF0WzAI8DVodQUTdUb8NSsqedOm2OvAFf3HtRZQyb+shrhJ5wRyIC7fYqC7i9n6MciHHpo64QKnoYLfwrr1KZubs9ds5fwiKnZYT8uiDbU0w+I2p10iqTHT+9sn/dXcFjcJBujz3fz9asio0I7o6WrtFLU4z9Ps9gBL9o7ESr4CejIJzEgKKcFP9VbYk/fWXOAs55yXl6GDwHSdwB00Hxtr4THl1La82L3tV1svh3I/tcsYMczlMEOBZ5C9fQNZEcck8CF8/v9A8jaAAhxVfpjfIg8axOdatVZi3oGATILYed9kF1YX6wQ/dayagjUpCiBKfyWKl+GzoMlC66DlRZQPTDDegMbHWbDMI9ew08HXjlVPnAv9FTYD66wAAUkUHqdu4JTzSDYkFU8OJrE1MkJtge3XZLKF33Xd0CWdT+LdXUqzXTVdF/WZZGmZkSbuChgSNYvyyBsE6es+Hmy/5UXVr+nneGuQkjn6VSoIcDue4uNrmdaoW5w3lMImnOnKgitUN4vjMo5+9qYbzpmEzpz1HW5C9NvfVZKmASgic8BG3ELANnga3lTWdOLyhRT8J6KxUtcCd3tEjZafsvUjbU+TH/lQn9yXF1yfnCtbuuJhohU9qGMwyPzHiCzdotuv/OUWuzu5epL21gBR4ZjBpjWBgmQVkrc3/aWl+ltoUi7fcokW4GcTQJJVXg+48jOrZKrnGASm9vANP8gURfkjAPOAjzD/7T5nO+ejZ3Zx+Hv+1CrGvvlcQqk7AuqrajBfWOOIL+r3o516cpxfUdIfZZfdrbRphsAuoc4XfPywdINT5hFECOyuoveaPKgpPF7/Dcads2YNgFwznhZBM/Ioraws7eDurj0CyMVsxgymBWNubTBDm+LydcvSC4MpZq8QwPu3Zixe37WrqEJmxWj44HGAtZBS7YEf0TafqSAnXOaURCPmrQobHg5bIFaawfUACMm61Gwlr5aVnNTKgWiIq5NVYq2ZD6dtuAPSocIsonJH5Q3xxvuDosG2aIzjDiXfuR7u2evUsWtpudrDgRSymNxq1CxJrKYaky8xhtgSKtU7nUD5U5GrGezMg2KBCkcJ4uTIx6E4nER+IOiIwKrmZQQkFwzCqKL3+StGdXGx1ac8kMwG4pcRZch8KJztFrSrnWVk8rPJ593/qt+qWXh5f87OGOGfCy/r1nFqhG+dDuuEe7lh3CV/ZeEdThhnwkaOMfyCmDI4mZnZ7NVrLvCi+JQB9TX92iu7HRlDPd0XRD/BRGZ4K71m/H6DGn9GMCT0PKsHvm0S/iEYYmrfhULH95Mz06kJrTdA1iZuJ7dqXQFMmM+Q22HBC/X8MimQBL6J8Tb279DTRsWZYd2Mf/6ZuWvk5eMABtmrhzn1ZLLgDdZscQTDUGfRhcdyDjNp1LMKSqA0h8/B0sz3G/yb+HnPjndKxWLqqVuZIQM4VuronUc9MQ1j9Cyr+OSIDDUh8TOMXRgMgAyJecpwK6Id2lb4mF9Rx4pLaP7tD6aFruNAlyb4Q5vxMRz7x6rU+kI6dPh4cp5mDO83W8umZCkQsIgRISgTyheiPTL/nX7Wr1cWNlJxT7H+N87NpsHiHglnt/qCcftlVGYgIvRav0Sr7LjeC2HqwWsI8taSExoxfiLsn+dVnKqaTzc7cJdLr/6egwsT9f+2ErFLHy8/vztUoLsg/HLJAiDEslSVubLVKqsW93fncYAAAAN1b1/+LHVPfJtXdvqlAGjVOtt8DDUnrpCSzPGYkt8hzsGhkKqyMUTxcp6zF5k6+HF9k9/08GQyQHqqVcdMgj8FuAhVOO4Bm/d+SiOHXHEqb/z2JiWeAk/ndaM7Ns5+y9+evQxnPIUwnZI5+6KbO+OgG+C3VD2Qy7nG9558miOyOCT6eXj+CxePdM+2yf/yBYKTjPM0Bos0GrQfDt14mp67Mm2DdVzrYyj0y6W6OI1FkfHQl3SotWEkPfthaWTAxnTV7359qVrOHTDCC679ffuW5DlqtqU06XlmtRS872jSb0hebGv8OAzYSUvXZuZ2/11LB0ttCSt1I2/ABIsNRaBg6JKChLh5U3A9r4lhX6zt0qvy9m6EoXLzZI/u3+kjeTOzvLS1Ldaef9N+DqZVSfvXO92HKtbL1aoMdl7UMFXE25L2B/mxwa9eTKB0xZO9SGirrLhfjfOO/oXsbjfJjBU8ZjPK7fN/0OIceoVxmCqyn2SKnRRC4SHZ6A3+LKlm+SgJ9yNEZ8TnZm/SIQ/hoshHsel5XJztFnfWYTuQR1YywmwSgmFtcK1pfD3Ms9PX9C5CMW5WixGCpzyHomqWfN7s2RnYAKuOePkbT6TAcieKeRnBXp3u+Rci+vKs8bPQ1lNoUpJ6aluBZLoUF+lxDOy03SuothB6ZNJ+Mc9XwXiIo68RJupMISeV+J9PAYz4WL+jd1yi47P+tjdPLxV33MlHoaTkJGRlB2/s23s+8KkRewwkwUPfGISUNOya5so0Zyn/svfihbygLF39K7c6VsdCft2OPWlfb3O6K8p7PtJT27aM1PJJAZLOCAcIyHzOAo1nr9cNCK/7QS9zokxcz7Pbm5qPcrS2T12khkZxW2Jib6ebupbbvaylZfEbk3qP08nxa0hF7nXoPSKCzZCudbqsy3EhH1k4JFwWvxHZrG+9TbUFCJLOlbftqlY2ZScldfh3fW71aPZuwnH+UgZTcBHhT1hGBv3nlubhgnR3j5Ue7crn+UGUR7N9ki4vuIXQCP/wTKWKfihwFwGe4dJg/RFyyPHkSZdaQYzjVjnuAShYzF4mFU0jpp1Xw300foSBnRrniR8KzBrojFL7B/QDq4LPYW7HRuzAyn6RfOxqat4RYa2eaScqMlGY6mVvmsHv4qfZeNSwIzncCHgfiRxwLFcPTucsOZJ0MR7n75Y5x/sKCB4NcnqaBElK52bVvJ3gcE2mwjOnkB/EpsBtvcw6z1jrOZEGT68wFDDBP44OLsTljRn99vNKNAhuYf2mdRQzcETCuG9jemz+w6oWn063St2biaU2UWaDZhDshDh64O6y1AvqIQ7gP1JAaQrGAF9AF8MAULzbs6PzULGN7yZEURM4Lr5v8KMSyqXlH9dEjLZR+ffnrPPS15UUEx8S+Q97phkCwsNJaYfiAH1b+2150o3KpvkS7+DYJxHslDdQ25CiM9itRJoFRsb+XumqgFkYnvfQ6SigyWxmMTNytRf7s/sDmAbJzWrFihD64x4ECZYUwvV96yK1dtDksbb3eghNbKv2dYvI7bhHteWwTjEOD9EvgTdf+k1LDOVoWasgd27iiGNqRvhvQoLtjnwj0GVlgar0a7648iohkTOgLMzfFbTPtEgfdWCbhXR9i3eObHRX7rKyMmz6QzVy4a1XK8geLikKP9+rVdvWNydFyx1iDcJiQ5DvqbMX/QUL3ju6zGRkcmjLk9OVnQHAkueLfCz7I1GtCUxX4rLwLr/aZvwXCt3Li3GaVUijTTVTD8tDbS9EItQfc6Mqy+c8YXSdolbosyuHCC3n/edVnYK2mROVuvcPBZaO2InpI/tSTYpC6nInr//HS1Z/RWUcjt0T3S3u5TfBHZHZChmdN2bR8n32INk3Vbazr/A0BmYNPLXgtEpAtf3X8YJflmoZiVSt7qhroBU9gcxc2SydePjiTNHFvQUcLiBC5A/kwjqzBFVdkIIkMMH5mH4lZSwgJubxpQsCkY2MWkSxt7XSdJgLU2OVU5/pA+oiYadKPNQ0QCuAxwePQDLYZxKLAJB4ow5o22pWF+zXG7F2NBflsJy/WvGZnVufaBS9vjXANqTfQYLE8ITGm0/F9upCQGGaCJXygXApCR/Z3eEZMMxouEoK/CGUsViwyLWs8k09cy+gOvJClZAV2OEcEde6nXCDHEwVyc3RvwvJeIZp/0nb08DSJcF//VOyvu37qwg67RlxcGe7I6L4E6E3hQDFJ0TfPPMmAAWgNA3V/RHe5Ovm6emdxViUG2nByxWtDqCS9Ui2QI6LLUDUto0XxbnYRnRA5/OQntbdw02K0+6INTcc+gu+X+/JnHq2AY9QoBwlx7ef8NwIkSVM5ICNPmE1wSfukwVdJYEFoJoCsKF4+zzHjCUWIQw77G1XOFMISccUsrlVm1BvDMi+FGb1cZyODaJG0OZFxnTkEwzTNGoyT6KvV7XjNjvqfyjQ4r/bU+YWSCrfR7QgWy4P5/iy8xTlNFNqds59xcxtyqJSFDBpTx1ke3/2SkHyFj5QesfTcE+011cj42GcUxoO/OLSklB87wKyCKs/jQkkYX235heN66EwUMU7GV6By7CxHBwf0EtMGXuzeJdbSAnRvsA8kKUZ/Keo5mtuRsG3DmGLwyVitsQbSXKkQqgjDResFcYrBcYvUV8qleN++r3ispgG/TPqg/qqC2p9uyMJ2tev+xJBrqs0jIL25Nfkf2DlX7y7bObMq/YjjOECOwosld7TEzNde4Vzjr9cPAOlXpPXHi07dFtu5ifxxbqIaId3z9kbwMP8HSLl9HDKtjLp/4ddSiPchmo56YEjpodPFY4maA98lBa0YGmWJALpPcAVMCiR4WE9pL8Quztd+ElJbS4MeCCiDseBvmjtousgji/9aGwU4+gHqamhyhqiE38DYM9gYzeHgMc2E5x23ZDrm66cZ7c8Uy+rOkK2Nur0ARId2kcolOXWvZnD/waUfqnF74bzPwonc2zG7OfeyYrKVWCBKgdg5xDJtPZJ0x6auYmBwrXQN7AFIrTrJVQIplMGWgliEXuC6j9ODXgjQjiZkScfq1sFt95jWq7MY9K/OnJB5y5BOSUYJYUzbjMxw4GwGIvUiphnBcUlXbU/uUS9pGMQ0OCsb/tJO/KtPrU9vZL5EgKFhB4OFW/Obtb7RnG99tZ3wy6nB00som22dSVPk9rqx6Y5Sb9/p63FvdWjPd0Y01pG6euMdeK/uk8xfQfBfqhkz93C3s6nR4S0cb7TO1rvPyI1QhDqzzGzYQ86hVB8Zua3WiWg2ifMO74q79kneaC7IDyaoxsFN9L/A5AUXaARL6DuWLMJYdsaoKFkh+nWBrqg5OrdZ1vp3H7tzNJvu0H7GX5pK1UypFAS+N5MPRLa0y9MHMnFviaJiDiys5xSRpFFGC9yPOss/76p+lNi8CnmL8sS/qLtycgDZViBRRuGHd5t0YlVDy2PrbWqWD3O0U1Uuwvae9p3+u9dJmwABSL5YkppyR+zzoDG/5I3ZI2R139Pp71cDqJGB4Zip3KLmEG4VOB5KL8ohDIp6yzcnelel3TQvrSvI1zIJkir0BodNVwW6M8MhvhlqVYTFRJ1oijGz4Nu2GaE8FJAn1JAT6sPfwObyZ96AQ0sz6xHQtxacCPDHHtckdvEy00Px+W2KqWbjdzqQFa1uVL0WzcUHg1n1cFL6n4jysHbpviuu5UyF4fGQ2lw37Yo8qMzsWKyGs+EJ3oZFYFg00MVqwHhZtreizTGdjeNSdG/OS7fHqkPBMBkiIo5+HfoHDa8Or6U70BPjnmrPOP+N384NGCF7IMdtmz6u5WCxuVnrYueyOdXHfUG5oeBqpvrz7HNkDtXGZCp5MW5JoCj9UztrFTof0VY/gT8sZhvpc3QX/bb5Bq8W3riml04wOXIpPgBA5B6EosJWx0aB8OJ1Vjifqj7RK2q7DpMIpJ6gWLb2ktG+Hrh0zMiBEaM8yNXZ2K/kG+YQ8bveoeMRPFJANOwnfKd9BdKO7wj9KVzws3h21XYkgYMOa/ZU2XJmIveNOuQKdkJ2Cc6HsiUGKmG+GfHth6TQfJcmvnCBbF9uIO+VFpvh3x0RARvDudCZotedfoW6WLdAF5LkKkW91ztQ3YwZieu/ZZqPsylSMRmUe01y0jzM/nlgE/VOgK5srveXrDQp4MFtnGjNf2eIPkCrbD+51057nh7TMqn4GjmnwHSiB1RXtOPOaczYWOveFnV2LCoCG/CwewOl/TOhuCle4SiuFnQzDyfXPYP4zmv2U/iEVzLTPKrmU0XdLoQhIYFXIrKQLyN65hTKb89Q8tgdkcOgzkzViWudOzE/E0z6QUpphsEEBR+4gJj2xm7VIykWydLyhbl9Yc33eVHhZVh1McTRzG/CSeDqU8BOhVC0tQ/q3dJyAYGJvCjMFnh18ZVjXFbg0LISAjvI+a8hnubG9OA+fJVUU/XoQLS1qtpmQIHXYM70N+QjvjEYBwDRjbZXtAoRcKHkX77zMoDKWa/Cme0DmKLnwFFdmEjcaYkLE9KV+gXkxEsTnPxAuQApYyqnSeoVzmalglodgp1aRMPsBqGiVoJC6gZeGBO1WINFJj9GVouStkanBG184c7CT42Cnn2HyZfk5q3LIyL04hgyEX/+kNUp3VzFiYiUgpvBq7CL3BOhd51Qx+iAfYLnmEnwN2RzT2tH4vixcjhlWSkkS+3p7IXh9Hyx495/Hgxci407l2tSO5KDKiIAqBx6PFsaEgFkHzLVNo8l7zyVfZVHQtVH9eCxrZmqexpMA+le2Mw/sOzqgqmqn7keqzqvtQHa1nZa4Hlc1joFzFVpWwoY325j9OTaL7DFXx5roobuQqilBbyFKgel7csNMIkXyQyYLayNhm7/Qt/F/yxG1+hL74GnuB2e4UidO2meM2q6UEXZpTUG7L6hxlAHC6OwzKZrzyJkHIH5U75V1lxhWupk38v65npMCqjlb7PupIbotyB0CPSaFiVxEmpwtMrIsJ0gK/6uxX1cD+zEKFUUfHqq0b0kfXJGCzTGkkzlZlc4JUrk4X0Tyk7ZckM/xCYJ8+c2XlshphfZaQ3te9f+TFxc6nrhlme1WETQ7DkvdyCdIsUbZGZw8Lutzed0ALRgbCYDp/WI142vM0p7hv7k+o4PBUzLv7YRJ++T8M9iYxooJ2cPZgFLX6Ji+0nFWIXkf2QBnSQqNrSSE4RAtpBqrz0+pGLKQ3Go0R3L0Fhtz7Tl/Htp3qDTIZaCZ0icnS8E8M0Utk2v5+52FaMMOhMFeG0+R1AL2LdT9kY5UPO0MyjdnYqVLwBo6Kk8PSsWcOu+1ckymoJlUMROXresXE7fVK7mxJf76+YJs/8+YUw3Ta7vA5xUK0on59unp8871bP55979cywRBTUX91iPbswSEykvLE+MKbDfIVn7AiFvdKN6FPselCtsSRikTCGnfwOsnzkQ8CktCbC0iQ37gG0WcVs/CzF+gDrP7p/64QH7uujh+R8VBLafQqJKfNN5FWpC7dOKsXc6iY49qh/Zv+gLWnn1VUNyWXR5w4q1mSju+2FymEiMQBDWMSNMGOOOkfbgjk/yuLIAryclFuenVUlOv+KrBLhWuFAA9yImpJV04T1LJy7Oi7UfICN3hrRTKybPR6hgbZ/BG4RDErady+85xTbaPVjSI+eBmUoWQtfzNptu11ZUSfuG6j2lxjk+9nd2pb0cKgur9YxeerQk8VwhsagadeDxNmPF7K2EPgatGr/L1JhxAyt2Tyt3OYza8KNRF97L4DkltPy1frxLhenFR8Yre1iG+XY4aXy7HGbJ0Sc2/1nBp7haDVNVuQ8ebr/IqYorpmv79C2IhnuLNKQOjIKNjlNhusWoTDkTPCee4KgpT3eKZ1aWut7f0mgFhGwIIXcCCcYscwv2kFOPeHLXLseBZTLCb4SWPBDGN1zVrgp72baLxspqWlpUqoQ8+JDKdPS2MKPDDIEQM+KbILuvCOpXQKMwEf6sltw0SJr66qumkQ5VBwJX97yBCl6FoDvRx4Qj0u/dZSaVJPWKsBLmz4TLh6Rt5bLwgX4ACq9eKWeAuxcOHaY0ELC7Sr2/bAz+XL2qLJTD5KiT/srwudFZcodeCbFgeUR7w2EgKqP6AzzaO7KXh8+ZkFzHo/bTZGs19W+vHQLGXgWtlks2ikN3dsq/huCk+wOQiebMHqiv9CP03Qr0SQ55KAYjGwY5OsHp+a11rwX8MYCxd91y/T7HC9PjSZJgu4e2NlhTtrMAoAKuxknkzdwQsQFXORgANwXb4id+WoXFD8Zv8lS+EqOVIc07boRpPg24DTsGhiMjSBqvLkY1ZVPqeCp/2RZr2dXZTt1+GLDT6fm3tZ9BldvrCsOx7i/awC+vdg7Ft9+X3Sv/FNxMqdrsXzrn958tbbLS6iP7bxawPIUtHDjmDRpYrMJ3+l6g90VmbPqfZtrqpw/NfbsP3NfUOvKRn7/QbrTdATJtDpPflyX3GKFwsFqoyiEgt8c/FDDtGEmN7KvzfnAFNfYJEaN2+Htg5d5ojhA0/AO/Ano7y27JDgo43hX7UNkiU2F+6GuYShUDlhI7M9flfrrWuHBJznHOaoc/XHKgvBaTZTHMybwD9nTM2bQVLWpslnPG8RotlqjPUSJtR+WWEBZWb26uf/aREg/CtPwMJdMyFOYXlgJTsbnaISvEedKkPRB9DypnhG9/73VaffrOi9zP6QSnFrkBAzP87fbWaU1tw+S9P0od+7MbagW+rbPHSJE5ExvWikzMgkWZ4/tqIFd9Rs5wmSj961OumfHuNcnP0X3p/Aa0agghUs70HtgR0pK7klSO56gz1SesbDkVNPuLu90eyJJlytDSweMxYPkxtqG4acswqrWooA99ro/y5havssoHrEwBq9eV+aE/oK44PGBiSMTpMFHoTxk5LN4lDR7mNwWzPh3/ejWKEc2nzouWTDt2AzhsKTnDuDxlz49nAh13IZRtJC/DZ+CxElGVHy/MedPRTP8Pz2cy5NlE0TFRmuaE5D9gvHOrf6+2dJSmuKChwkzahwZ9i+tE7lVRjK8XqG4/8IcJEwFcNI6ZLMhBkqLmSml93R3B1G332zO2Zxo4zUtxSXM5u2ZXhGhuqNyHt/BJTEV61lwl11411gRHvIIQfT6e8RnXB9MZ6GLWNQn1aLmWHdoY+CBwattolRHfgIJ7rJrP+V0PCaGeTqhZcFU1AvAwVvXUWA0rGy0fMl69H5+jcgaZSjYeQ+8X6UjKOlDa28O8MuJe7aBbSx0KLxjkVAPIwjPXFOYAP2LluwsPr8g8QQpHhKHNyOv+f7ELPcLQzFnZ9uhdYXPKNFz5xzXVaehbknh8hQYqHZzVIus1a2sS3kSnJGXJa1SSvSm22m24f1F44pqkzTbDZaKReor1TcxYiOOt3xghe8Pe/u8tJc1NjilKWKaBezzXW+XQbTssUi3/GOTRNWSNggu6nE+zPT1BhwpWGL0U299U5W1PIcjmAq82u7o+TrpnPY8uhhZ964xFcfTFInDvPJCKV4au1TMgMCmJyPxfMRN+kx+XFioKQnCHvExILkqSH2+u62IhiXQlVSEqo653hLsMf9dS+zxaN8ycQkW3UtNTYX8CaQWSs+WkwNYefalDg5211ta/xr6ojlN+F7As1uvzZHtzqn0nyrRuLjk/12DuZ3uK98TEKK2vdUalmnk7KdEk3mzjoeiBvXC33BDxJuyNpeMUD9zr8PhODMEZuaVsN9BVNcfGKmm0YrUihyMADT83OzcE6uOeYBl5/4yBy+31eBGqKFAbZ7WF6H/5p+Uvm1GSEAozJ9p0Avcmpjduxj6DhZGgxpVXtR3sk9OVDfCWXecQXZpzMnd+xS4uyV1J12o+emqjFM9B242tu8SJr65RkVDTKVlR9pMENn6u/9TVC/Us1Qno3aTrmE7gEcMLyu5Uts0cXpr30dSCrUn9DkllVLd6rQXh5y5ugYXzmvd9ksKnXNPCkba1Obb+OoIr7bFF40IV/M4aSLqkuiZwBgJ5mSAtp6mIFV2oD+kUn2jxJVou7HFf9qWWYVpzz9inufrks4JYQwLIIVWmhpvNZ62bcUFFhU9z0rcSSTcVQW2O7S6xmOWNsMVqFm+mrvXog5BEmL5F1CySVTKYVwryjF3NzcRcL2NSdE/RDP5gJD9/bSNSuNG3TlCDQpUsLCikVuFNvbdHFsc6ofFBkTpZwxswCick2MIsMMebZtK/YT474gD2VLlx9LUcioF+XE5ntx0B3lURPK0X9Tyq4Pbn0ytwJpEvH3R3VLgS/2By165DK52TAHO5Aafp9YeKTQFTsRDsc5fsKgq0M88uxSSC4CMjIJeKohqK7R+1iaB3dAZiIToUpuJyYML702n3fM3qxItKSnaP/6GwGGHjI4UGA0XyI9H9pGaiAcVHcIDuJMOCgHr78ghB3plSIiD2bk8l/5Uq5d6umEme4Un65nScMIok5JQMdC2gGFS5tglLuOLOKKh0JjYcbOd3GerR36gklRw+Lq1dGBq3JWrVcdn8vtRjJXYsxbousenxHwfnt8eOFHGCo9l0/4Ph3RQ1RBCR4UGqCacyzY7eBZMageYmreOx+voAIyfGftFm/H2XEp2D+UqDbGgB1Da9JDT2JhX651moPkit7l/5LDZC3UKNJIZTmxNYFzdmiymC29AQm5v6zBH9n/4aYcNyC+RgOoJlImE3ehWYykksDwracTIzmZtUEgF9eWI7nS68BkM/XMRX4K4iABXTV3kvPKFuw8w+7o/QOSEzi2+s5DMa3qasyI29esMbwqMJnNT+WuGtUT1wFEaJMbdG93nDXLNrYIzqWzMv8tlAKSTqZL4My0hwVJNCFjVSdY6lAAACfeQMouw4VlCTxBAEpfKUnDKjFfWUvKWi1bYc79JBnXPqYL1a7hB1CBOprLwJEmJdJmJp6U0Y8T3yjs+t8OZmmxx0MmcH/fDSBlGMl7Xbh/UMw+sY4zSx6u1e/i8pYmgyDZNLDFoUA+Ovj6Uj/YShl/n/XM1Qba3PRn2KzDzUgg8rgckez24juh1d4K0b/5LJzYXuRCZxV93TZQeV7Xxi/RDEXbauN8BO3NWi1gtP0+N1NATckUQFZIHtbSpcV6tvuNy/gxucWTBHV/bCFs8FrPqPGpaew5MIK3fvyBD3ZGImyjGkyVinqcom5gMLTbKBM1ANC7sMbJkp2FhYN82s0HpfMxiEea7g+wTqZDDwgY5n6lzGM2RtRdfVgCJhT3W6Dl0NSJ4oOop63EKQFE0qrJMHzYq+75fhDwf/TNBc2q83WDhFGt4UA4OnMP6/5xyGk0dBENhnRMH8VQJugjUiuLb88lC7ZXthTAuIeU+pAgBml1p09Frv0p2OqbnRPu4DDETxmubJs6AeOynjvmJojjFnoJlpdCGo6jEqLvRGwTcHskgq/kB6PIZ0dz8SwqaXsA0dFlGSqQzclKH0VcrtOYvuIOnrxbPWjIK7+beWVNpDAxdb+H14u/cVv8AkX1KgT2BAuSHlz3Y9eTfiTcdK7vpsVkjfnJ/w5Ovl6oMleOHpGElyl1HYZo/40p6QiIWK4bzdMk94+B6nHfdN+KTEv526QcvC31OZbUiGT3A7eYfs5VW8+3D2NdrTwNqZVErq5skJBQa4Floc0FJ9fvfxXiM0lFTZAGjRiFqSLaYtDoA/thgal3Jjs1SKBpmjwQEv8ZnjwShRi5j8KX6vKUCQDcbc5DwvzEGzKOt3+9KUycLXeuQlt410r3LrnVjmAGjyUhBWCm0jXzzI/ZDZWAP9WnWO6h7iX1bG6Csy868Jshxy3ayHSIj9uYAxlxpx0RjtpXygHZHsaJ7n7MwFrFo8t4wnVc8yiaWPbfdgJaKB+QWVE/dWdvQ8pepq6eKdMpRJ88uB1aI1YGJFXmRbmeeQnNiBVApADeHWi+eSLADY18MtcIBCSeL9FWvG6lyQbfUU0YgKOcnbqITsXt3/Vxr7Xqxe25A41FDl/W72v25xr/9XD+RpG8QHo4QBNBC+45I7EsdywvlUX2wHAY87sEXfyJel2ThIEL+/H1f6qw+wNdZlBDMP+Gxku5vOpUeeezx42LgW2xqea4SqXgi1cvlCYP1BoLbyu2xRel3YCGzJSESTy0ctuqrhniTsvgPENk1VtoWK317hOGEJzcf3ZEi2pJ09V9Y/07WxpPbiX2MOVGop64dN1o8mdkYkxLDX6Ov+z7C2OGf2roIpU8wOkE3+LEdbLvl514tVmHL/stXAvDrr1azPb9Yy4Ta39LKw7uS5+LdxVwcGUZp4ZB86LMr2rY+vTHrpo/VY/tzVOVZN1G77r9mV6KkzZL6hP7jaQPwiHUt/kJOj7TVpiHDTqljMjze0okJ2dKQyaiF/ptLpruZ+Vb+H1G90fD72sWv6HfSwxpDMGt0cMPZMzq/ar6/y8JS/y3FpqSzGkNQ0Tu8UvFx7m/squCap6TbF6JFO1um3AgdIdXhvJ80zW17J0BqZF3FM5zu9Ww9vinv8r9TCwT9rxOcEPSFg+m2kvLQYE4fc33VrIeCP+Luv7o5EkqPWWrPHGnE+62UqHK3j2Tu/ya3Cr9vg3u/ZFvR3bGeK4If9tM4moHQGUmmsFp1PQHZpnA7g8I8TTqiAkEesJs/G+ziRtjxJ6Km/+riZ00oL6KN4zj2kzsbcvV4Yl3/uXA5VZKl9ACZcizC9HZiQe4o/VsE+ZgWqR2u9bSV0opIvmRksapY7szB9cObt4j4jFIZjVTDQGRpWwfL4msUb0C4ppiWzVlDzqS6D2CK/dpMPgHAV6gna7+cH3qmcVc42jjAxva82rFcqB01qql141WV95T1V1sBTSR5cXVsrPfiS/+V3M5ItOWM3X0x+F4AaeEYGKx6I0ir93/TavSnWwrXHQHH0UtoIRTToa12lL0P8Nos4c/pQIxtZBbfnPLYCH8i04zpCIBk2f2/JdBQKif4ffjOADTIikTrJFSiN6FrUVTHIi7HrFm6EhdPUriTVym9XqpGzQ1EFWejZRpFIY0Exudgk6Tj0sjjcXvzF5W2e3/aCQxmwBDOF0V3gAF0ZfceaAg1FhfiZDm4iU9/KwZ6fLfSrcc8hzhHzAWsyvjiVKPqckqwwe7ROEZQKv2LKL/aMpvUlpyktgZ9x6cZvn+l/CQc+VD7uH3snQeM2DwaLWT4roaQyyBV8XROYXzg4E5nw6pLfL/8CAzkzVWvgYC0w2kRzDRnv4XWl5kFyiI5jFxkaCmKlBPR6F7WtlWjO/SYeBrHKmbzOji1kHZ1hGickPDGeXORZuyLm541fcJnhI629jEfI95ifmDwg1ZLbE9v57w73Wb1VjS1WN8UXunjHMRHf4hFb/3rUIAEfEDMafIjBLCwjq1EhJOltiQVRGoll38+EUQHMwyJgdO7Urcb44fpLqJQgrH/sUcrpI85mnN8xypUXwc6r7bNHq90l8mIt0zW1Suzjpo/7aVQBFv/vvXGmKbiU2/7rC93TrTeDJcq21eUVgSnGhWxZsJQYYb7HP793nJG328v5wvP2Kat719R+P6mQXFD6bJ1c//on1UCRqz5NXR20XM+XGr034iHpzmn3BZ/IyDeLaP7j4ri2eNeqC/er3qz5Xe02Gp8OfW4TIzgcYJ0U6MIeOWxzvnZZyPNz+CG7d36+zEE7A7A6ISEcdZTZNRGRSROos0HDgmpD3eNab2NrBhGPBNolEM3RK8gyKWjtwSe817NUSKJO/Csg8c0hxDL5fhIGDY4yGIo6YADOm9ZuQVIeI9spe14TuhCyfdPW6OODmRLS3Iq8tHG/aBSfDcZY7g7kCZTvAj0x3omRW715Q1E7neL4GhTACQkr8AKSz+ICp4yzlwPp13hcW9gIJcyngQWK1gGvZ/H2ASKjnTQlxubLGsyXnzu7dTJ0y2bxASVQb4dFU6HnoEnV7mdm9icim1X+acBYatrbcRqxM6G54NOUleKQmjSoRpHwirDJJEPkLfugetrKoqChTyjyDHOO448dyFZfNWtmcfvJfOTMR/BKdeWHFfwDvq5RxwQhxOQhsLpvi0a8ho8ZC5bvvW+dAib+6fF7bsXOqyvkula1y0QD6W7qkRXAhArTbUmIJNuicodDcTxfpn4SzOSwFBwcgqlQGys4Bnp2YJh3/Hz9clI+z/ilC1fjjURtG5df1O6qAEp0ZqtinJ13XvX5xivpZsx/uz1cfvkt0kg1CHz2nmXZ1B6q3SE8GlpmlVsE3PjPn56g56Qj3a8zNF6l7B6EKQ5i5pLXqbVghRoUUz29KkmwbxAGJck/+C0ZMtSimN+sBECvNZDyxfvk+/VnlIuTw25sHq6bbfWG7BBmDnzlsj5zLIzFZZtiThZV3Ldm1Yk9UlBM/OhB7zL+woacS98YbvzXmq/lh4JXXkw0/zm2ghG+lHlzPkGqvWtkVvGcrRzZYFxxag2aqizj9QLBgb/5B8/22TIBJppJpEY8qOLRNcoGPiOpahSpoWZdS5vajSQmkk4+AI2tJOm+VYhVLWgfDET+bClZeCscqItdB9G7TW5FiEiwqgPmK4DS24RhPc8fjJ4qXp1HZcXEoPWo29r4sODsBwyXbgvf2lBEtrQEclKwXT3GryiZFgmW6G7XRVw6Nh+NX2tVhRazboX3LmLmKduD7pWhNlcQvbd3FzdE9plo3tcRizhQc1Y7JJtNDOZJbusYg6sAgs25TiXaYIh7cmMf0JjZ1x882UHqy9C7Rbu5Ck5yqn1Zqe7VU8weKz+IgDwZjdLAKwB9XiUDaZPcnJyxc6IBmkI61Xa6lZQR34RjJjL3/UxQT+zc+RfdFLK5LUJrpReXHWu3Ff5P5u8xuK3gLdRBvwrihkzu8KJh7+yptOxNBSHsRY9ouYc4ZvDmIUOf+1NddF7NuWSbOTe79sPvyetF+urgAezSRU59qow/8uFhNY7AbbB7oTYvj4M9VbdvQRtgSWZ/Lc0OtaMX2RZsAtb+eJH12JVKCvtZsiTAZw2tcH+kmrxGobp4N5mQA1pEddxvQpmuPFEyFUm4wE8TpXwnEGQF+yT5wRtTuq+FpjesB+c8AO9DX43zH730ie3uUzbAAtKHq73/wUcDcezhxz+H+Jr3tiHUL1743oPYmZI6Uko25unK+Cy7i1b0J5cOdDBVNPuWjm4kzTTllCLAI5yFpdZSNkwvG9rkgz9sb8wKec6H2Q4k90kcDL7Io31UpBl2+eyTbMkKooH7YfXPehdohhM7tZIAY8YhcQ1skuRNn+pfFMp6edXo9lB9C6imhS5Hx76zfCSDcV1isyK2WUcsM7ulWaMuG6kjX3IaG9xMouYadu7sE30l5a4rBVXHqLObGY6HkIglh4wR58KVc6TT9toiKi5/pQJEF6+nTK+3LVplpc3TM4ZLLZMJ8e30tyhnmb44yfJN5UxMh+IyufRHjwNQSA5MCu4vgKA2bH5Ee7qQDIdzyjuZrPXGAC05l0hhpY9m34L1+F1zYz6NcwlbvS/Tx4P8Rq6A4A/RAOmG0pXc3Q1Mb3sVchxvSoUpIXqOxx2w00Yvbcs75SQZ1WhovSmRBHvc2jSM/WhmZllO/mRXK3XKJoh9y8VoRf4yYlGffwjZsEuKYTZFLYTORgPY0WsaraGjaKWmooeRU2G0M7gQXcNArNJ/s/iXK7zfSWk/co5vidJsht9LLufXsjyFalqFANeBLr8SjTrz+mNpqXszUk0E8VewbaELAmOaoN5b0aN0Zs52PjKnKHH9i/MT+2IEj0ubgZo4nCwNId38xjp7szouK65pMtLsxbyl0IfbZ3XJXH2X3bITan+8eWSiG/yq7DMH0e2Z2KvmmkLx7TQvXP+33uaDYUBPqab0Qk2PXF+wbc3gxwlcBw0JReIYPYDM3auMq9v4ZIMdY0tpToF80s9789IVD6aRrFZY5wjhzLKp6dG1lgYrvljB7OHHrccfU5yh1N1GdS315CmZ+k9wUNivXedxIY/XTvIK8SieY4hGa+b5DtGa2GQiagobqdzAIfjT6YOMKfaozvFShWYxrmbSo3aMmzUTS/bAH8DpN1hZ5D3L1M2G+3vcJwnfQyyau3zHO2QdMNKFAG0YA4GDbXoBM+DHo0XWZfPZQew1HiF2SP8mksEkFUFQpgEZZSzGRDab4S2MV//e91dd7/mZf8ihH9tAxsFADw/cf1C7tP3H8qridP5YrcZvTDxyMbcW76biOS0K2w2TQ8OL393fC8emB9HEn/vlQ8H9ZS5A2bjqpqj3V09vVjhKGqdex1eU3CUa85L1g+HHS81Cf3Ab0o+9T1yu1KnWPNTkjDcPTSt928lDv8YSlPBhc8nsi8UHOpbPjyImDbi2nj4ecEXHFPgnOKNK7BfxsPi+1PojcpdhGxj8gd53UTqkPKKdmglCUin4IjLDoahTKEnDP8rWOFyyaQmPs1590TN5iFoU8dLW8AYh7EgZFXNTYwrtXPEP9KH6hTp21AkUAB09NXIzzYVniDh64pyjSEzc7qt2rzCDxq3OLaY/7jmeTE8ycW/L5+dsax6C7oou46AmLIwczo62NAFFEwU7obqosGeStitMIF3ps1DMMvxt9/BvM5t35pkWTqFXDvTmfLVwEeKtol+WkZTbGl0jKeC4s69p6bKTmTa04IPsI18xlYSwW5shuEK7k797Ii09A+M/r7lrpPS38xovXi74PjAmit0eTHzKso45xIbOMXWOPn6fM7j9Fwqd9rs5TaByBllSDE5i5zHylXO1iuOLsBU3XzIxceAdbapZT5wlStJ2bDQ+PlBkrZIccqM1qJQsfyOWk2lK6O+umJprwYtl/GaZaRvnMooj8cKccSNrX8F08IHi/DR/j+JTkmu/RF3T5VNKXrdAujvTzPEsQ1XBlw2eIPtQsUolsAQcylbLV/l75nlld/hV4Hrwkgw8iQ8/h4zTVt+9T6OWCVpWvM1pIy4iRQP6S2HRndMUVwJGrNqTLO75XtG1TV4b2khy8Yb7LeH8yfSBkn5SC1ld1VUngcsRwZl+SvBokyEnl+DhPJAsCOHvWP3JBEyHuOM7aMDhDg+KtBti98QAT4QtFuficTkdvAwFJqTjVfpsv36MQyXsPAsaeWDgJwKzUmLA0jYr9euOqdY4x5/F1fQU/PpoKznXYX9KtfUfv+NnNlsbzuUi2wsazQRgERuyRn2xMa3LADImnDSUZEicLmrfaSxteT/LrSDqPXhkCJ4lgpbkt21FbCTyCMEbrW7fItZPnvWwklaOnrkpIiTlDOsgr4x8xFbS6o5BjsXXsz4P6yyny3vufBRCQ3qClYQ3bYEtiH+ZLI8UyyixslijHURPZ919A8WPeJNN50pLZiw3C+clSsq28NauJVI90IfgsehsQLPSMVCIfZ1aRVriFGGNs8NsFPDs9jXww+IRqhVeSHphPI/eke4TlV5cXoapuYN/w8xSblChWJawipSTSeWYQYwwRDhgkcZduJh5c8aDU2Me7jqT9LHiqnDM1I9xK4M5fYx+aNB8RQ/HOLb4sR+B29o8vfQ14mhAq9oZSmE0OAD4UnT+CMpty7vHc9muXjAhAjq1W+137BDWLWfSEfnpLXxj0ozb4i5BOUCwkTPLXCzjiBq/wr6OXddNE7NITxrqkhyymmjfvBteFEtpY8STyOfpjv82BMH7ViV/W5jnxfVIL14nhE3L2GluYG057LAUpUskIR0Wf8BmwQDvDzHJsgeeDjGQJ/3tOaVPMb6cxWDanoHopfpOwZx5ZLgpO39J/gGXOD/x+T4a6Np6uxwRR8BufE3M9nmwexSz7k6Cj97vARvQAO8EltAe4EzcBl05R+4WGl0XuUQMQAZTxfH8wYgxyG13P7qhbZoXUfKLb/OHD1plo6ihTyZlEVaiah7aOPF1C0dPshd6plqNZWxtNmsIuDD57KeO5lW53BDI/56uFGUEv5UOY/75iZNxnhvHw/qiVHy3c5Geajm9gtT+a5MezIyfLGnmAlUFUUN8KFBKkYNXZRq8SyeNzwZdrjM4zWYEc4arun0pImX+UK7SV6mL1EC3dRo/v953c7+VelEKBueFdb1jRbJWh7SLaLKhLqyQgUNfMHnSEP5Qb0/Ru8rkpXUB48d4C2uU5ExbjgwyVPvxFC5P3fJCzGCo+PxnwTSFRhZS4o/lDlzdq81u4FwLwv15wAFkz/IiQYIq/ui4c9/wJMMCXbUpP0KZZ5AyHkLKF4FzxWf/LX/Zer9fgQMBPvROHCj3CpuGKBLqszJ6A4H9WYFATPFNxg1PoGag+xidcLGndFb+vUNitY38lr1r0bZwnC9dLD4c8Ci1WCiUdd1HfH4/b7hBCctx/d3quTKpw6Qug6xUgxmJhjyXBTFoIcYIn1Y8lFZi9I2qV1kQqnaoHNPv7UyTkj6XPRa6us6rs+FadzOF++bVlD69aWT8X5CiXHT2gLiGkh8lkOnT9k7u1k5fEtwbwzG1nOKNLW7skg4pmcQdoWJA7kbe9BWcsD9Rt7yHG29CP9Dxye2uv7l+zUO4TwRt53Tmmv++9QN3lUEZ5PDMr9WkQLkQh85iufej1S393GeqnfYmdg4lYH9KgdZcsDqNAaWLoWVLZK3kgcUkA4aAgWt/mJMxJg9NRPCrOD4kp9ZYtFe5sLM9YGGt7U/B1mfgA5HkzFLumvyxszWeUR7D4BNpCt5lMTuVsLjSm4X5mehibM8NQLNKbTi4Xs09mt7LB2HJznpVWscFIAzmfmc3MwqnSEX/xMom8xJP94tbABOxxCpUr9sPXaNyWcgYAtQ4PIWXp6rIsL8326ep4Vv+RaKKDfYP+EN3I9BDXmi40UiWVtUKgpIcOLNr/K7Raf5lr84OFNmb8XX9Qy3s8GIPC8gESGCWoPKytl4I8lKgQ2kVPlUodgd38DO65cPFf6kzfEj77gBg7falmDkhBpNOKTv5ysCfoTDuwBvf6YSXQWSNW4nP0W8RdfF0ZcWj0VUAPZ8Yo3WbOYPuu1qwN/+2Pri+DH7aJVGarJw7NaO/iyypeOT3YKpgP8NYDsuIhoZCIHG7d5VRSTHpUdSxPRbVkbHhbe3Xs7vqCsi6UZNusCkjdSHN2YcKC25p2ZTa0PybA1qK9KOOWX5rX1H83OQRA6mD2zHIyBN1/HhX8jhFt1goHIyEhm7wzZlzrsW4aGHZu23W3VZwhIYNEx9TXGbMbqEdq9sVlHBXieRZEuYifFTripVwcXSFF3Qq87Ds0pLXoZcQkZ/DxoFkFc5yPDGyVua9h8EB8MtApHg2BktiqRliDKMIj6ZH3Nnlit6P6C0tffBYz4xFS7cQiUWbDjGmFH9JtN0b0BLqxIXHX94nNwmIOkJV4drDThT86N7f31wYUq9qI7w1T9bcDCHXor0+HeQSd1cGRNTy9GbG87KitKGkPSNwSvDPHSVMzEvu4ths1OqXSXSwCRr/12txTUxiUxJ4dAL/jfj2D50btiH40gPoOi8NXrOMIG+/t2eWvlB2vpveQEMW08XO/8VVvebZ5n14Or/70/F6hjobtWVL1Tz5CoQJDwA2O0CvWcAjRUUcp+0rgf8L8Y2TssBu+52YPu8awsnHHKlsl6DZO5O3LVcMfrqiw+N/ND11CnnLYkEOpx5/xqxhY+2x4E0QE+i2xJrvGDxI7onyGi0HemgqnzOPvjVtel8sQZe6wTIq4MeTZ8pQnt1vNmkYewmoNO32otAE4pSAzH3z7e/r/zU9+RD/jjDqwmA1ABx4RWs0dpOUDA+g/9wVjnGzNO3XvZY+scAPYpkGTsTsX3iahXyciQlDBblE+m0IVs9AsutTlxtKWLzIlbJd28lGf91uZIRgky2BZGVrE+F743byyUtghv3hzhijn6RLU2TeW91qOyY/9kj7dagr9iyrmFXczSWuQnW4wruapcGRCSQTfH957AqW5FSVq5aa2JPXfFhjIo1iBi5DIZ9za50FwTqtYLJujavyH1xJ+sHipldU7itT4deh7vy94NHlLPKM6NEWbvtRPSmMgiKqSAHo3QpNdtfjYwt7GOSTfO9w3Rw1VD/BGeKYdo400XG1cc3Jqc9Mru9Ko1FR9UuMxzO0o4q1ozbMMYiOiDId41zHstCgz5EjQcJNm4Dd/X9h+nW857JyItBWSMc+33QjcuKjqyCSIS+9g3LXUxIlGia81cbwzndEMW3b7zHenBq3wSVLHZC9KWHTt+edDdswTv5U+V85KP3B4AeiYm9s4wDYMIaJuH/uX7ax2ikCRps+DeHeRGP08WZHxBX1RDJNef6VjPnGNiwvZ+pDuxB0bKE0mK9B3jonGGi8NhGWgL/71MX9EP1KXvYBiDhIScOjcWJz2d5mvsyeyhBWSUoFJ2SJhPKtOdsjg2vtfn9BDIsIiWNY4fB/mdO0S+qcTd5psyJiZWfJ7LBqGSAx1r+Eagiswrv9S8FBKy8gIZO/SciQ0srJHQdPyegG9EWHRYWtEWaZWUgV7zVWeLYopDzQzeF0wqU0F1hjS8z6k0oSQVCJDFVvbWDqBx1Mzu6cMtPy3tBTghia5qTszlDiSoF0Ztuz5L6FTOZ8gGnzQd/c+NxfCTieZHSXeN71wVErMf6N3c2M+LXN0rxhixPZNLNbIXWazKRSBuobLm4iUFRCHMmhX5TTXbidY8VYfD7zAMSX46Od/7r2+ysCxQpBF4mhsR2q1qInC9PAPnNrx+JeAr2BbtiU8iWeHUqn6FPDhszKtuEiPQ+ThITYfj2ds2qWKCm/S7ij2zHFBcuo8mibb2+/M9FDKy+rlJ0ZZ4dV6QBYw92JMjkglk2/T1pVJPdrTZgZCng6OdCxDRze3/6NmaozsGxsyLh8Yta10YE41gAVj8yMbGZoEQXD0QPqqfgIAdI/6ozUh4d9AP/EfyDD4VIC4qIyqsw6djRwEaS2p9tGl8/ErAB/JGWj72NGtcFQmH6T0FTkx+PTBlk2lMqJ8UceF3G+cCyS01E3vbX2C/j9xs+0Q/pRQjvMmUjzzY7yDKWWeD71rsUrEUCT75Fy6xcJQ9SGBRHme8estZ1i/gdDpapA1UKHI1duwUNezDEAu5of2hDhXbsmzg7ATXo+vewpLD6VTnVNk8aLL1yEB5oUngmnmF9/82B54AVYDucC5+KySjmX+k2rjMbfb22HGakDmYx6z9esKQY0nyb8qWO4KuAf0bWxWjsD7fidUBdIXH2N4IsLjGFcred/TOKDqr7ZpcV1zHWCJUCl4XqM5iVdRKxMdHPPoUx2J+mVkE8CY2jToa83mefNqa4rlcXMHlSJ+uQWn2CljhfHztKwGQCWUyhSy3xSzCDHNxtFXz+tYjqVS59dNfb5MjWR4vK8l3LwRHjORxnxF2W68tdXKSo70NuvxbMSAfVPTBRcib993KlEF639gaJsI9mh4VeOVCbflWxie3jVHbR8pdVlCaeoofx0M5HvOwlCj6gxt/IqCvAaA9qLauJwnrKS5NrxuSpRCk/oTbRM3za9JnxnYubx8LiRy5bs2/acwoevpbSlIqjYDYkSpFM9PshvKpboGmzL+c5X360JQ9Kx8Og+XP9Hd3F6L3FizkVZD8VYzIkgCsh7+/bwog/J7RMDRfASSnUJUkaM2/8UScPt30d6ZfK3kNgIikNVpk/TVg/mPRj2YWKSMamIhZhY4RthiBwvV0lXNVDVrTTOi4VE4Ckbir7Kt6cVGr+JjefcEuANM/o+ThKVAGORVHySDarsXRR+idXAOFk2UaYbd4zSaas0UdF8tyN610NYW7ppL43Qv4b7MWENI/2AZP8xn8eSlSXnBgRVdewyPdIED//pB2jsZLqmYJPhSPb7WaLAiXKhbrKl4uwNwNzmDlx0bz+PXyFIogbG8YVAtq+nZT74/nqkEPkOVhGjGfPpYuqK84c9ZiySrK43WXA5Q5zT7XQfiRY/PdH6cIgw6qrKDCdiFx8ZqjHKTmLnw3GBYacmcNI8XlfVGLkp3FVT+EKE5IwZE5gQgNy/Kdg+lPPqG1W2httSbwXdyW5ei711NaCN0nxuuLUeb3BNxZoEFO7cTr4W15ZYDW1/kvhtjQvUqd/uKJd40dFlzkKRnPVtNX/RcOvh+wpulddD/+mBMoyKJd8ayGi7vprRyFvp5KHB14VOaGmnadjgfA+Lfz4We5774l+FWDkFbam6LtcswNqCumkbdS5Bj4wR4vAIosq865BsOYWlZbz4zwooNO0jWugHjO7lg3XjGRrR9MhuGzh09wAD3fZXT3hu6KDAItpaBwoRVI7ZFEQf4UBC5Rao/JhAzK3+XIilQ7e3iMLPiZNXfjhSfzREs+MEQd08getoOe3G0kcLEV2I6Oncq3jVK2FtY7KqXauA8OEy1s7f1wPxgOAio4Na/mhH4VD6hSi1t+mUowyZUQxxa2e1VFkpu1Snso8Cb9fzrdo2VgfedHmksXceVz+TKgv9usEc4T7oRk5AC0onGlOW7dMOJ01QRi91DFHWCoQ6zY0ve3317gJ7FGX6qqFqQF37x2BecVgM7s2uW8mWQcfxejS7tfWF5KGNUCMUncT/wfiao/hJc0vnQDLtfhAdcn1xkgdAGYV7XatyBEnFNihiylTT+YBhOvcRm2hO4TouwGkG94FD58PHbi190bfsbPyxsHHoRhOAtjnvCJeHox0mZFMVfHBg38lkIHHvIabsBeWYNR1ylsATmnTdqneuTTv+ywVkUSot4cY8gyacomKzNzKiOu7EWdhz09rSNgknfwWP0cDJi+2BxivFceNiEU3Jbag3FTd3jqVi36cURHzYgAkO6btSLhhdwi0ctwutc/BV9/5nIr5pwoKD49g4KWXZito+yfHxl8MqiDXwePXckdIS+2BqUQt2L9pMqzlLHTwqxjRibPKSl3noRZjwNBSQyRYAXF2uvVrY5BK7IHrHSuMkQx/hvi8TEAT2mXXkzpCAbjsRPiIhA0AetfXIyAxLwirRt1Jmr51vH50Ct5fx03MVqvyodRkxflIgdwA4N61oK7+sLXGBtiJM3DYIseDBLDpXId0GwkZh7WJiFypiKsyKeOnQ+wjaSYytckuHqrbrwhy0u3HUKvMPu0iUsle9u911qPwytYpvhlO7pfk/xhgIGIMPauUiGoC7kfNupLOzA0moqUb2i+1r5XKmoZJqYsnbq3MFzpjXhROrxaViEYRlX3k1zEg8ibRd7q3PQwT3A7HAaqlM4Jw8PTfhlaiJSa6QO7FhEVUz+u4gIKd/RTLydLnF58H1qaAMpygI+/2gQfTM58jORP/2i/aaJkFAc1HaQWqSENH7crlwA397YKrx72vKCk92ZuAX2NTQXN5HsnMwwD3tfq3/CthaniJvlJkFkjy3xuNWg0f3DKfE0bsTO/brUd3wT0esXGEOSCjVj1yfn97ze0Yy3AlSw3JkX/Jhafcbs+RO8K3GWw0H6gGx/ATdi7p4o2DwtB7rEaWxP1dYZIq+bkaNGHuL2VLyTnq8pTWSw3lHaMA2jLxSvywo8vNe67h7a/EZiCvtf4Kwhs/b+bJN/dk80zaWTImRMDfI/jDaJlbPldfOjjCm9zMY3q6Oo26sEHCym1AWwKDs/7pIB4kSwk3Exk+8pVIYbDb4/hwm1QHr9P7iJygDwKYaaPB86DCz828saiNAh2U9ALEFMRdlRuITxAUZlrLkzrJcjxmCU4HOzukVX/Nqx8ueEfz2lsZaSloUSWRCOkPHBtHxKIEsdFNzlEZ+wu6mC2soaxcPHDJR7wjC7D6qVb5Y4Soj+0OVi9+kPy8y5+hsA56PlVzNi/YFTO04+zloR2HrBbLWJe8CU7rTfYPpvY2yu12cJlRfweVVLRUJSus3MU8s7Yr6j9ecWE7rx8Ruz/kDEsQYu5vKqGQVM0wCIC6s1NqW/tyUFZ3yUJVVBnSbSQzyR7z3ls0cdix/BAkhLVzjAqreXcULTjMZLmpx33loan1hLd1kjxWX9noIKd8QdR7PN+oe3E36a0IvO3L+uV5oXisXHFLrzVMqCH22OgbtRowvFczFnMTxY4bTtlEjWMotwnfDKatbhUDV4Kis59n8kHj113qH7ppx0oMeNEd2kET2bMPkk0UDfAZ+rAMM9lJOu54tZRIMPIBfjCcpmz8gZR0Y0ILF2YDXpyt43Hb15MWs747I6FY9K0JvpHJy3L9agLOOlpYjZ7In1DcXi5m5rnAg8FXfrFfVRBd7edfCQNKPRnfXhzVD5MACRTdJWf1kT9uVMIkXqTAslxBlvpN9euJ+aGKYQBSAZkLHXDkYJt7/xNdhI07WNnu34DhoDDkg9Cp4AxwO1T0wKDnWQp4slBUnhYkS7m1ZWVjyLS/f/6n6mCHnvwCeSGFYAXUIvtq2JQ3lG9CBD4sNSmPhJWN4n5P1+bk9ZkW2A1stZq8XGCy+BHc5Aqrw2UfS0AP2RwXxXdqdbkD+i2g21wl+mUMGGwYB00MODP7d3nHkjYQElQCuFdzIfl2m7m7xN94lN8SUmTQfMTbzTgpUEvjpu20q28B8Kq+tNHRhs4U3V7Jj5znV9CSy/1lfkprMpL4rkjUtqQsaC0Y7Ide+AjIfVltJUSa3dqVDcPQuPAaBJZQ4iDc++8A9MG/eZRtcTzuLmyONT/DJz6Y1faOl2E5Dl1awNQKQeT9segVmxL/DfL1K7XmSu8FXtMFQdCOpheGseFGJ6LzaqwZXSlAv5l5LLNpigqAiQwOfXN/E9aP/KTnF6Df4ZXM8UCNl9+YTqp6m9hWT9tUwtSxLxRX6wytiZm8nLUZKpnPsBjWYNImFO4MO5kEbuUkAtEPTrLPyvGz1YZSbfGFYuLMRtJvwbmpFJlDMB98uXFxE8aRbKy1eY+xKQh3JRGLuDI1VrTJRtxwSebu24xw1CmG2+pdu/533j/1DtAcu+Cs2smEbK3x5CKA17AFhEepXziDPKkSZFmVHbgR9zZfoHynvKqIGRkzFxnl0npyQhaFCqZ1pLf1P1Ib5PgQ/J3Uw32u6Rw2MUdtwxmyYBe1iE29ZDWNfutO85p9RCvsq9s/MrD4rKP/VSSl3DXXwu4uD7+6bofIh+U6LQfVql0c5B5O1BAGAK+DaiENkL7LykfNMBYOkzLzdMhpZ0DpCWoioao5gwjKVi4RMVJHubBxNBZ+gxJePvZShRyBL+bbidvC6AdSKaeN2aLKoEOdR0PDf4up1CEb76D2/DosEdRir/vQQo3//219L2f8yLtORNgvH/Iec1c20cIYvTr7K/PMel89/JcTMYN0+EjEvQN1pJ/tuk9bT4tJV8VUcFRctabePOBrN2JfKaBKdyrLah1lYTOfbQrhmEBACi6Md+uaPvRijBDYoDHIvitSd9GANQz06aa+HeeTmZrI/nh/Hel9g6j1OGDtqBRosEOpsFFAhcEy5wNCvDyJgtvlYG8Mdv5AkYR8Wud5xLTMtweqVhAHZOPNtTrwvbIMwOft58RNbfJJoeuOmcxiH7u1F5OhF8W+8roD7ByzBh/aukJzjTgQSbDR+r5p8UL1a3kGgEQOT8YZTxpyr45xOs5UO7tSWGyNGpigzke3zC+aeCP6ZfTYB5rdljh7CqtTQXxcDHLHtr4sODRmimYu96VpcnFe93s9o/GxmCRBOiFXSi+hjrZTGDyW6HImyxD+B5yguGp3m9HOpiGShxExluI2jgfT4gpnoo97x86PhX2zl0LgBodHHsnbx45eFrXiBaXW1owlV83tKmKSW5iNLLIB2Zy54aKRCzsin3kDALF14IGrOORL+zEb67c4t/2vZbd6+Tbi/yZTBoZ5Zy0obpE6pE7QhzvwzWU5qbEsZeQF7sSdkjvlQlVW0EsvCZHX/1rJHCRbJ+hyPO5jZhPW/zgmpCK56v34g5/a8bVFKYVOqTvwqTw6mcWiLkumoOX/qNumdgcm6pLu8b/DocyJ/tX7lGcFnDAjAweIyeTgJcmd6asQy0FFq43M4a+6rRWsrQmSIgNABa4lT2hUlzwB3+cITv/8BwrZwcIMGdcoo7/sQMaJBg3TIZVyR/bXFbtmNd1il74WGRMw5F7aY49TT8jakWY2er0z8ilK6Wkwu/cjm8C8OKVcv7WRa4fMCwW4tvoxEdzL4Q5/0iuXzwA/mJ8lddWWn69z/Jx3bn5ukFSnnxEfA3YxeWjn+tv3vcohOZfWguZWExSjLVnfKNl3uk7HIRmjjv1aqrpuzEmKZ3UlukGXaAzYT+8KJGkjYBr2AsxNGsGKcFiHpJlsxd05fyGRu23018PGNAGAQcIv3hGZIaIluQ/DVZ8UDZiYXOucQoCdWc1Dvfdx8Z79TS0vQy0H/xG5QRRxnYWrTsDUw5OW37T/Bc7Qh0Yhbtx3X2IwcBPiIyobqHbqnCQxCPa4JPa9jZwJJmxd7CbwswUMoq7ymsq1WDxSq6TYq9RmOvOkHNZ/AhkrCh4gJWOOg88vQ9XkGgVyg4pcNJv16XHkxi2WQ/9npJ3agYFeS68iXjeFTP1dgnNaZD4RjB06ChWbULkj6dTVUxSTF6GABx32dZDw9r1QxdCGLHMtHGz9+OkUBRdmVhBKSZkPynOj+xzw7pcahVC0QC5BnPv9Wn/h4vnK1xMVAfqUMF0zYHDD0GJj3peYRdEsez0A7R9vHwEl4iy57fyAO7AVIQOSIT6cQgzVEHcym/d5NF0BkajqiNpAK9saruH3C7Ei8QrcEmPvMMjZbnzxiWx5sTZUXoUTkZ2dc7QscC6ab3+a46dS3zAJ4n6peq5Dc/m5/BICoB/ftj29qZUOKMdkfVQiot50qz3El0T9THPG0mHwVpBRLx5oeTxLK3L2MnRsaReXWb1IM9ZQFpHj3wrrT2p1nLCUpkfTCtd/3kAdBjuI7kECj/bg6JSPqZ58gfGCl9BTKUZVbSyeHJS5ulq1R3bI/KrB5A4EyY/ZySMr28oGTl4TUyTyyyqda8dvzOcdUZax5L8uSYru1BLSSTXVU577pJ6NYPqOkCUjATMpukUyrfd4xFHd8X4UNDFgf8Nv70QP3SZmPPODmopayocR23V5ZWftKdPYoshuEF4aoKyWkWR9O9Fev3ay/4jrsSS8Eu+Qjhxqp0Rufk084535M1jLXv0hUU96ehuOM6MOtZoXnX7TX+4l0FofsR1KrdQ1J+UvDrJH1Ni1UH6oE2QGmR42FYVOwelHegcHGSPgGH5h84V0jj20zpvEt6XWGILz4VFILUB6Sb4qJn5DOI6fTXBbLchfQaXz85dFbtzoj4kTj82DfErxHjRBmUl4oZDLXXe5d/GeE6C4UB7xJrRbX6WcGlMydKaKVDlYDSVSm0hqrDGVtR26LxZCNvNKs5Px9mrKwZqOInykoV/hyWQs0ENG16mLjTG6NUj3kE7rq52XLEx+QE+6qsRC6USEjCI5L4k9wmZ0sa2t5lBBr6g7FL2xvlPnXn0uI3phUGx7XTxOHrf0NNlLCO5H+bUlRZVZ95edAiJ3/osnKgMqPUyemB2lwSZAjs0DGZjPkzHkhKZnJrVc0s14vxfmNlGIQD90Vi0uwdR/g/MEM0t4XDk3hjCymT8gg4iyLOUcoYUyqO7WstkonSzSOIrAT5wHn2fLARaj0rnBxJuWIEx5HqKwyuBE0Tsq7FXRgqLFwFfJwstHOdqz8Ck+GA8UdUUDXgqjKL01Kz0UjLmfnQD5TkeDcXzApYGTuf2JtKnsq+zXa/qSxglEprYQb5/qYJEGYttCjs7X+P7WQdYJIYFMEXVtKr1tJ6WxJvngVUSmPQ+WBxRzxNi8l/V7BBRKiA0hmSm0y3ChhW+BrsbIyFsWMU1uFW1pcoX+WVoSLEL1uwIzgKSqSIzs8Tp01I7TkYNctPOXcADah9JvYlWpzuz5zzKeD2ZtNrSK+oGR8KHuZCZ7Q8KijtIyTdO+jb73/pGMGCnBOL0Vq1Q8G+d1qRLWtuUgIUIWQWH4InTTgb+K66JhRYCLQIG7rLNRx+OMa0ZOWU+AjowFJEO69Tkty2kYeb0XhfqYpH8hRplufDtk8jjpQipl6fNZBaeY2isMPVP4TBK+kP/S0Y9G22/RQYrqG2H4Q4ygxMGM00I/bLh7oI5DhDJEUh32KjtUnvfsuQkSS+yaPdlMYbBuENxHHWd+N7qHXuOQGki/F+ahtUQm02+GZ09L4RezIwXo8G+weakGAGJXVG4g8pRpI4gK1CnSsz2zKiIcXyYiZ4gIdJ1dlK9vGL59nIQ5hr0gMDzbK14Ot4IYz8FqLMy7bnlY1+jYyCJlJ35IA2TbgTqFITsGWCFeglaiAHcsQzeS1HVHQgc9whT+aGTfRecYym5fT/td5YzJ8b1vlHy8jkT4+AzzfJ9wp+tN2c4aeSIj87cSzgi/NfMW+PZXjoz/Kxp9Oxp5q+maoMjyTihn0y+LZ5qYT7uLbsbNyDam0zMJ5wsZmNHAGn/PGyrCJKuPnnkPbCIk4Rd4PfIf96EkaxIn4wJL1gZGmLzgZcFFo9a7CKsNMVR3Wa7Q+TJyeyOfD9MVITDSjJyCgJ/abpVEGa5M9ZKwYTyyFhxlXWQf3SZoYbmbEvz9jQvxVv7rBZp7V2J8o65Me9J/x+1mltvE9APfNPTYQR73avZGMlDaw5mjb/WSDYRhPuNLAuxBdERcISVwoHAaGzJDZnPUWGqs+VwvHRxP5ASBV7pl5BrW+mRoOQ2zNYumIGWy6UoA41fXaXwyjTKhV9vBbxd8+SBSTpePDOxoDhCb5CNXsmi19P4Cfqtkd5lmxP0uvHhj6le3pYB2Vd/RF1aVeN3alxJkGkG/TG04aYfckhUuZggIZxrmgLSDqDggfowb8kNMoRl+BlCv89luAkUzCG7FYFfD6PGDmRcGaLtG8AemvEQY+4cgPk0C2YZUWQ3u75O8lGZAiWYw6hNBNle/F3O7AgHnnGImp1sMWGuSlVml/2H0XsUBpbBmQl7+qW5a0H78ixiAK4g5+zWnq72TQhIBf1ascTnLARIojcxbHJ1/Wc/fJMpekszFmEn5VknUviXSrrcT/kB7YUvQuLJ7fNlOKFka83WIX54+9N2IghxLGDwwUsJ59Xgiq2p+ymaQn5nynjUvBpJqkZ8NdMujN1vPj8WqFNpJHQhYX23FtyZsXBxGXbdkPav2UgpRpSg+0ZSnd4OMwKytvbDs8ByF+8nlzsEHmDXqDmWH4Mu93/fmsr5bLLbSINy6gvRM5wA/E1h5mV6yJBm8AAANxU+hdz16Xfwv9Io7OUEkr16MFgZpAmq4cfPmsn82ebAq1qdvz/BLPClSMX70P63j/hoMyo98jXxqN/FfPRBA+t3Im04ovH0kma3OQh12WB6EzSYs6ejj1FNyAxWeLHz+7bU+OnuZ6gP/P7lIaoAqtGoWGvlkGVGeoiqy1JYf3Bmnr4fmgeZzOG+iD/WCeJPVLRfiuc374g7Kxk6Y1XuSUtb7AxcKhOacujuKzCSXJ3YGp3+VjCcItdIVM0WYU7p9k5ZVu5SmSZUBgDCiYIdnpBWXjtB1+3uHWczEeuyWnPoojJVHH9e5jAq/7NoYHtt2bMPusMRStDj0ql/VO+up6nv9Z2/G4yqxe+AKAHvGmgBt999E/BIJeH7Y6wMdUs3P8IfF5n08rgJHcNbU/L2wAk0T+Ifunu0TYwdfGHhSabLeZaIEmDARdghDfVfxeVdeXKzXK8IW9yaGB583SK1sf5GOvXWXByUzQoRRvfAhRq+FhgheFRe8FFscY9/aT/yYU8nbOJq6+uk3L027d8BTJ2JTiXc28lMvAGFoo0mWCZTugoXfdfYBkLHtru7l9cfhan7B7SmX7jJJJ5tCGPmHNynyJaq9GwMPtndlhuDktuBcJ75oBlikpTryor8IEeSOX2ppxZRluc4VN11stjRvXn6P2fv4493I90djgupx3GLCJr5vr6hzVMI0u1BhGsqvXrxheBYyCly+9v8Nt43SWCZ2BJIn663kIu0kAVMsIkc/DNh5ljS3QODZRBmNmfrAgg1o7dkD4AS3n1gZ6PhwdIrYfgQ8ZjEBoDAnP+JZuvXHFh/VmhAM+VxnoFovFhK6WcJGentFkkrvPkZsxIxz+kIH79OlCMcqcwykMQVGudP5q/L31I8FOf6HzNyMfS3GR1mfLUE3iZnhWUfAPSaIFzgadeLc1NPM448k8q7tKJd7qL0Ns8low5HjRYL7V38IU4bEwM3CyVEeaLlsOOArFU/DArVT0JqUq1XKlsru1gm+l/2fceaZ6YNnHeensFYKTXlFMYWZRZknvuV7p90gBb9KXy8mllofZmD0GejsdOg3k7WQd0GO1CypRCXA2N0OMnGxo9QFNrLavESLpvTPk/5EIVj69N2eDdCjJl6PKyLzJyBzzUIJToQWoNNtDN+dMLrYZ1Uv17ZHSoj7cj0NfSGQWllI5VKBTl3UpB/zBAPO7+6fCh2x4CnZdnflsGWSAnYVMuGg4vvvs9AGvKYVxjb20UoEUJAWUw4oCgHGKUXPrWqgE5mqRnpYh+pgLl+cAKJE+lRuF4E8XQ8iUkWBQcA/pIm7tqyPj/IxDYdxRGHnf+y5MLRIRxAFd+HIx1ZUpcDR0GvsuvgXdVFHSRWY8PNdOq+4jrSkvK+fANNAw2b/bWXDfx0h87vaJSo0CtOtdCsm6FEJw+NzLi6hljP2wO2Zx7a8ucDRqjfyasfc0/5XRMvBgsBZhqcUCxeD9m+ZpB2rUEbgTEiVWc//uln3UXRvnnvajqZZIV+nil//zrTCvji5nEUWGjTXUYFkq4tHfp/RPZHiUDTC3P9Ovy4gxACbEgX2jIgArnYe605InR+IP1NZz3zVpYI9J89vYOJlaYFMCMcxGWxh5hf9vuzc4vs04Dl2Ja/Hc7u5YERcex9BpyoPP2y7ofMtppunl8ULX77zam7yBJMhw7ylnU26rFnupwg1pZAUqU69HNfEEtyDf2ibpS7IFYw8ThXLdbDBzyKNcmB/nrjArQNtjmM0T6e4ZJFOknd//KPBJCL49ib0+gkR7BGKPQDK5RuKkhdD/ovnyxVHLigzTK8PF81LZ5v42c/LY8xyTeSxr7jM0ZsPK7rIWtU2ch4vaXCdGQiETsFI40Mx9kjZGkc5mBR0zV6Qlb/qMzPDZV4t9XDpJoVC3owPyiWRI262CsMaKZZM47sJAPfG4PwJXurWSBRAphQSFE9HOvF89BXAOFAgJrlFy4YEVcZdnFSJjC/ord6ckIV5CGxCcHAIs/fDXK+casYVf4kx+/Pq2xk0VXbm7QZdU5j58WCdXR91k16RPOK6oYfR/GekHBYbfsRQ9P25oxGieElbkCMOW4MarU5InuStXJoxHWC3GJVPMKAcnz/BW0wK7ecknMnNkuoNLuonYTtb/pm8TdkdRpV8ktWrpGMbfZJKV7FecYofBFFx0hQRW6mqtUWQ7+X7R/8au5OPARFN2BEWaZrRpBWWQ83n94tnRRF4ZB8LbBcyh7uLSYUH+IE2rg4y+tmSANuEAmMPOf8gCS7h3YahDBbY7bwxI8v8uuYyyTEgX+pdyhmYiU8Az+IlgqV/11SbXWq3WQD5JlONAiHKB4fcn0qb6kvzU0IJpSFAr/RASy6tQbhBEFxC4fU8ykLNccA3HwmtMleXY19D9TiNYgocJNh28FxsEuQm71ee533nC77H4svSkZ2JGlZHdBYib7DqkdjuJcL4LLAotIAFkTrcl5UTyZG+nbvIeTvjhc9gbmZwkiUHdmVP3evuqkOTCHeukuoeOQK4TDPjNlRdQK8mclpCvT0IOPvpR1hdI9XPhGIWuVjI46I1iw9mQEXqjCOlel86uv3CgLuDQrqAGY9fsKZ71hdLFVQx9BkNSPSOA/RsufUkzJi8PEwRATJmm6XZ3yOhXIE088rKOJ+oNvZyIuj8wiqDgoshoPKNym+FisX9/tLav9RnC/qv2ujDr1l3L7+Lv9mf+zBfN3gAv27pVcqdEPCK5qO6AFERHoWBqO0vYWFAR5IJyIRrJWZsAoU4l/PmfYsNl0r8ePbpnrRksromkzKKWLLOGluz3YA3rCG1VRfQMivHQQS/r+2LoeiDXO6i5G73mFooTzUtXt1qljoSw8xxbSG0IZk2k4Eg3Sxv+HN8c5BRmmiM9QIoGpytwVKIfUqwG/NV+TP1phWRrlUCe9YwQ5Q3kjYT75OIErtCcwDSeTjd25nOb+janvOJQRyrbdGx597/BjVBhabYy74AYqwiBFEIvpq1s8E4fOyhXrJbZFjo37Ox+aiDmACAC0XkEwJ2boUvP+KZbXxzgk29gjHTCAgMJQdkBz8D5Oqfbufsc9COpoo0m6JI4R1yUKnDiilTn+NnnRkhyubJRTNhYPfUaYIRLDi2KJ5XCIgBXnXIBh7QOtTjRUTKn4EDXjeF3rc1tEJPUoRCq10eb+loo01V8BO3tcFgZ+2fwa/DWT9rA/EfWBcBK8/nTkQ3DLyylPvBVVqvba+MAL5wAB9Lw8uG8U65zhCicUJOlMXSX9Vl92tmTE0wR5C0aWDdXk67q1WtCB7LhBZxIwtmWneZKVsW6sRw8p5Ex93fR/bXIY0OcIcI6ZoF2+fIVgVltk+q0R+8/LCoDKHLPmB/dJCR1dNwe7LgYijEWcNUZOd3Iqz5mZDPydA7RzQKrpnqTuB9W5xeeijrw0OcUBnuuWaddH7LaRlWA3EZFKiWSaOXl9QREqHvZaMkSiIHjCrGp8J82Zc9vBV29r6LsXW1VHj1u5bYCkOR5iwvetpH4f9aaSQYrjJmuy4Cr9tX+l3Bait4dXt2cBJLYoenC+hloHZEmD4460UlvvBRg4563YTFftkV1KtKH1Dh+VDBU53b9QltQmY39BLBciYzA655PT7BWzQrrRQJUJ63B3NkSJGlifm8JcM1NMMfbAkhNS6ZxlNl7eldXpDBO/KdQtWZWxuUvEp4trF35A7Y13PDfWAXc5peipNdjAE5eLFjbK/9+QnzhVjzORcHEdz4nzYUodqd0q++2Ugf8ZCwnyc4mksH2Co1CkzBJ8AGWkq6iX4n2WL5ZK7krYVt37yXyopK+AKkCf875RqVew238CcdAa3dI7Li/oF440117o1Lg1qqCq8CJn2q24M19lbEd4T7dnLMKy5GCe2EQA+oh+3LsSdzkX4qJ6cUSTfsrjd3rSIhyZBn1AmLTuAOOp+nuAFlHODFe+Z5ZYtg9sOAD2ptQRIKz8K0nbglLPfN7zduAQA3ZWoCPb5OAu/hF649T93dlS+O/DH33e2q4NqCE56nBfituBtZINL0kRa8Nxc4UxA0yO4L9qAKzX0CL2uZ8wLUUxHhuonU/Na432uJScF2h9az/PRqdwsEQLIwzbsvTE4NNCL6UWsqs7+ZpLyimwByS9LZXZrbjR3PMQPlgMzqKrpHXgNtvLxMCtBktK1OV90NjMZPVyngpBgRaaRI4ZivuHXLYHsqtzN7xpRXAAlY6XnosKEhg1JIktpCwZiIeiWysTcfME1nCAfT+Hag6f8VWu7h4jFToQa7Tbn0WKOqCiDf42jfPFHqyT26dxPlrop4uDdacDn90IArSSCGaQla8BduSws/qjSYFCvzdrgE2byNGiCcq8k+lynDiYPJVI0A9RerBfEHysisVACIcxthKvbMaOtrywMB+dLM7lIBwJpkd1lsCvtsGFAZ6qLxPwk5F63ZD64bqxYXZfXO64vw7stB3Mtmy0rIJ/aLYLbHKz7gC7npa6Wq9GefGz11H4DBW5e1f86xuxq0WWjqyQssCT/eoXKdJp7uBO9RQVpazEqY6VKTt64ovEjVB/aw85fhzpadkWQ6LIBv2r1xN+EwJk0GtIa3vo/xoIuLFcBe5IqlGgZ+TUa7ACBuqEbSpyAtXO1QEeHhquU6p7PzTDNeTfX5aKglkrB0EiybyfZrZt1L2yGI5OM8CmUFxz32KS9WYYRddppsPXKq0lek3q3Fkf0vAO5ODbpVsJTjKUStOqPPDDT3MDTwyp/Q0pUl9Mb4ZWTa4aZsXHOTgsQEPMAuyIvRVagdAJ9i1ZM0JwuIXoqtjGXYedr13zxdiKhuw6v+ExksrxoDRIBZVqFangoZzznt+FQAM7EnT2fHBeKkNZynMXz83dfEoPh34o5uAXAaa76xjPnwYeW+p8mft2GC/BL7tKIrEsWtzr0uhf6xXRy+ZHSlshxF4sfR6i633DBTbJWabRanJE303hiKTOSp+mu+gJK8KhUwBwGt6N1ye3cXroKQp+IUBFpLcwjx5S/X2xeAzmsXmt3FWTVh19qd6MEMWWdXWR6ylgby+kjyQwZRUDAFkIhFZXoTNcpzzUA5UpaO9pItGHh6CxM/qY9SpJooHt6gbpv32S9lqByVcAdL429/vYAAl5dpQYkuM7Ch8XcfbMU6QV8utynykrNVnzX2TqUr9hhmoeg38eeM1GPHkiAbPEohwoEsG4fLcTaBNCXSWttYLOs84meJ5C1yZt+QUhChZn4UYEhncxB/KR/11FGgKx74nxusHYu4kdNeD6oYsr/bKTXmeoEYo+6N6K6cYcZsoVYq3mN+6wCGjXiUt4xU+iMaHJeW/mcOAgM9860T7OYqyaZXXeOK4O9HZWk9XLpym1hBDcHsZ7694GkB4wiMhQpJSW99NxDC8fw0RyhTGR+HjzcwoSncPRtL+MAaFHpce+Wf5KE5Il7tmUYOH0OLAvnXSey+vsCxjYOQa1UMCp3tOKUd2uTbUE+YYYxzA8Hn9pZEXyo9tQKBL2Ko7LhpELa4U+figJEOTQmrUJd94iNsrht2PES4mYzgi+oIbsnU7Yw1lt5gKhZWdh2Jxrt8NxR7m7gDlQfb9wU7Ax4LvZbqzY+jcESrromWAn6AeBDKr1W9kd/s2ucRHwClstfdnv9fS0W9Pm0TCfFhdZclzaYrafUflyT1mO7PfNgM7pQlW1HeSHBK+p1rNs9mwNjW8VOtY9WE3PyHx07lsmYkr5gP8nMc1UAAJ+0jsRkjnlvH0xR6481CMOXuYSR07R+UcwSMFqp6GY+bk6M2IsUKIosHrNbE95jdKB+yF2cF6nuHO5jtruDBoPLlr7JXzLDuUP4VoP2MohhtihUURDvfqKI0ReH7lN0EpBO/kAOKIeRag0RxyaYA7llzfl80/N56oSFh9CaysDp96xjI6CsEJAXNgmg4iAo/l0uN4dWHz6KYGu0rWLdXbCKTyMcJy6lZ5HUJV7x0V/KnJ69XlQEjrQoCBSJoArLZFG02tac7Scj0amxwAwXIU8ZnGpK8NRfkg7qo2ZTqhe9XDPeV1tk04+dxB/CULGU2oZc70YUBFA0M/eO7mjjR8suiYXdhpAv4LHX4e7NCQNN9CvWFSDZpzNvdbmKzFMNKKPq+ED+OY4Jq4bIulnDzyS946OnOhAXheA17pz+udvwRMttNHXLmlVHJUysD9BNSHQsoTbUOiXLi/5RgqD3qmQVVxKIgUOOdAdRcXmp7lx4CfWYrRSDCphcsKrDYPsbYDyfpsIBKamPhrdlIVlRdwjE6ja6inD/EDlQfsqELU29OtyXNv61cx2z+s9L/QpTHGEoniq/BC/gVG5a8fMLZqKDfHaezFeq2gftkcZ20f438KUJFHToCuFpP/0FvAWsYEb47kOSV2ob6lwBegJQZK49WqbQwecuKJwEFIlNvza1bmRBSXIt4E9BwTzRSclftDD6Bpioqt2X5XNW1ESGC9CBjgi2ocBgMJrhZZ/WxwSiXBmlaPWU1pQ5hRR4Snx4gUym8oMKxl3DEN6285MHKAfSrvdAa9cvMbh17AMER/VFbbkrOAWyRKCW1ZtsfRQr23RK1snvU90Mlu684wBG3SbrHDQnbhJIOZEka5Owl6BmSVQDLwytlW0aGcQgVRQAl89T+XrUqDnZ6AumaWKI5HYYNr5pwcdc4SkezIMaHbRC0SJ3lwrOFz0WwR08Cf0TITLi/XUbGPT1i/+tFXFwn3xEicCqWNDXVsp0bBvcJeyDBTLbft2rLDuJpBk5j9hILaPhrUiYIKcrEPzb2QjjzGkEJVczLqIeGSbmk17E53SV9OEuf1eQLBGEzWmGcohtevAa4wjjOXt0fYo5WzRHoBghXRLpq2Rgz4eq2x36Kcmt4RLnBBYhzydGApbZgIaOmsHWqWpYwjXKBaS5yIOjNFMY3VeelvZG6HrZJSnTAOkv0y/ihmxLlRnKSsO6sS0gwxhFf/hpXK1sy0w3fznXy5IwZJqP0vHzKEsVeTfZY1QDBEfjzSzFACPqean/EolwRqmJVePSrnk25K841EvoCS7Lkf+yAHkxmJwQq5AjCVO0UXLTsa0PkMHHjdaiZLb5ugMlzUbggPYeqh1u5PBfHwnkL4gAY5M6et6O9tNSzDLx8rwDbEwATBk5O4+tZt108/la0RfwlCr4C0dqeJMYVIewEzTR5UXVDHn+MvlJPoi3oM9jz6SVCX40hf3GGDJfg19n2RKevvhm1CSxjKY+9Jvzhui0CyXOMluwXHwcPYWHGPxqrAU/Myb65fYOEZIg/yJtbI9r21ZwY07oMoxD4+fjMcsGvBbwwRnkzVyaSmTJqI2pxRTlg7hqp3GLZEsawv4P6LpqLuYgGH2b1ajncUUr3ZCu1DPgdzYEIicJSPaSLRSSig44+WW7PrOdiryOKZhSGCFfIxSYOowO6BnajjXiHDpGFBc9kGi1Tc+fOoLed+gu2RDyjowLGS33yVvbulBcgBTg0/bzl+PgYj4vqukNdPkj0EWUGkyqUsx8auaHcNnI8m/ObYrwi3kyKy319Dm5v/PeDYkNI9+ChLte8WPYSQoQoYd1ORoWLl/c8z+Fd8udflImgjg0TIN+Nxnyo0GCLwj7pL/KcIovuRIDnlw0aPMZ0+38lfUD/KihugyLrBb25ysqg9BorQALQi7yZKV7w1Zwbe4XY6HkK2j2/yiH8KeF+n0sAzvmYaIVA/8WVteWoW7Ek7etyHOAadp4BcIhXq0IsNYnsWarV7yyCdF6sRq2/nMfmEw8rCvVFCk9H1cjISU8hIJvEK9hBWk7Iqydw8ntfBldzsmfBOEcg7y16mggSxi5UwE9g7927g00GWyET+qDa9kRfded0HUOgLMghrYd+XaDwfkhH0ZVrBZZHjmdZMcI4ENt4mJ/1IH4ThdXmewhMXS+tjwh5nRVhkfmrRAglOxZ2nB7gBX9OioStyqPJFIUtcu9v1FdbGwhGi1/Bybsc3yt6yHP+m10b7yUr5wusTRVCbVVMlpSA4velj+P3KmgHaDAKh8gY8ugBg3+xI0PmUq3hdApHBDXJ07OzxzVF2arIG1SfD+7qseTy+s0VR9y/M/0XA7FO7Pk+5x0TjbZz5duqiNVrmbo2rxLF4f2a1rlrv/nQWI/XvxGIsiBeWNeSqSH2m5KSVQG4XhqfhC9GcrAp89D5VXvTB8MYe0yhMTHKd3X/ZvngGUnVA/51kZ3A0KA8xIDl/N6ZtFkC8oC/RaHA4mX/acefQ7QGint1L2PqPLE+W9cBC0hegXtOD0RN61hecWgig5SVS+4GAHF6V0JBCcesMdj877mMdXqZHdDvmlEKeUo3uEN/wz/UTsAoK07ocxa2VajFu/hWE/UzTy7LKcShK3YuZh2dCKSv30aJ5aN1OVO/KNucpyJKfEGf2H3VzQA6j/cLdn/qhk5di7jNguvqQXwg0djpzNQPahXPEBXJqqvcnmfgkFjaKxHBjQUGa/En83ULWt1IoDCIf6AEKbpOZfU+ll0wW5JzYWVxQthQMUKDGT3M42eZHv8Z7VYu8qtiIuCScQ+QZxO1om9QWLCSpSwaW8ijXVT9ND7PCpmQB3SERLpVnUpI6uqH3x3Mob81rbtL/tAia04wxofOTs5MqC93+gin2XCyBg0HdRv4XnKguhvSYGiELyaZ978utCKs7/9ANvWN22+tPAxzEi5kEp0lhvJJ5kOCtj8qI6ma5US81TAqyPCR2AJ862shiDwVut6wfMM3rjrnL8gJ8hCgDYTxrB4kczrkAvqtTFrJueG8xuEKIUOPpF+g6+A4/iyTQFk6nO+FLJtGry2uYz5FzYcal+MsJXvXSLnEzGbaq+OfIJMYiuIIlzVy+OnzkXIl5g87Pxi06c9dLMaFaNYPzD/T5leGBujF108lhAF370XaqRKYzxRLHcg9lApIgTdPNjyks7YmiGEJVs8AcHH7VlCzK/PV94XXtMbZkd0uOR12IlQtfdITwTnj3ovhlJ4B7Abf8or15LO1ileQu8xNRpqiYC0f2zdGodw4NRvmHrcplr9RVUHYgQ9okUvckudfBpeJ9+KD5Vr21Oa3cFa/Zchbz8350PdPUeurgZwzTwLPk4cnLBKuEgAAByB6bIAAAAAAAAAAAAAAAPVXxl6c7MnbJ8CuBuIMkZwqmKjmZ0myYLhh269AQQ5S/EQ2APhLJoeABhRC0GFtoTeyq3xxOzF9SjnA/kMFJEqypBVAx06ghJpR2m/wV9S9yPwhNCIj/vnpBFWsmBEv1RsYM8u+wWGycCfZn4p6ELiJDV7Ly7GMZLnEzOYKSB6HHiayw1ukfJmjdurUMXVA5pxwL1x3nPv8IFz3AJL2Kz5AbBhUJMTMhz2yijspI6yCKBiVS7GnpUsi4nPB6TDgunr0c3XJyEItZGziwhBUF1nfVUrjghwz/QkIYitbvAdJ/tyaGO9mETvLOELQXsVtJsmvAEEJrVNwBBsc5YuP7712jBHE5UkLvMQRkulEAnruYwddItjIwhzpV/eNmyW4Bqh8G3xqOs/oriBnuyz0FrNLRZhS1zZ4XKI5D0AKMP65lO5Q+R5qoU/WyXmzA9oSqovgkpaPT99398FjOAjQpR1NBkr1rpi4HULp4he38AwIX47RgSobPaYy6GZSdCKr0+V0w1jO27m5q0oFecZUwrvn/qtfRvhTMEGhjBUtBEJZnXdQig5pEghz5+PnU+pyIvHgpnXis4dKH9wXyecRqt04IXwAmZ0aL+UeoO3pV7oDin9Gmxohx96fmzzDesAPDA9llq54f5U/8O3EVT4Xz+LUflI9SYMv9lYR1njIxpsmK8kpE1z1fgm4Haz4ANXVrc678c/8wwJCvOughUxQKAUHc5wkn7TtoDOORYfIxsynMe7iNHbTv8XCKP94yIVjAJwkhfgrOH0tKtWz+7Iq2hyPzva56enMJpSewhbffzagUf7qstU5Jabx/DgYCikmlFLHtyFJbVnN/JDqFptIj5ZSM79PSncglVaCpBRxgm0orl/LUvxQCQ1dsGIJ2nLp2sd+fcGq9os1mReISnIJ9PKAVCvpXDFdy/mg7GB2w6/0wG+J4/PRI4w+4FBCZFhN6y5CBzrsP4mZuONxnHMa4BPHi+v5iRekW0BiQcYDFGS5ehGvAqZ90uYa2MMUpT8KMH+aISS/EXjck/KkpPith5lXyA063G9ZpGIF/mef2AfCkMnIrcDYImwhLZbs1qV1Uwx66XED98mv+gD0noglf5y10RNCWLLvGhwPaM18q5uElPGHbW46F04q00vT05W2r+gwcN8oJs7HMoCx3yE3aWfYgK9eIhmjfLXgZd18UwqDUIw7c2IUlE4zdlD99UZR1XrouXiI3zuWS+OpwFRx0XlwjBoq7YQ/aWRFFgvzVJdQqDMAP5nEyRtW+EcqqP0CbfP/xsDozQEDqGT+FfyOityJXVlzQx6bEY6g5AZ4qgQ5+rMWXvfEyDdC51tfk/igzKBEI4TrmMC4+SkKAduwfglwqu/7lF1GX+1orWWk9hAxi4a7G4Gc1g5xkXEJ4UBHrewyePz2CzKMVl/dwklYsEtkZZCJCe72ieIzfEH+Pqi7fe0UYH7psxvipETd0YRzqTI/tN+kyM3gv/xmzkJc/+ODLtMlUGfvjd0lrMrBNIfO6YKIjxxxNsKVtktdEB999je6GwCUDBG0wkfRstvTqzFZ1ABVr5Sm7djAQIF/WpD9iF/2jq7fFaRs7YFXV0CHS0E/3ROFBn0mTqlDkyUIFQQ9N3c/q1evZ6eqXoqnEsZbV1hf19YZtMCevV3vR2pRDf6dnA9XMpPP99VbakBLz4RNmkcbuliOcEBo0J9NueziqkBTfaGFcLgPYJ3x5AtkPxIv7fjI+JZB4fd8IZu1VbeDes6pZVQLVfQ/KzOrrZsCdCEl0n/3hR3Qmb7KD2iwP37zH8bvY66kn104f6qIRgVFCut98jFVA4KubU4TmOOBUryrmNtjn5MPPegybpfM91+L3WAuVseczYWo8AAAHUFAM14eLgE2RQvpzMp1P1kTKxQwtyNJ7zh2IDLu+c4stdmYRPuIVwB24BYUYFYVuBpWtGa+vkpD5HYM60HgQfJysUh+ozcN0iOrChnT+oJ5+9Cwx+8tel54f/eXxM8McaUR6va/yuUHaAnxzaQ+Tq/aHyvy3wOKqAGPXMwZlCRM6gflQVls+UtQoE0oyhf+crjFdhBkyYnAx3q0st8aMy6P6ABbePQYcIG8Wzx2c0Wyuu+5ZR3x7vH/R62RLdCXmgN9NDX1xkuCwNhwJ9+lvsx5X3C6iprP26sBlm+zzyJT6tyLHsNXKaO6t1UocvQ0gzhfpVAEqqucVL/S4oEHsplMCzaPdiIYY1lOriEvudjWd9wmJXUH6iVOV3hgHUZoVirlIIFJ9tOk3XLHDBGDX0qrBU8hfYIw8vV2qlsL+JLHaWZYfl4g96v0uiRcQXZmh8IGIBpc+dcoi1ip1FuIUPLIClMDCpCVS34Y/HAekGeK3W2wW/17Bs/5dUVpOPyzxTde+weSoM7DpD6U7nIZilGywiCetw1KAYLCW6zZFhSlzLZfCYA6py2iAyVaE/KIYVfDTuVGXwRIA5GzOhqAg8RL1rNBfAvAqtJEdJl6swsn7ICT3P0k2+HhjM5a5RTmwFAj3U66yZsLrsWtuaMSkDXLl4D7lt/de21Hy/yD22ohmGfbixCXQbrZI3Sb++gQ/zvvLnM47aOr64udR8P0AHygRimQ8KiCP2hnO7GDOvYRRs0d+Huijq/TRZx1aoLtTD1inmaaNCD52812VXCNSpMLe+H4h64v6JLBkI9qej+lpvlXqrORcs9Pz8bBvUo3fCEt7mIuyqvGqzjNL0rgl3em8I2CBVoF1VAn/OUuA0d5FkFLKCL0rMxPHFga+NO8qSzYc4bVpBedkPZI/R++OJN88NiUZaYQKFfPklXWW1+p+47whlx12pfX98FAdy/c0EOlRMzh9HaWF2novNcgwr/R4exUQ35I2rGXTPW5PAhFHmZ2ZPqq5FJF87ZTsMc3gyFV6V0H9tW8xsoOrElJql+tCp+PC3puPnwgNiUXGZBA9QKGdbIgdVZoypUHb0kqwrQPLJiohYAro1kxgPScGX+jnJslkjJfRPHQiC1o4LkP9zBYpr0wSZYtk4GKtHsKPzpvqpeXXZt7Wh/EzHonBBjEC3wMQJw1kR55O9StwxFgYRK1n8Wn95nETiHzjXxA5WKIZVcsIebyiZVGeNaeTxS5d+xMAABOxWNAwYs2adRFZcYSWyNEM3mx9ObhvpEf+uKhW2acGy1u6IJJxMtlBjfiKCqFQ/HtmcemfOQtpetYEzo34lKZC76YbLrkjyG0y7583BadNbDIOrIeemDNRAsIqfvNPqurPhGawKQz3+koX7ZpHIX9pL9FrJEWvzaI4mHF2NRMgEWDm/30G6JGrj+5Ri4oyj3ewpbq1ZMWRbSGo+ciLfYB+yRYFTj9HEsUGBJY6m/BcOVBX6WagUxIJ1ay/KP97FogruOYUuYQnrezSpSmbQB1t2svD4Gt/obsqqY3FFXEBA/Rj/3jVcnkjxBDgzibpMhyfzerOdC4kOXjGY9dbqTQE23nqfL+FYD0ANKdSErFMfIt4wbpY5HeVQQyk1kvmQgXZkFsAyvdVXhvyUqZboyYDoss1NyTba8LiqG+3MwVP8oggW9WTj8yM3PWZvJZo24cH3zIs8QZ/y3cHNpMAaEOcpgwOdgC/cbMHLj6aSekTpYzut/S+wUMcnOdl/H7skR2Yh+8bTEdOg4EadgBRi+GAAAAAFSyR/8kUm9dt8FQeMWv2LzrL+p+EMqV2P3X0CuY4pqfgFqVUroLC+giX6ad5ROxP5jNcOYpQh3MrnkRpuSWmr+4wJ12Ui38xHMxYJbfUwmct7PeFF/0DwdqEl+HbzFP3uvH+0FenyKQq+oiaPQd3sb7DMC/o4ggVkIfhtZiWZTbryHRkfuRs4Thcj0widj29QHzQV0Xmu+A4sW/vSpThdi1td5DC4p2jajvu0YxKabQfTOsN2ZekLcDF7+l7/+i4m8ZzAXpMYXHesso11BORJ6VJ31ERU9RsHxSydHEaWl+GRNt6Gbmy22lPPVfD4Z7jvp2w7u7ddF8cc5dcwI98c+qh1Fzrx/rYqCprb7//oIr/X5/s/1fvEwu1hO0ibHO0ooXbSTgvm3n5/HH16MdwUR1GP6QUc4wiP31LCBMNg07AElCkmBK5pFJ+qxbDrjCo8GoyHboIAX2MLG0i/nG3yfvjhXjiirguVf6Nib7rEP168ib/z74nJ++h6j18v/DSRF3RVSaQq6zf30ZEEJSUjUl90U+wrTHYVZl0nqxQZroGiz9/evTO9OILP2cMwmG1iK7hWWePC+Q6yynqi4RvAA5or3xu+yLr/8OmI1ogd+SZGFhf71UcwdFq8w93byKkho/goBVyPld//QQ9oSadd0B/YBw8cEFu8nTrdZP9DyUbVrFY8+Z3RK3UUdmiR8ABNNfzVQUV0oS4yuYcefuWakQe0AI/Ue3OQ+eQhp3K3xjKRXelwOHoVuF4m4zVWU30FIBycfFeJjr6s3Zbs3pjVRjoJD+gBJm2k9GeV4ks5UHmniz0xj1A6QEVeH9pfBesDNWqjdcGCnQeNMQLO5xc21x6wReyD2D4JSfcGR9NGGifcKzGkLNnpJp1FQD0J+6PUTJEfSSheDxbnp/g+UVh2Ztc2elzbwsKAf3M7p+oiMlEPsydDQd76VUWutdJJALoUAHOdBKd8xaSZt6HUG7xgmyo6xznZVPn/Y/+esh6im54G4T0auTLT7dE/g/H8idBKrDsTJa0lec5g5XzIZlquOft3fN0hExmzFyF9NctVUt/g4xQUFH6mF4HCfd8odv4M3LUyxrl7FX6dFBbukzpHvYLt87I+woGIhpLpGw1x4Ddtnm/2r2qfYTRN7rRm2RlzLU4zIIK0YSiDYipsRTLYrEVvGC9lClxf7qkxSPGG8VUjZ7We6jWZ0gA0/UFzKe59NRTkc3lB7kqYuGj84OWDuyNL4X6RGZagaPuILf6D8uX990Ltn9s7GArDqX1139iUFmqmTT5oREBAaW9tJdQAO6THA3i5xjqD7Cr4OL1bSLJWgcFNyNc5D5BPk36CiHuB/6L2PE/VBfe7tFUhQ5gOhxvsyqMaVCXTUnkkWBV1K41DScQxOw7c3i8ho7Xxcs6S43IH/eqkt785O+F2LIDdQO89iCEngkEbiI605p7szjohaLGraZ7iRI7M+6nQan+bUqg6pVDexcRd+/p/fqNvy5VAi1LI7smU/cZWnsiD8vzaNpqXS6mt6szg0yt4oV/oeNDdZwxzTav5fXW9GU8/jPYBfSWY17zqbf2vLWmVmEVaAuIQekEOeT2hJbzngHdRaxGeldbFPNDoKIPxrUptdhHdRao5ABW3gkXl9ce195KaiGR00Rj0aBigCCQSTCdXsIw9GMh2iqihqGkKqIU324w9bf0ssLficluPJMm5tkZASjOAu8FYF7B6P2E7LIT7ZrY+khZ7CkUFbkv7WgaEQm5sJfNL9/fyG0W7KKybiKujPhnf4H0jBZjrul5GExuxBevJQvEeu1+8yHwWHA8nHcRwJO/gzqg7r8Pn0ARzHYvT81f6csPuXWSIddoWgWrOyBqLMY24p9BO7TmSel0OdGi8dztPY6bQYrv662Y7+x/gsIVG3OVTeabxp6rcj0NbFSLy1VsyBGZ08uqjgMAlXG4JEsNGQIgRujl4wCegjRDdQDOlroAhhWFMXjWkEFGtMnOFhWW5whOrFDwJ0Grq86mAS0h98c9jqPdFmdO1/qmallGHo5pgyLK5hh+G54QzUXMmn9e+GdWfKjDKSH7fNsFQcI8a+SOSYRIGbDCC0DL7FNHa//0FW7oH7uKThr1Z/m0VvqX2PuRjbAR+VNo5Rz2BJnK968CUYiI7fwDVqLV8y05PhWgsIlkihDJ/1AZcEJq5ZbrqHN1fTBJWO7SR0JVEuXhRrQoU+82wYlvYV06InLwTwtAMuTZBFtO422T2ixOiInHzEN/0eCK6tcZyTAWgh1PGnqVsngXHZbuDtHipkTnHAr9E4SxW1T3lQRc64Q2FPKouKKW/ZCucdNPrcmvFhULNFy4mWG4ZqMnbjRitKS50KHzDTZfheQzuHKLDL0VHV7yNIZvrjK8DHxwBtPIsF19ot11AFnZ5a0Bh3edpUboe1nRT6dHtr58D4ZgOTDLwCxoYkzkWDSfOAFkmnNc7fzyHbE3dtRIQxom4TV0l338xgJKB+W0RgzWtXa5/6cV+BfzjxtWxRThzplSmG6MRnplKugkOYdloXqNcG3zRbCkVyl7MsRgZ8hYYxj6FstNIvQXTA94LqPrH6T6SSsOLZU1MOO/2szjZ1fDN2vgrt4Jps08RzUCKNqccoy6159nAVWt6Xnb0JTbEhKFj/3W39z2tsBXNJ79e0whhLqvhS12tuMnucGnU+0fKMZG2g9SgF4JwFLAImUAiJvzjFMNn2dJSB0kPlDXba58gMGmqZk/aQsATwCnuZYzSc+4YmIRasp3iulgIOm9Rzw6q7CrfcXqSVTuaYmQnziVq1fAtLoxLtnwzCG/aXu4lpC/5UykbSNHPk+eMeN+MF/1sKyEnUIVxXMFxlkVf0Tjl/Xwf53hb1HJe35BcTsf5oNNMSIY7s2qaFjYkdTBxm7A18oKJXYlPGyifD3BYnESk/JRM9O0TU2wUgMNKctLY6l0gbKQRsFkQpXB0OkYEU5V2+JWq65rFJSFayJqBKx7T2bgpE56wgsQXJS43ICJxeWtNzzk62uQnFZC9KvUsSqws+nKT4gb48MEyLe4drqEK9MlWKiXoeXJXENNaSAJr2ta+XHkQ7l7EKa2AlKrYYpS9J2M9fRol8oj7SfjrAKcasubtlp5xawDEo79ozFeBEFw27QR2ZOWZs6KTcBkr/bvUB0P7ggVnoZaqBccq113UxXXiIrudGaiwMabhlaqrGKb735hXF0D8I+Ue+/2VkdHUI9RrMBt3XZ3eujlrDMM+MeMmmA+pDlEAlfN7uzRgmCrJt12uO51LaMV9UR3tBX80xVuWqqnVBjvW/OwhScgPhs0GgPrRVc5vFr0aQLZmz2fatN6x6rWqIjoosjB2+KgTb10kPU0ce1cvZpyY+jg1t+oF5XnYmvPZ5P9Z1+XMWMVYXHiyeafMpN14Fvq1SeZ73j6SLtkge260gUfYysKpuq3cj4xsn82HEZkO4TvLtlb4voiG0NKy7KA/kPEp9Xk8AeqR7yevALQZ7lyT47hlfwoFN8nQOE8qDn33ESwPrvE3Y5jn0IBTppK5ymn0ochtDNOpaOha2WvyJj/+NJ+s7dRnvREEA2Lrq3DZ/HJEaTw1LIqdv88g1VaME/XkMGrCbEfqsitGOucGChHdkrgHP1RLBGjXlzBdHB6c1naOG3ygWMUpwfLwfslPBbL6YvvH54EZzxEWzziXD+xJpOBtFPixB2ljhf8b37g4Sv9/4L5EDj2ONed31UeRraBkewweOfyzMU/HlfdsuPITaSRdBiClQGCSAQQNkdlKYzacq/tktWWSXgh8V16WcD0pgFFD1i2J5/tW/dsnUHAROCCNCAWVDkCS4Xa0Zh3QpVXFtzfJZ7s7vm1/xjRDE7BrfVoeKjZ+/G+7vrLM643zgkHht4AGo6YagWHpVTUlyUwZUT0kq1PedhkuWiUD+spk6ZhkCtXjN7nUpCbsPtGw9l8fBthsdXG+4oVensp7VKbB0+c0zRihioCR8fG+yPYUcrvQw8fEC+clHbcZVyEW3OBqedeC6enCq/3tbm8l2FjdohOR/l3qHWQiKAM/DfePuzRg1XZX5WL2rrqJIff+fOdM996fVD8T8vZgHwZS3LvwT8uJLX55JnARD7d1xrFtNSV1nxxXM9uWR0kK4eRrgGywQJ8V+ST5b0gEanJAjQZ5pcWbOJPsmeXUF6MpLZL4h63KdBT88lPcW7KAzebBHh15YMiVQ8rJQW2dKhGH2GmH9GTbd9j4i2AEkEaBvLb84hbOP7rXRajUC5XGIaUDDuEjGu3SEgRYuW6wLnz0xEyOeeWRzJnX3rSot84M4j2leMDOScETxOilK0hKqMpQ3SgSmh5jBYx5mK7v6wY2Fm8pa1I05Sv3ZoR1luCttiQOOz3rf2mJdCGVpVSvcHhMkv9y1ZK2ubPmKLD47a+mzWuZS9Bc9EKj4S1436ydcssDmyHHfbZwv07rCn13RqgMFylPy1qO83hL/IBvU9JfF7Y+ie5ZD5XtLMo89fzOUHpq7Pls/+ZOeYrNK7Mdn8xYxOpnndM1QC4X9jCfM179H74isiej8msk9m002y/yLkA7q3rueToklCvm3HR8nkgVk3ovVPcOcPmtYzCPLX8bZSP0MEPSx1RUQuLkL2xpA2XqKsESxVNvxXYi3tSh1/4HlzAAY3sPNveQGn3eFIou4KH0d3SgZyjK7Fv/1MKhcs28mn89B24llCqyGACzZQa1nUBKWrrrYySKx0OBi1k2OLK3g87hhzqtHfB8H/rKO4N/Vl3TjZHLQM3LNBv56H8wokCqHLBCyDoVI/UoRfzfMO3ZqDcFDUZtzykoAdYZN+rZClz1uOHBU099crD8rciVuptKxzzzKUtMJCddpUoCN2Xqhxq1qTcuw4+DJ6e2j1j6zE5N2vANiuAsyTXgliBJIL6A42b/QcR/h9QNdx6JLmYEGuWauNTM/cGpN3mmaHtQ+JsW7L+fqI3hPqjSUeSQWIk3FrojSuDGuSHHDbi2xx0JWt4jxaLWP8Kt77AJcqdLbmWqzTc73PSb6uiQBKyEL1o+m0Cm/kzOKjTCrWQoNoXR2YFjAcbL7e9X6XdgwHEvnKrgmblSXVqsWSGWNf2LU+EqZ7CZ4J5NdKINa4SwH7OilMoLFtEHEWecLRg1d3MIyFBpKnov09E0ZWw1RZjct70ab24TWsvI1pjWNCeAdWW6dcCNazVti9Lqrn3EXoLqk9kx4bxSYpu5zU6j7eOTZiF5Ec6/zfqz9kwenNcboqU9oIfimmtVqIAM1apPmP9Us0/VrTw9ON3vanRU8/G3Y2tnWaAxeeIOr12WfoV+ae1vXgOr3oNGOJ0ofzu0VXQ7Ozya7GNPW3zvOQXYp34rC/rBny2DgIbxKuvPo24nTCuQUa0WHe9Hspy9KbdXw6YU6UqqOIJTPY03uYJU3BAIkH3sSk9JP4h8dwpDnipwNJJpl42+YOQeXqTY9jEycpfBenTnjM/cMRkvP2/yuZtTFnodujhszHCu6hAMrTnlYQ1BvCVn2FhzlvN8532L4Ld1EPF1TGThpoiThv1442RQTXEBrok5qeGQzgj//+DobASruftPVgzn3Bwwl2jgax/xdMSFVcAbigXTQXIuX5snJiRQ+w5bIUUDNK84llFeYt0yIOLXjHsdg3ZoHwiD58lTxGc7a5fJT+FaqZ4os+MOp9aA12w6AoPPuLbTRjUpODLYuk7tvEhLtOJ58FmUtaiPR+H+fdAiSuqRNbPSjDXBcjWXT+DK5BlKuXXkuiMH3xreTe6J7LhkKcjrBG9vJoBdYzXClHar4doyBrvXvUXOR6NsNWc9mUsNlH5yD+mhVevVSdHy3K7a8npXiPoa6DKF0tEO8pEBmZu5AjWY55ayhl0L6Tv+4XHon38yEDS8dXvzeNJCkaHi3F4XuzSUi5/jiVOxFEPseaCH4lXdY3hG38d2TgAlp6H8lrHEjx8CJq9eOSFUVRXY1w5MVSap0NBmtQJ4ld84tT3vw2zJF9bNhdc2bW7HqJ74R2TQkIl4rD/BiSLmZfyJkKhybDGqRZnP9V3PyljGK2E+BTKyfo106kZwfYfgP9xg2AXIGlUZHlYnIn8ZcVEDo+67DBJlHP0PM4C5TMI3AP56JLYQtZaf22RRczNqePiq0jLq5hUUwtmpHE8ehdyvFTpxtQj5Epnmjbltw7NIZBUqJyb7aoMk9wndP4u7ABrX11K9sz0qz1/YlEVS7wJIctEbOmSH4flJgUM9uEnZpbewF3WIRVD9tVaV+5gnuAyUmYOR2246181NB3d1Y/Jaa6Y0zdcaniLAznkw93EagLn4YWY8pX3jYOUIBnArQJIMHciplgbyz5DYHZavINPBzpxzoC2ZptPgQswsWeKW2yY948b/yorlUidxeO5InESFuQy/YkdsjCAEPQlAKrdODzpnrAEgSjuYTkD4tmYgN8FAKePHCsskQA2ev5b8NM23OiXCTIeTTR/Fl2T38n+jCOmtPX7DcofNpNj9rbiMfSyGnj7GH5n6E76cRTrpsqeO2/EpfMo2frZsOsFVvJmXODijMGWkts7yHkFHeu7A3RxLT3BbzDjcUdIZSIbMnZLXhBj0Wn7T3hYplo9TNW9NcW8uT5N5lhqHZnDIoSdOlgt+GkNbFfRBK4m+IqXqjw2L4ujbrgCzI0ENc6BZogwd6mqQC/behU4gvJpv+CP3tTsIg1MuW3HRuLbbPwrELpE1jLSCByEh/olnJ9hLUlDsIZBaRHZ0iqrpg2DAGKCBBEzYb2Z285DuWmHNmrjdGgfJ4M8GfL70Rp1dR1v4/ayBsQ1CiYGcAMMTTMSjBrOyz9JM+iiDWhv3W+tobiYrndb8ZCacASPlRbWgaCSK7AFuQWRGa5UG33a3le0x1fdb4KvOxZbGEw3x+4ok+Tj+CHwTfjkXnYw9prlgD7GU0PQjv3oN8GgDek+nn4LFEjpW0wZaBfx2gFSGjs64EBXS2qDSRcTLbdiSU/xZkzkdrG3mGbD57Bk5/CY9D/BNcac+3j5SLD/xXld59pwxrIKC4bJhXl2lIlpCtJ0A9GftPwDHxIh6Q5l/zeqwgTHz1t08utS11/KtPH3bIt5J8FT/L7eJTJJqePymaL432nCiWvYkgrl03l7HRAV7NbAEQtdSHYW8tS5o4icFrwd7OyHvHxPJREgukBQXQC7fWVNuP925VDFHZKqtJnNqHMytCX5pwaNLQcnX4pfazAvx0S1XlZAhlNbDMZ+v4zD+Kae68InL8ZHFeTZ4CbeXzqoSZOQz6lt3xy/aLjKMwsexGerejyWcIHswEBtslhlU9j1Yo3fAklJs6XUdUFYyEb2wBR9ej+p4ZT94W59LNAsFI5rfMt0/1drksrE/LP6mBK8qQHv3v880dEPHLAm5A2RaZMY57fyGyGQA+sGaPnnJw3VLHoqk7DzyTk7WLj7O/FbbdVn3EwE/a2jp6jGuVMJmWkfxzkmATmkR4RYxeoBUbAUJYAVrYrKyh/z5m/jgngJ2kj+FDVlJujcWexY12VRD9482dUT6uq6vz7EBgVVAssiCtBku+KbmwI2wxkeQ52XsM748n/mb6/loHNcD2S7lFh+VSZ5UziRbRphLao3tcUEq+gD3u+Kl7ko+Hdr7aNz5hBioFuTE7ORSBO+6onoNb5kLQPCRwr5lwVg+qEEwFTjkG7tMQd4bulhBnviM3+jFhWsGUdVXER61bgDa+FInbE9IAlCPgeC+DKymgwc1RBvRbe/VU7LiFVk7BCkko1dttfz6CeaKbGy8Hv8NOQd/pinLeTxR8y40wa+1doc9TuCYdGCcnyR0YnwTa4E1PIATQK6+fROpmLKKwTnas8ZXrvcyAxwdsOKrGNhp/pkbARp6efWT+F8bPetzGshTH1wTlRjyPZ5HcgkKGswDVYxSo2ahX8BaSc2OXf7Zzf2a/FlRQ2lB7z9Q+W3QKhhrjgfo53UxoXxvbPWSTfsYnl3kh/a50wpryfd55AfGmrl9W49AWA8+cibUefyq/PC1j3R0ewiwptRJ9m5Q3HoN2hctQ39mhXJ/p5LXJEtegzmQpPoZ+ip3IhJffQ3jJ/XC4rk7L/te8LfvWbP6q8p5/SpDRMlYhuUQjBYCgf75cosZsszYB1dgK7Wf8RBE0jaQDKyUO8Y67DzfVuKGUGoiH48qf7J9Uf3M7tbQ7a9apwEsPyq6NPpR3wr/5sMp5QxY2kh1HFq+y0A1zNkkR81byx+H8HlD30GI7rj1FhN81mptyB+ZsoIxrs2BvEg8CZGyU5ifk2O4DuSdfkUeBm0XlYeuz2+VwCdWu3V5XhTgwMJdF7ef+H44Lbpi7MSI8uLW7MmaR9e9MMx/N+P0wx62b/+yGYdmA/XjEh3wbvuxNMOQCVp1ayQTV8jZTgccYrQ6goxjdPA0D7KVJEIbGCcPiWCqhOJPRqxOWwy8gQ1/Ijxk+t7+7n9hsfRE/FuWswIu45KsKv4pqwSvLQIucOPIn/R6fk7Oxukp5I0KIk1LLwGI5OC8Wg8VA3yOOJBKMHo4MuVtPlLzXgWCw6sqTbUsiETqnW1va0abJsPiNqc+27/JaFB6Iokmtt0twr2CxyYLRhHC4Lgxfu8Wb/0weHpoqfimI7RDEOgNzmf3QLZdzcHYUaRKJRYEqkE913KQ+fr6AhZnzOhvH4utSL6gpR8vedFnw7Gb9PurARjqkcTBXXEQC8wZyWk9EjLEIte6XhJj/+L+GZLR7G06M82Njyw2qjOY1Od2/CFuq32fVgII5H/mnU6K1gku8XprHItefGkMmsLqIWI8/oA1jdNkpypaHCEre9sSJ6KbbqODsjHFeEfakWufyDURbGWLWKeECvqQzZojxlN0Q78yH8WEoxPevvZVXqqIXblvBU7JjZmr1cd/3HAD20JSXWEjMX3cyWK9bfG/WzCyqbWD6B2lS2fvXFVy0E0XsEyxulDK3OAF2q75lcozpcCT6sW4WXc8uHU+++jhe3RXp0ZfQgeoUpfzrl+UsYLb72na0HILY50Qs7Kx7//3ioyTS31aLv3j5YFbPIWPFRnVgFmCwSo+L8Xrac0ATEEgTSzia5IxfJJhpYxP7GY4q1XrRQho2Zfs8hJLaQizAbLK5SrNIsvjp057dZ6/66yXrevVFtWv7MjQkZvoxDnPgTpwAAAPDXRu7BWuDoQg0e11PhB0IUcHriEsK/ZUtTqujmhrDBeTPVXe6wlaN7kiWG5xXI9qE5CEsAAWAS9Oq8NgLWd6zo+Ey4/jX+1RSSayYqRJo0HTR1zCiehsFpkxwyrDHKaE+ADCSkwUEa1tweEieNvNZkX/Q1R7MgAqB0FO7Ecux7MXucVrKmpxsdGyq1ODVV6JneGZThadJI2xh8Fc5KMhsad3aURymCLxIkbqGrfmxjGwAuPFZG27J9cT9Uw/QXvMDw3gFASf7HQ33+tG0Wg3MW0XtkQo649IiewJWaCNLnNiVqetJBC5XxPJtT6ZReYHsFRodKpMvUxVE32jcAvZzucC1IEVboVZ3OvZp9ll2t4CaMRUvPy11mT9oZAaRXtZ5QCIUokMrXsdlKmr8M5kJ3VlG3yG+qbpxUFkbumwk1lFmixl+EkfNu7hgDXnLDE7E2lJvStCv5FK5CSJuqjCbRTwNbkt2/CYpjtiBXbS43CGYh7C0ntfddqNzGc9Ts0oFOOECC7LdnVKCMxTtOvxImd47vVLFUSy8C+d5Cjq/FDvnBzfiLIYtwZtjBRLTunExMhr96qlzrSqnO7ALOIEUisohEoWVVxuNJS8Wpib5n0MIX/oaArhsdBxIl1YxzHHOY6iJ3r9QVwHWKjLAPD1oVI2JFRSWM7Fkw/yxyT7TUJ8EI+xqmjaeGIlrsIzp/EQNPJNbal1MIaH2opCYL5Mfie6SFGLCh6hGxMe+kcZaawUP/hShlk74bUY6hUt/hr9Tln1WB8mTyel21cRr8QEP6p5ObIaLJI+PGWCXfuc70L682Jco9O/XJUyiDfGt8grtCiBuRTCN/HVoPZeAqW0IFdDcFlZ8A4o2o6v2qz6PRGCJWxRbl+LZENpm4RP8yQ44Q38yaJDVU5jkS8zlCwdPy8rMD3uVLJwgDIHQotPYwFHnnKrcHxwjZK+MszQ1FrqipNOlPFadHWUzv/Q1F+lDLsc0/sMAOcWfy+RXVkH0Cw4IIHAYK9F87UL8LL1VoD4PcBBZuxtIMqZ5ycKNJDQeqhG0wl9BL1TNFDed97tFn8QuxGISKzJPihk0WXKg6Vbs4BeLDa6Zyhm39kQBMWuNyXzNcr0LDUgYB72BVmmWIJP+WsHfeVOMf90qSPUcsnaCerINW6kOEkxmjlR+QmWGbTt4AiM8YGawl4z9ymhSBIajgCSngjyfawSnRAujBFTU2FGOB8k7crKAWUCHo1uB2uj5EtajFn5C7q+Rpr/9LbOhN1ph1wLRkbkb/JImpTie/KzJ5/eEyzaKRx78PyqwZK80E/NKdGGx4V0oiPMESuh7GzbLsP0EnHrSsIL1svK27WqNakxEAXn+bdudMrEtn0+lPB/B9V2YrsQTPQWdn7OtfrbMwp5jKfyzudgCzo03X8zlGo1uCHoyMYapVFlB45+OOqr/i8aHKbEnOV3USrhXpiJQiPsCzpJuR/zZM8pIAGDlS4NdZWECKKAAAVLAkFca5pGIZUr38zvsdOsmvaXPBDZtdHDJFIi4RSlk4gTRvCcsGHFbf5CD4LA6KhgkXRfSKDZSb50/zNnksxSJufzhr2jAJJ1H1XMcSzyyjI33TbVsTuq9K2YZ6PZjNnxR8a2fdaXoQPU4z/pxS+B9zSg5FhkfETxSFl32gZmed96Imx2yK/doOpym9qA75jwmnw+gGzPJFVHj0LRuYoyuoF5dfC9KndibvY7wsIsUx+RshraLH87I7+pJgYjsAlhTv6bI/r6oXAADnCgBY4eTJhvWMPLoUAWw78whEYcIy+eSUK6+ea14nbbywSOQ+iWpbNVhOn6t82FoKz2c+HXVW3t/Spy2P8/uhrY6LYa3kbnUhBgtZvDtpAaCCmtpLte3REXZO/nNNbCvYesoKMGjgi1C3DY5FSKENEbybqPXMeNrm+/kh6mzZbfOTRb+O0zk2E+DE3ObrIqzaugAAAAAAYTYVgFDpWmUmynd6mIbpPx8G21ZQEppAW+vXLC1IA2IeEFUTbGY81xaPqFkAyfUI49K/RN3PrMF1iWqDp9ba6ESMskvhmPC+5wiYz7aaFd9sD/vfrlw6Pf0DBw18DshUnGAAAAAAAAAKdsnpPU0yyKLMOMZbUEnBkEZGu/uFkAgrIAAAAAADqCgAAAAAAAAAAAAAAAAAAAVoUAAAAGWFFogoaIFCzDG7IBphQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="

# ==========================================
# ============================================================
# 🖥️ DASHBOARD SUPERIOR — SCREENER INSTITUTIONAL
# ============================================================
# IMPORTANTE:
# - Solo reemplaza la caja superior del scanner.
# - El motor real, MFE, Telegram, resultados inferiores y panel broker
#   permanecen en su lugar original.
# - La ventana visual sigue el diseño institucional de referencia.
# ============================================================

# Paleta de layouts original del scanner. Se mantiene aquí porque el
# bloque superior institucional la utiliza y el panel broker también.
COLORES_LAYOUT = [
    ("Rojo", "#e53935", "#ffffff"),
    ("Naranja", "#fb8c00", "#000000"),
    ("Amarillo", "#fdd835", "#000000"),
    ("Verde", "#43a047", "#ffffff"),
    ("Turquesa", "#00acc1", "#ffffff"),
    ("Azul", "#1e88e5", "#ffffff"),
    ("Morado", "#8e24aa", "#ffffff"),
    ("Rosa", "#ec407a", "#ffffff"),
    ("Marrón", "#8d6e63", "#ffffff"),
    ("Gris", "#9e9e9e", "#000000"),
]
COLORES_LAYOUT_DEFECTO = COLORES_LAYOUT.copy()

# Estado visual/operativo de esta interfaz. Se conecta directamente
# con el mismo objeto `servicio` que utiliza el motor real.
_ui_q = st.query_params

_ui_defaults = {
    "active": bool(getattr(servicio, "encendido", True)),
    "start": f"{getattr(servicio, 'hora_inicio_auto_min', 480)//60:02d}:{getattr(servicio, 'hora_inicio_auto_min', 480)%60:02d}",
    "end": f"{getattr(servicio, 'hora_fin_auto_min', 1020)//60:02d}:{getattr(servicio, 'hora_fin_auto_min', 1020)%60:02d}",
    "lang": "ESP",
    "window": "Incrustada",
    "broker": st.session_state.get("bk_nombre", "Interactive Brokers"),
    "api": st.session_state.get("bk_api_key", ""),
    "secret": st.session_state.get("bk_api_secret", ""),
    "bridge": st.session_state.get("bk_puente", "http://localhost:8080/layout"),
    # Filtro original editable desde esta interfaz.
    "pmin": 0.5,
    "pmax": 20.0,
    "gmin": 3.0,
    "gmax": 50.0,
    "float": 20_000_000,
    "vol": 20_000,
    "refresh": 5,
    "ema": "Hacia arriba",
    "macd": "Positivo",
    "order": "Actualizado",
    "top": 50,
    "auto": True,
}

# Valores persistentes por sesión. No se toca el motor para construir
# una segunda configuración: se modifica el mismo `filtros_dueno`.
for _k, _v in _ui_defaults.items():
    st.session_state.setdefault(f"ts_ui_{_k}", _v)

# ------------------------------------------------------------
# ACCIONES DEL HTML -> estado real de Streamlit
# ------------------------------------------------------------
if "ts_ui_action" in _ui_q:
    _action = _ui_q.get("ts_ui_action", "")

    if _action == "update":
        def _ui_float(name, default):
            try:
                return float(_ui_q.get(name, default))
            except (TypeError, ValueError):
                return float(default)

        def _ui_int(name, default):
            try:
                return int(float(_ui_q.get(name, default)))
            except (TypeError, ValueError):
                return int(default)

        _active = _ui_q.get("ui_active", "1") == "1"
        _start = _ui_q.get("ui_start", st.session_state["ts_ui_start"])
        _end = _ui_q.get("ui_end", st.session_state["ts_ui_end"])
        _broker = _ui_q.get("ui_broker", st.session_state["ts_ui_broker"])
        _api = _ui_q.get("ui_api", st.session_state["ts_ui_api"])
        _secret = _ui_q.get("ui_secret", st.session_state["ts_ui_secret"])
        _bridge = _ui_q.get("ui_bridge", st.session_state["ts_ui_bridge"])
        _lang = _ui_q.get("ui_lang", st.session_state["ts_ui_lang"])
        _window = _ui_q.get("ui_window", st.session_state["ts_ui_window"])

        _pmin = _ui_float("ui_pmin", 0.5)
        _pmax = _ui_float("ui_pmax", 20.0)
        _gmin = _ui_float("ui_gmin", 3.0)
        _gmax = _ui_float("ui_gmax", 50.0)
        _float_max = _ui_int("ui_float", 20_000_000)
        _vol = _ui_int("ui_vol", 20_000)
        _refresh = max(1, _ui_int("ui_refresh", 5))
        _ema = _ui_q.get("ui_ema", "Hacia arriba")
        _macd = _ui_q.get("ui_macd", "Positivo")
        _order = _ui_q.get("ui_order", "Actualizado")
        _top = max(1, min(100, _ui_int("ui_top", 50)))
        _auto = _ui_q.get("ui_auto", "1") == "1"

        st.session_state.update({
            "ts_ui_active": _active,
            "ts_ui_start": _start,
            "ts_ui_end": _end,
            "ts_ui_broker": _broker,
            "ts_ui_api": _api,
            "ts_ui_secret": _secret,
            "ts_ui_bridge": _bridge,
            "ts_ui_lang": _lang,
            "ts_ui_window": _window,
            "ts_ui_pmin": _pmin,
            "ts_ui_pmax": _pmax,
            "ts_ui_gmin": _gmin,
            "ts_ui_gmax": _gmax,
            "ts_ui_float": _float_max,
            "ts_ui_vol": _vol,
            "ts_ui_refresh": _refresh,
            "ts_ui_ema": _ema,
            "ts_ui_macd": _macd,
            "ts_ui_order": _order,
            "ts_ui_top": _top,
            "ts_ui_auto": _auto,
        })

        # Conexión con el scanner real.
        servicio.encendido = _active
        try:
            _hs = datetime.strptime(_start, "%H:%M").time()
            _he = datetime.strptime(_end, "%H:%M").time()
            servicio.configurar_horario(_hs, _he)
        except Exception:
            pass

        servicio.filtros_dueno.update({
            "precio_min": _pmin,
            "precio_max": _pmax,
            "gap_min": _gmin,
            "gap_max": _gmax,
            "flotacion_max": _float_max,
            "volumen_min": _vol,
            "intervalo_refresco": _refresh,
            "cruce_ema": _ema,
            "macd": _macd,
            "orden": _order,
            "top_n": _top,
        })

        st.session_state["bk_nombre"] = _broker
        st.session_state["bk_api_key"] = _api
        st.session_state["bk_api_secret"] = _secret
        st.session_state["bk_puente"] = _bridge

    elif _action == "reset":
        for _k, _v in _ui_defaults.items():
            st.session_state[f"ts_ui_{_k}"] = _v
        servicio.encendido = True
        servicio.filtros_dueno.update({
            "precio_min": 0.5,
            "precio_max": 20.0,
            "gap_min": 3.0,
            "gap_max": 50.0,
            "flotacion_max": 20_000_000,
            "volumen_min": 20_000,
            "intervalo_refresco": 5,
            "cruce_ema": "Hacia arriba",
            "macd": "Positivo",
            "orden": "Actualizado",
            "top_n": 50,
        })

    # Quitar solamente los parámetros de acción antes de volver a dibujar.
    for _k in list(st.query_params.keys()):
        if _k.startswith("ts_ui_") or _k.startswith("ui_"):
            del st.query_params[_k]
    st.rerun()
    st.stop()

# Aplicar siempre el estado actual al filtro real antes de renderizar.
servicio.encendido = bool(st.session_state["ts_ui_active"])
try:
    _hs = datetime.strptime(st.session_state["ts_ui_start"], "%H:%M").time()
    _he = datetime.strptime(st.session_state["ts_ui_end"], "%H:%M").time()
    if (
        servicio.hora_inicio_auto_min != _hs.hour * 60 + _hs.minute
        or servicio.hora_fin_auto_min != _he.hour * 60 + _he.minute
    ):
        servicio.configurar_horario(_hs, _he)
except Exception:
    pass

servicio.filtros_dueno.update({
    "precio_min": float(st.session_state["ts_ui_pmin"]),
    "precio_max": float(st.session_state["ts_ui_pmax"]),
    "gap_min": float(st.session_state["ts_ui_gmin"]),
    "gap_max": float(st.session_state["ts_ui_gmax"]),
    "flotacion_max": int(st.session_state["ts_ui_float"]),
    "volumen_min": int(st.session_state["ts_ui_vol"]),
    "intervalo_refresco": int(st.session_state["ts_ui_refresh"]),
    "cruce_ema": st.session_state["ts_ui_ema"],
    "macd": st.session_state["ts_ui_macd"],
    "orden": st.session_state["ts_ui_order"],
    "top_n": int(st.session_state["ts_ui_top"]),
})

# Variables que consume el panel_resultados() original que viene después.
PRECIO_MIN = float(st.session_state["ts_ui_pmin"])
PRECIO_MAX = float(st.session_state["ts_ui_pmax"])
GAP_MIN = float(st.session_state["ts_ui_gmin"])
GAP_MAX = float(st.session_state["ts_ui_gmax"])
FLOT_MAX = int(st.session_state["ts_ui_float"])
VOLUMEN_MIN = int(st.session_state["ts_ui_vol"])
REFRESCO = int(st.session_state["ts_ui_refresh"])
CRUCE_EMA = st.session_state["ts_ui_ema"]
MACD_MODO = st.session_state["ts_ui_macd"]
ORDEN = st.session_state["ts_ui_order"]
TOP_N = int(st.session_state["ts_ui_top"])
AUTO_ON = bool(st.session_state["ts_ui_auto"])

params = {
    "precio_min": PRECIO_MIN,
    "precio_max": PRECIO_MAX,
    "gap_min": GAP_MIN,
    "gap_max": GAP_MAX,
    "flotacion_max": FLOT_MAX,
    "volumen_min": VOLUMEN_MIN,
    "cruce_ema": CRUCE_EMA,
    "macd": MACD_MODO,
    "orden": ORDEN,
    "top_n": TOP_N,
}

# ------------------------------------------------------------
# TABLA INSTITUTIONAL: usa resultados REALES del motor, no demo.
# Esta es la tabla de la ventana superior de referencia.
# ------------------------------------------------------------
try:
    _inst_filas = filtrar_resultados(list(servicio.resultados), params)[:TOP_N]
except Exception:
    _inst_filas = []

_inst_rows = []
for _c in _inst_filas:
    _ema_txt = (
        "1ra Vela 1min por encima" if _c.get("cruzando_ema20")
        else ("1ra Vela 1min por debajo" if _c.get("cruzando_ema20_abajo") else "Sin patrón")
    )
    _macd_txt = (
        "Positivo" if _c.get("macd_positivo")
        else ("Negativo" if _c.get("macd_negativo") else "Neutro")
    )
    _inst_rows.append({
        "ticker": _c.get("ticker", ""),
        "sector": _c.get("sector", "—"),
        "precio": float(_c.get("precio") or 0),
        "cambio": float(_c.get("cambio_pct") or 0),
        "gap": float(_c.get("cambio_pct") or 0),
        "float": (float(_c["float_shares"]) / 1_000_000) if _c.get("float_shares") is not None else None,
        "ema": _ema_txt,
        "macd": _macd_txt,
        "volumen": int(_c.get("volumen_dia") or 0),
    })
_inst_json = json.dumps(_inst_rows, ensure_ascii=False, default=str)

# ------------------------------------------------------------
# HTML — MISMA ESTRUCTURA VISUAL DE LA VENTANA INSTITUTIONAL
# ------------------------------------------------------------
_html_institutional = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<style>
html,body {{ margin:0; padding:0; background:#dcdcdc; font-family:Verdana,Arial,sans-serif; color:#000; font-size:11px; }}
body {{ padding:4px; box-sizing:border-box; }}
.window {{ width:100%; box-sizing:border-box; border:1px solid #999; background:#dcdcdc; }}
.header {{ height:150px; box-sizing:border-box; padding:4px 14px; border:2px solid #b39212; background:#000; display:flex; align-items:center; justify-content:center; overflow:hidden; }}
.header img {{ display:block; width:min(100%,860px); height:auto; max-height:142px; object-fit:contain; object-position:center; }}
.grid {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:3px; background:#fff; padding:5px; border:1px solid #999; border-top:0; }}
.item {{ display:flex; align-items:center; justify-content:space-between; min-width:0; height:24px; padding:2px 4px; box-sizing:border-box; background:#f1f1f1; border:1px solid #aaa; }}
.item label {{ font-weight:bold; font-size:9px; white-space:nowrap; margin-right:3px; }}
input,select,button {{ min-width:0; width:100%; height:18px; box-sizing:border-box; border:1px solid #777; border-radius:0; background:#fff; font-family:Verdana,Arial,sans-serif; font-size:9px; padding:0 2px; color:#000; }}
.item > input,.item > select {{ width:58%; }}
.timebox {{ width:62%; display:flex; gap:2px; }}
.timebox input {{ width:50%; }}
.buttons {{ gap:5px; }}
.buttons button {{ width:48%; font-weight:bold; cursor:pointer; }}
.save {{ background:#fff2cc !important; }} .reset {{ background:#fce4d6 !important; color:red; }}
.tablewrap {{ width:100%; overflow:auto; background:#fff; }}
table {{ width:100%; min-width:940px; border-collapse:collapse; border:1px solid #888; }}
th {{ background:#ccc; color:#000; font-weight:bold; padding:4px 5px; border:1px solid #888; font-size:9px; white-space:nowrap; }}
td {{ padding:4px 5px; border:1px solid #888; font-size:10px; white-space:nowrap; height:20px; }}
.up {{ background:#e2f0d9; }} .down {{ background:#fce4d6; }}
.ticker {{ color:#0000cc; font-weight:bold; }}
.pos {{ color:green; font-weight:bold; }} .neg {{ color:red; font-weight:bold; }}
.macd-pos {{ background:#a9d08e; color:#155724; font-weight:bold; text-align:center; }}
.macd-neg {{ background:#f4b084; color:#721c24; font-weight:bold; text-align:center; }}
.macd-neu {{ background:#e2e3e5; text-align:center; }}
.layout {{ width:65px; height:17px; font-size:8px; }}
.num {{ text-align:right; }}
@media(max-width:900px) {{
  .header {{ height:105px; padding:3px 8px; }}
  .header img {{ max-height:98px; width:100%; }}
  .grid {{ grid-template-columns:repeat(5,minmax(150px,1fr)); overflow-x:auto; }}
}}
</style>
<script>
function parentParams() {{ return new URLSearchParams(window.parent.location.search); }}
function pushUpdate() {{
  const p=parentParams(); p.set('ts_ui_action','update');
  p.set('ui_active',document.getElementById('cfg_active').value);
  p.set('ui_start',document.getElementById('cfg_start').value);
  p.set('ui_end',document.getElementById('cfg_end').value);
  p.set('ui_lang',document.getElementById('cfg_lang').value);
  p.set('ui_window',document.getElementById('cfg_window').value);
  p.set('ui_broker',document.getElementById('cfg_broker').value);
  p.set('ui_api',document.getElementById('cfg_api').value);
  p.set('ui_secret',document.getElementById('cfg_secret').value);
  p.set('ui_bridge',document.getElementById('cfg_bridge').value);
  p.set('ui_vol',document.getElementById('cfg_vol').value);
  p.set('ui_pmin',document.getElementById('cfg_pmin').value);
  p.set('ui_pmax',document.getElementById('cfg_pmax').value);
  p.set('ui_gmin',document.getElementById('cfg_gmin').value);
  p.set('ui_gmax',document.getElementById('cfg_gmax').value);
  p.set('ui_float',document.getElementById('cfg_float').value);
  p.set('ui_refresh',document.getElementById('cfg_refresh').value);
  p.set('ui_ema',document.getElementById('cfg_ema').value);
  p.set('ui_macd',document.getElementById('cfg_macd').value);
  p.set('ui_order',document.getElementById('cfg_order').value);
  p.set('ui_top',document.getElementById('cfg_top').value);
  p.set('ui_auto',document.getElementById('cfg_auto').value);
  window.parent.location.search='?'+p.toString();
}}
function resetAll() {{
  const p=parentParams(); p.set('ts_ui_action','reset'); window.parent.location.search='?'+p.toString();
}}
function layoutSignal(ticker,sel) {{
  const color=sel.value; if(!color) return;
  const bridge=document.getElementById('cfg_bridge').value || 'http://localhost:8080/layout';
  fetch(bridge,{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{ticker:ticker,layout_color:color,broker:document.getElementById('cfg_broker').value,api_key:document.getElementById('cfg_api').value}}),mode:'cors'}}).catch(()=>{{}});
  if(document.getElementById('cfg_window').value==='Flotante') window.open(bridge+'?ticker='+encodeURIComponent(ticker)+'&layout='+encodeURIComponent(color),'_blank','width=400,height=300');
}}
</script>
</head>
<body>
<div class="window">
  <div class="header"><img src="data:image/webp;base64,{IMG_LOGO_B64}" alt="TradeScanner Institutional"></div>
  <div class="grid">
    <div class="item"><label>MOTOR:</label><select id="cfg_active" onchange="pushUpdate()"><option value="1" {'selected' if st.session_state['ts_ui_active'] else ''}>🟢 ON</option><option value="0" {'selected' if not st.session_state['ts_ui_active'] else ''}>🔴 OFF</option></select></div>
    <div class="item"><label>LAPSO:</label><div class="timebox"><input type="time" id="cfg_start" value="{st.session_state['ts_ui_start']}" onchange="pushUpdate()"><input type="time" id="cfg_end" value="{st.session_state['ts_ui_end']}" onchange="pushUpdate()"></div></div>
    <div class="item"><label>IDIOMA:</label><select id="cfg_lang" onchange="pushUpdate()"><option value="ESP" {'selected' if st.session_state['ts_ui_lang']=='ESP' else ''}>ESP</option><option value="ENG" {'selected' if st.session_state['ts_ui_lang']=='ENG' else ''}>ENG</option></select></div>
    <div class="item"><label>VENTANA:</label><select id="cfg_window" onchange="pushUpdate()"><option value="Incrustada" {'selected' if st.session_state['ts_ui_window']=='Incrustada' else ''}>Incrustada</option><option value="Flotante" {'selected' if st.session_state['ts_ui_window']=='Flotante' else ''}>Flotante</option></select></div>
    <div class="item"><label>BROKER:</label><select id="cfg_broker" onchange="pushUpdate()"><option value="Interactive Brokers" {'selected' if st.session_state['ts_ui_broker']=='Interactive Brokers' else ''}>Interactive Brokers</option><option value="Tradestation" {'selected' if st.session_state['ts_ui_broker']=='Tradestation' else ''}>Tradestation</option><option value="Otro" {'selected' if st.session_state['ts_ui_broker']=='Otro' else ''}>Otro</option></select></div>

    <div class="item"><label>API KEY:</label><input id="cfg_api" type="text" value="{html_escape(str(st.session_state['ts_ui_api']))}" onchange="pushUpdate()"></div>
    <div class="item"><label>SECRET:</label><input id="cfg_secret" type="password" value="{html_escape(str(st.session_state['ts_ui_secret']))}" onchange="pushUpdate()"></div>
    <div class="item"><label>PUENTE:</label><input id="cfg_bridge" type="text" value="{html_escape(str(st.session_state['ts_ui_bridge']))}" onchange="pushUpdate()"></div>
    <div class="item"><label>VOLUMEN &gt;</label><input id="cfg_vol" type="number" value="{st.session_state['ts_ui_vol']}" onchange="pushUpdate()"></div>
    <div class="item"><label>PRECIO ($):</label><div style="width:58%;display:flex;gap:2px;"><input id="cfg_pmin" type="number" step="0.01" value="{st.session_state['ts_ui_pmin']}" onchange="pushUpdate()"><input id="cfg_pmax" type="number" step="0.01" value="{st.session_state['ts_ui_pmax']}" onchange="pushUpdate()"></div></div>

    <div class="item"><label>GAP %:</label><div style="width:58%;display:flex;gap:2px;"><input id="cfg_gmin" type="number" step="0.1" value="{st.session_state['ts_ui_gmin']}" onchange="pushUpdate()"><input id="cfg_gmax" type="number" step="0.1" value="{st.session_state['ts_ui_gmax']}" onchange="pushUpdate()"></div></div>
    <div class="item"><label>FLOAT:</label><input id="cfg_float" type="number" value="{st.session_state['ts_ui_float']}" onchange="pushUpdate()"></div>
    <div class="item"><label>EMA20 1M:</label><select id="cfg_ema" onchange="pushUpdate()"><option value="Hacia arriba" {'selected' if st.session_state['ts_ui_ema']=='Hacia arriba' else ''}>Hacia arriba</option><option value="Hacia abajo" {'selected' if st.session_state['ts_ui_ema']=='Hacia abajo' else ''}>Hacia abajo</option><option value="Neutro" {'selected' if st.session_state['ts_ui_ema']=='Neutro' else ''}>Neutro</option></select></div>
    <div class="item"><label>MACD:</label><select id="cfg_macd" onchange="pushUpdate()"><option value="Positivo" {'selected' if st.session_state['ts_ui_macd']=='Positivo' else ''}>Positivo</option><option value="Negativo" {'selected' if st.session_state['ts_ui_macd']=='Negativo' else ''}>Negativo</option><option value="No exigir" {'selected' if st.session_state['ts_ui_macd']=='No exigir' else ''}>No exigir</option></select></div>
    <div class="item"><label>REFRESCO:</label><input id="cfg_refresh" type="number" min="1" value="{st.session_state['ts_ui_refresh']}" onchange="pushUpdate()"></div>

    <div class="item"><label>ORDENAR:</label><select id="cfg_order" onchange="pushUpdate()"><option value="Actualizado" {'selected' if st.session_state['ts_ui_order']=='Actualizado' else ''}>Actualizado</option><option value="Cambio %" {'selected' if st.session_state['ts_ui_order']=='Cambio %' else ''}>Cambio %</option><option value="Volumen" {'selected' if st.session_state['ts_ui_order']=='Volumen' else ''}>Volumen</option></select></div>
    <div class="item"><label>TOP N:</label><input id="cfg_top" type="number" min="1" max="100" value="{st.session_state['ts_ui_top']}" onchange="pushUpdate()"></div>
    <div class="item"><label>AUTO:</label><select id="cfg_auto" onchange="pushUpdate()"><option value="1" {'selected' if st.session_state['ts_ui_auto'] else ''}>🟢 ON</option><option value="0" {'selected' if not st.session_state['ts_ui_auto'] else ''}>🔴 OFF</option></select></div>
    <div class="item buttons"><button class="save" onclick="pushUpdate()">GUARDAR</button><button class="reset" onclick="resetAll()">REINICIAR</button></div>
  </div>

  <div class="tablewrap">
    <table>
      <thead><tr><th>LAYOUT</th><th>TICKER</th><th>SECTOR</th><th>PRECIO ($)</th><th>CAMBIO %</th><th>GAP %</th><th>FLOAT</th><th>EMA 20 INTRADÍA</th><th>MACD</th><th>VOLUMEN</th></tr></thead>
      <tbody id="rows"></tbody>
    </table>
  </div>
</div>
<script>
const data={_inst_json};
const tbody=document.getElementById('rows');
if(!data.length) tbody.innerHTML='<tr><td colspan="10" style="text-align:center;padding:16px;background:#fff;color:#555;">SIN CANDIDATOS EN ESTE MOMENTO.</td></tr>';
else data.forEach(r=>{{
  const tr=document.createElement('tr'); tr.className=r.cambio>=0?'up':'down';
  const mc=r.macd==='Positivo'?'macd-pos':(r.macd==='Negativo'?'macd-neg':'macd-neu');
  const cc=r.cambio>=0?'pos':'neg';
  const gc=r.gap>=0?'pos':'neg';
  tr.innerHTML=`<td><select class="layout" onchange="layoutSignal('${{r.ticker}}',this)"><option value="">⚙️ --</option><option>L1</option><option>L2</option><option>L3</option><option>L4</option><option>L5</option><option>L6</option><option>L7</option><option>L8</option><option>L9</option><option>L10</option></select></td><td class="ticker">${{r.ticker}}</td><td>${{r.sector}}</td><td class="num">${{r.precio.toFixed(2)}}</td><td class="num ${{cc}}">${{r.cambio>=0?'+':''}}${{r.cambio.toFixed(2)}}%</td><td class="num ${{gc}}">${{r.gap>=0?'+':''}}${{r.gap.toFixed(2)}}%</td><td class="num">${{r.float===null?'Sin dato':r.float.toFixed(1)+'M'}}</td><td>${{r.ema}}</td><td class="${{mc}}">${{r.macd}}</td><td class="num">${{r.volumen.toLocaleString()}}</td>`;
  tbody.appendChild(tr);
}});
</script>
</body>
</html>
"""

st.components.v1.html(_html_institutional, height=760, scrolling=False)


@st.fragment(run_every=(f"{int(REFRESCO)}s" if AUTO_ON else None))
def panel_resultados():
    filas = filtrar_resultados(list(servicio.resultados), params)

    if servicio.ultima_actualizacion:
        if ETAPA_PRUEBA_FILTROS == 1:
            detalle = (f"Última actualización: {servicio.ultima_actualizacion.strftime('%H:%M:%S')} ET"
                       f" · ciclo {servicio.duracion_ciclo:.1f}s"
                       f" · {len(servicio.universo)} tickers vigilados"
                       f" · {servicio.n_radar_base} en el radar base"
                       f" (solo precio ${BASE_PRECIO_MIN:.2f}-${BASE_PRECIO_MAX:.0f}; EMA20 arriba + MACD positivo)")
        else:
            detalle = (f"Última actualización: {servicio.ultima_actualizacion.strftime('%H:%M:%S')} ET"
                       f" · ciclo {servicio.duracion_ciclo:.1f}s"
                       f" · {len(servicio.universo)} tickers vigilados"
                       f" · {servicio.n_radar_base} en el radar base"
                       f" (precio ${BASE_PRECIO_MIN:.2f}-${BASE_PRECIO_MAX:.0f}, subida ≥ {BASE_GAP_MIN:.0f}%, float ≤ {formatear_numero_grande(BASE_FLOTACION_MAX)}, volumen actual ≥ {formatear_numero_grande(servicio.filtros_dueno.get("volumen_min", 20_000))})")
        st.caption(detalle)
    else:
        st.caption("Esperando el primer escaneo (la primera vez puede tardar un minuto)...")

    if servicio.ultimo_error:
        st.warning(f"Aviso del motor: {servicio.ultimo_error}")

    st.markdown(f"**{len(filas)} resultados** · el motor escanea cada {INTERVALO_ESCANEO_SEGUNDOS}s")

    if not filas:
        st.info("Sin candidatos que cumplan los filtros en este momento.")
        return

    df = pd.DataFrame([
        {
            "No.": i + 1,
            "Ticker": c["ticker"],
            "Precio": round(c["precio"], 2),
            "Cambio %": round(c["cambio_pct"], 1),
            "Volumen": formatear_numero_grande(c["volumen_dia"]),
            "Flotación": (
                formatear_numero_grande(c["float_shares"])
                if c["float_shares"] is not None
                else ("Pendiente" if c.get("float_status") == "pending" else "Sin dato")
            ),
            "EMA20": "✅" if c["cruzando_ema20"] else ("🔻" if c.get("cruzando_ema20_abajo") else ""),
            "MACD": "✅" if c["macd_positivo"] else ("🔻" if c.get("macd_negativo") else ""),
            "BB sup.": round(c.get("bb_upper"), 2) if c.get("bb_upper") is not None else None,
            "Dist. BB %": round(c.get("bb_dist_pct"), 1) if c.get("bb_dist_pct") is not None else None,
            "Noticia": "🔥" if c["tiene_noticia"] else "",
            "Actualizado (ET)": c["actualizado"].astimezone(ET).strftime("%H:%M:%S") if hasattr(c["actualizado"], "astimezone") else str(c["actualizado"]),
        }
        for i, c in enumerate(filas)
    ])

    styled = (
        df.style
        .map(color_cambio, subset=["Cambio %"])
        .set_properties(**{"background-color": "#080808", "color": "#eeeeee", "border-color": "#2b2512"})
        .set_table_styles([{"selector": "th", "props": [("background-color", "#0b0b0b"), ("color", "#d4af37"), ("font-weight", "bold"), ("border-color", "#5d4b19")]}])
    )
    seleccion = st.dataframe(
        styled, width="stretch", hide_index=True,
        on_select="rerun", selection_mode="single-row", key="tabla_resultados",
    )
    filas_sel = seleccion.selection.rows if seleccion and seleccion.selection else []
    if filas_sel:
        st.session_state["ticker_activo"] = df.iloc[filas_sel[0]]["Ticker"]


panel_resultados()


# ==========================================
# 🔗 PANEL BROKER: 10 activos del scanner ↔ 10 colores ↔ 10 layouts del broker
# Clic en una fila = envía ese símbolo al layout del color de esa fila.
#   · Con webhook para ese color (ej. trigger de Macro Deck): se envía ahí.
#   · Sin webhook: se envía al "puente" local (URL general).
#   · Quantfury: copia el ticker al portapapeles.
# El envío lo hace TU NAVEGADOR (no el servidor), así funciona con http://127.0.0.1 en tu PC.
# No coloca órdenes: solo manda el símbolo.
# ==========================================

PANEL_BROKER_ALTO_PX = 500
PUENTE_LOCAL_POR_DEFECTO = "http://127.0.0.1:8765/enviar"
BROKERS_DISPONIBLES = [
    "Interactive Brokers (TWS)", "TradeZero (webhook)", "Binance (webhook)",
    "Quantfury (portapapeles)", "Otro (webhook)",
]
RUTA_PANEL_BROKER = os.path.join(os.getcwd(), "config_panel_broker.json")

def _texto_contraste(hex_color):
    """Elige texto negro/blanco según el brillo del color elegido."""
    try:
        h = str(hex_color).lstrip("#")
        if len(h) != 6:
            return "#000000"
        r, g, b = (int(h[i:i+2], 16) for i in (0, 2, 4))
        brillo = (r * 299 + g * 587 + b * 114) / 1000
        return "#000000" if brillo >= 150 else "#ffffff"
    except Exception:
        return "#000000"

def colores_layout_actuales():
    personalizados = st.session_state.get("bk_colores", [])
    resultado = []
    for i, (nombre, bg_def, _fg_def) in enumerate(COLORES_LAYOUT_DEFECTO):
        bg = personalizados[i] if i < len(personalizados) and personalizados[i] else bg_def
        resultado.append((nombre, bg, _texto_contraste(bg)))
    return resultado

CSS_PANEL_BROKER = (
    "body{margin:0;background:transparent;font-family:Calibri,'Segoe UI',Arial,sans-serif;}"
    ".tbl-wrap{width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;}"
    "table{width:100%;min-width:560px;border-collapse:collapse;table-layout:fixed;}"
    "th{background:#0b0b0b;color:#d4af37;font-weight:700;font-size:15px;padding:8px 6px;"
    "border:1px solid #5d4b19;text-align:center;line-height:1.15;}"
    "th.cred{display:none;}"
    "th.cred .m{font-weight:400;font-size:12px;opacity:.9;}"
    "th.cred .bk{font-weight:400;font-size:11px;opacity:.85;margin-top:2px;}"
    "td{height:34px;border:1px solid #808080;text-align:center;font-size:14px;color:#111111;"
    "background:#ffffff;padding:0 4px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;}"
    "th.colhdr{width:32px;min-width:32px;max-width:32px;padding:4px 0;}"
    "td.col{width:32px;min-width:32px;max-width:32px;padding:0;text-align:center;}"
    "th.gearhdr,td.gear{width:30px;min-width:30px;max-width:30px;padding:0;text-align:center;}"
    "td.gear{font-size:18px;color:#d4af37;cursor:pointer;}"
    ".swatch{display:inline-block;width:14px;height:14px;border-radius:3px;border:1px solid rgba(255,255,255,.55);vertical-align:middle;}"
    "td.sym{font-weight:700;}"
    "tr.fila.ok{cursor:pointer;}"
    "tr.fila.ok:hover td:not(.col){filter:brightness(.94);}"
    "tr.pos td:not(.col){background:#0d1b13;color:#7be2a7;}"
    "tr.neg td:not(.col){background:#1b0d0d;color:#f28a8a;}"
    "tr.sel td:not(.col){box-shadow:inset 0 0 0 2px #d4af37;}"
    "#msg{margin-top:8px;padding:6px 10px;border-radius:6px;background:#15120b;color:#f0dfaa;border:1px solid #5d4b19;"
    "font-size:13px;display:none;}"
    # --- Responsivo: celular. La tabla no se aprieta, se puede deslizar horizontal ---
    "@media (max-width:640px){"
    "  th{font-size:12px;padding:6px 4px;}"
    "  th.colhdr{width:28px;min-width:28px;max-width:28px;}"
    
    "  td{font-size:12px;height:30px;}"
    "  td.gear{font-size:16px;}"
    "  .swatch{width:12px;height:12px;}"
    "  #msg{font-size:11px;}"
    "}"
)

JS_PANEL_BROKER = r"""
(function(){
  const D = JSON.parse(document.getElementById("datos").textContent);
  const msg = document.getElementById("msg");
  let temporizador = null;
  function aviso(txt, tipo){
    msg.textContent = txt;
    msg.style.display = "block";
    msg.style.background = tipo === "ok" ? "#166534" : (tipo === "err" ? "#991b1b" : "#1f2937");
    clearTimeout(temporizador);
    temporizador = setTimeout(function(){ msg.style.display = "none"; }, 6000);
  }
  function guardarSel(tk){ try { sessionStorage.setItem("sel_ticker", tk); } catch(e) {} }
  function leerSel(){ try { return sessionStorage.getItem("sel_ticker"); } catch(e) { return null; } }

  const filas = document.querySelectorAll("tr.fila");
  const previa = leerSel();
  filas.forEach(function(tr){
    const d = D.filas[+tr.dataset.i];
    if (d && d.ticker === previa) tr.classList.add("sel");
  });

  filas.forEach(function(tr){
    tr.addEventListener("click", async function(){
      const i = +tr.dataset.i;
      const d = D.filas[i];
      if (!d) return;
      filas.forEach(function(x){ x.classList.remove("sel"); });
      tr.classList.add("sel");
      guardarSel(d.ticker);
      const color = D.colores[i];
      const broker = D.cfg.broker || "";
      const payload = {simbolo: d.ticker, ticker: d.ticker, color: color, color_num: i + 1, broker: broker};

      // 1) Quantfury: copiar al portapapeles
      if (broker.indexOf("Quantfury") === 0) {
        try {
          await navigator.clipboard.writeText(d.ticker);
          aviso("✔ " + d.ticker + " copiado: pégalo en Quantfury", "ok");
        } catch(e) {
          aviso("⚠ No pude copiar automáticamente. Ticker: " + d.ticker, "err");
        }
        return;
      }

      // 2) Webhook propio de este color (ej. trigger de Macro Deck)
      const hook = ((D.webhooks || [])[i] || "").trim();
      if (hook) {
        aviso("Enviando " + d.ticker + " → " + color + "…", "info");
        try {
          await fetch(hook, {method: "POST", mode: "no-cors",
                             headers: {"Content-Type": "text/plain"}, body: JSON.stringify(payload)});
          aviso("✔ " + d.ticker + " enviado al webhook de " + color + " (sin confirmación de respuesta)", "ok");
        } catch(e) {
          aviso("⚠ No pude alcanzar el webhook de " + color, "err");
        }
        return;
      }

      // 3) Puente local general
      const puente = (D.cfg.puente || "").trim();
      if (!puente) {
        aviso("⚠ Falta la URL del puente o el webhook de " + color + " (botón ✏️)", "err");
        return;
      }
      payload.api_key = D.cfg.api_key;
      payload.api_secret = D.cfg.api_secret;
      aviso("Enviando " + d.ticker + " → " + color + "…", "info");
      try {
        const r = await fetch(puente, {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify(payload)
        });
        let j = {};
        try { j = await r.json(); } catch(e) {}
        if (r.ok && j.ok !== false) aviso("✔ " + d.ticker + " enviado al layout " + color, "ok");
        else aviso("⚠ El puente respondió con error" + (j.error ? ": " + j.error : ""), "err");
      } catch(e) {
        aviso("⚠ Sin conexión con el puente del broker (" + puente + ")", "err");
      }
    });
  });
})();
"""


def construir_html_panel_broker(filas10, cfg, colores_layout=None):
    """HTML del panel con columna compacta de colores configurables."""
    colores_layout = colores_layout or COLORES_LAYOUT_DEFECTO
    key = cfg.get("api_key") or ""
    sec = cfg.get("api_secret") or ""
    key_txt = ("••••" + key[-4:]) if key else ""
    sec_txt = "••••••••" if sec else ""
    broker_txt = html_escape(cfg.get("broker") or "") if (key or sec) else ""

    cuerpo = []
    for i, (nombre, bg, fg) in enumerate(colores_layout):
        d = filas10[i] if i < len(filas10) else None
        celda_color = f'<td class="col"><span class="swatch" style="background:{bg}"></span></td>'
        if d is None:
            cuerpo.append(f'<tr class="fila" data-i="{i}"><td class="gear" title="Vincular este activo con el layout del broker">🔗</td>{celda_color}' + "<td></td>" * 6 + "</tr>")
            continue
        clase = "ok " + ("pos" if d["subiendo"] else "neg")
        cuerpo.append(
            f'<tr class="fila {clase}" data-i="{i}">'
            f'<td class="gear" title="Vincular este activo con el layout del broker">🔗</td>'
            f'{celda_color}'
            f'<td class="sym">{html_escape(str(d["ticker"]))}{" 🔥" if d["noticia"] else ""}</td>'
            f'<td>{d["precio"]:.2f}</td>'
            f'<td>{d["cambio"]:+.1f}%</td>'
            f'<td>{d["volumen"]}</td>'
            f'<td>{d["flotacion"]}</td>'
            f'<td>{d["volrel"]:.2f}</td>'
            f'</tr>'
        )

    webhooks = list(cfg.get("webhooks") or [])[: len(colores_layout)]
    webhooks += [""] * (len(colores_layout) - len(webhooks))

    datos = {
        "filas": [
            ({"ticker": d["ticker"]} if d else None)
            for d in (list(filas10) + [None] * (len(colores_layout) - len(filas10)))[: len(colores_layout)]
        ],
        "colores": [c[1] for c in colores_layout],
        "webhooks": webhooks,
        "cfg": {
            "broker": cfg.get("broker") or "",
            "api_key": key,
            "api_secret": sec,
            "puente": cfg.get("puente") or "",
        },
    }
    datos_json = json.dumps(datos, ensure_ascii=False).replace("</", "<\\/")

    return (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<style>" + CSS_PANEL_BROKER + "</style></head><body>"
        "<div class='tbl-wrap'>"
        "<table><thead><tr>"
        '<th class="gearhdr">🔗</th><th class="colhdr">&nbsp;</th><th>Símbolo / Noticia</th><th>Precio</th><th>Cambio %</th>' 
        "<th>Volumen</th><th>Flotación</th><th>Vol. Relativo</th>"
        "</tr></thead><tbody>" + "".join(cuerpo) + "</tbody></table>"
        "</div>"
        '<div id="msg"></div>'
        f'<script type="application/json" id="datos">{datos_json}</script>'
        "<script>" + JS_PANEL_BROKER + "</script>"
        "</body></html>"
    )


def _direccion_por_ticker():
    """Última dirección conocida (True = subiendo / False = bajando) de cada ticker, según los eventos."""
    dirs = {}
    for ev in list(getattr(servicio, "eventos", [])):  # el más nuevo primero
        dirs.setdefault(ev["ticker"], ev["subiendo"])
    return dirs


# --- Configuración por licencia (broker, puente y webhooks; la API Key/Secret NO se guardan en disco) ---
def _clave_usuario():
    return hashlib.sha256(str(TOKEN_ACTIVO).encode("utf-8")).hexdigest()[:16]


def cargar_panel_broker():
    try:
        with open(RUTA_PANEL_BROKER, "r", encoding="utf-8") as f:
            d = json.load(f).get(_clave_usuario(), {})
    except Exception:
        d = {}
    broker = d.get("broker", BROKERS_DISPONIBLES[0])
    if broker not in BROKERS_DISPONIBLES:
        broker = BROKERS_DISPONIBLES[0]
    webhooks = [str(w) for w in list(d.get("webhooks", []))[: len(COLORES_LAYOUT_DEFECTO)]]
    webhooks += [""] * (len(COLORES_LAYOUT_DEFECTO) - len(webhooks))
    colores = [str(c) for c in list(d.get("colores", []))[: len(COLORES_LAYOUT_DEFECTO)]]
    colores += [bg for _n, bg, _fg in COLORES_LAYOUT_DEFECTO[len(colores):]]
    return {"broker": broker, "puente": d.get("puente", PUENTE_LOCAL_POR_DEFECTO), "webhooks": webhooks, "colores": colores}


def guardar_panel_broker(cfg):
    try:
        try:
            with open(RUTA_PANEL_BROKER, "r", encoding="utf-8") as f:
                todo = json.load(f)
        except Exception:
            todo = {}
        todo[_clave_usuario()] = cfg
        with open(RUTA_PANEL_BROKER, "w", encoding="utf-8") as f:
            json.dump(todo, f)
    except Exception:
        pass


if "bk_cargado" not in st.session_state:
    _ini = cargar_panel_broker()
    st.session_state.setdefault("bk_nombre", _ini["broker"])
    st.session_state.setdefault("bk_puente", _ini["puente"])
    st.session_state.setdefault("bk_api_key", "")
    st.session_state.setdefault("bk_api_secret", "")
    for _i, _w in enumerate(_ini["webhooks"]):
        st.session_state.setdefault(f"bk_wh_{_i}", _w)
    st.session_state.setdefault("bk_colores", list(_ini.get("colores", [bg for _n, bg, _fg in COLORES_LAYOUT_DEFECTO])))
    st.session_state.setdefault("_bk_guardado", _ini)
    st.session_state["bk_cargado"] = True

# --- Persistencia de configuración del broker y colores ---
_cfg_guardable = {
    "broker": st.session_state.get("bk_nombre", BROKERS_DISPONIBLES[0]),
    "puente": st.session_state.get("bk_puente", PUENTE_LOCAL_POR_DEFECTO),
    "webhooks": [st.session_state.get(f"bk_wh_{_i}", "") for _i in range(len(COLORES_LAYOUT_DEFECTO))],
    "colores": list(st.session_state.get("bk_colores", [bg for _n,bg,_fg in COLORES_LAYOUT_DEFECTO])),
}
if st.session_state.get("_bk_guardado") != _cfg_guardable:
    guardar_panel_broker(_cfg_guardable)
    st.session_state["_bk_guardado"]=_cfg_guardable


@st.fragment(run_every=(f"{int(REFRESCO)}s" if AUTO_ON else None))
def panel_broker():
    filas = filtrar_resultados(list(servicio.resultados), params)[: len(COLORES_LAYOUT)]
    dirs = _direccion_por_ticker()
    filas10 = [
        {
            "ticker": c["ticker"],
            "noticia": bool(c["tiene_noticia"]),
            "precio": c["precio"],
            "cambio": c["cambio_pct"],
            "volumen": formatear_numero_grande(c["volumen_dia"]),
            "flotacion": (
                formatear_numero_grande(c["float_shares"])
                if c["float_shares"] is not None
                else ("Pendiente" if c.get("float_status") == "pending" else "Sin dato")
            ),
            "volrel": c["volumen_relativo"],
            "subiendo": dirs.get(c["ticker"], True),
        }
        for c in filas
    ]
    if ES_ADMIN:
        cfg = {
            "broker": st.session_state.get("bk_nombre", BROKERS_DISPONIBLES[0]),
            "api_key": st.session_state.get("bk_api_key", ""),
            "api_secret": st.session_state.get("bk_api_secret", ""),
            "puente": st.session_state.get("bk_puente", ""),
            "webhooks": [st.session_state.get(f"bk_wh_{_i}", "") for _i in range(len(COLORES_LAYOUT_DEFECTO))],
        }
    else:
        # Nunca enviar credenciales, webhooks ni puentes privados al navegador
        # de un usuario normal.
        cfg = {
            "broker": "",
            "api_key": "",
            "api_secret": "",
            "puente": "",
            "webhooks": ["" for _ in range(len(COLORES_LAYOUT_DEFECTO))],
        }
    st.iframe(construir_html_panel_broker(filas10, cfg, colores_layout_actuales()), height=PANEL_BROKER_ALTO_PX)


panel_broker()