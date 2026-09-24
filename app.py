import time
import base64
import json
from PIL import Image
from openai import OpenAI
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

# ==========================================
# 1. CONFIGURACIÓN DE CREDENCIALES Y LLAVES
# ==========================================
DEEPSEEK_API_KEY = "sk-8f7b21227e494547931f908d5f9068ed"
ALPACA_API_KEY = "PKS25MKAHSFGTQTSXUSWQC46ZE"
ALPACA_SECRET_KEY = "cnhS7BpuWw5dQopXm98giNc1B8dXwKGzb93rioeuhd3"

# Inicialización de clientes
print("🔌 Conectando con los servidores de DeepSeek y Alpaca...")
ai_client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://deepseek.com")
alpaca = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)

# ==========================================
# 2. FUNCIÓN AUXILIAR PARA PROCESAR IMÁGENES
# ==========================================
def encode_image_to_base64(image_path):
    print(f"📸 Cargando y procesando la imagen: {image_path}...")
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

# ==========================================
# 3. CEREBRO DEL BOT: ANÁLISIS MULTIMODAL
# ==========================================
def analizar_mercado_con_deepseek(ruta_grafico, texto_noticia, ticker):
    imagen_base64 = encode_image_to_base64(ruta_grafico)
    
    prompt_sistema = (
        "Eres un sistema experto en trading. Analiza el grafico y la noticia. "
        "Responde UNICAMENTE con este formato JSON: "
        '{"decision": "BUY" o "SELL" o "HOLD", "razon": "texto"}'
    )
    prompt_usuario = f"Analiza el grafico adjunto y la noticia sobre {ticker}: '{texto_noticia}'"

    print(f"🧠 Enviando datos a DeepSeek para analizar {ticker}...")
    try:
        respuesta = ai_client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": prompt_sistema},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt_usuario},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{imagen_base64}"}
                        }
                    ]
                }
            ],
            temperature=0.2
        )
        texto_limpio = respuesta.choices.message.content.strip().replace("`json", "").replace("```", "").strip()
        resultado_json = json.loads(texto_limpio)
        return resultado_json
    except Exception as e:
        print(f"⚠️ Error en API de DeepSeek: {e}")
        return {"decision": "HOLD", "razon": "Error de ejecucion"}

# ==========================================
# 4. MOTOR DE EJECUCIÓN CON ALPACA-PY
# ==========================================
def ejecutar_orden_absoluta(ticker, analisis_ia, cantidad_acciones=10):
    decision = analisis_ia.get("decision")
    razon = analisis_ia.get("razon")
    
    print(f"\n📊 Análisis recibido de DeepSeek para {ticker}:")
    print(f"🔹 Decisión: {decision}")
    print(f"📌 Razón: {razon}\n")

    if decision == "BUY":
            try:
                orden = alpar.submit_order(order_data=MarketOrderRequest(symbol=ticker, qty=cantidad_acciones, side=OrderSide.BUY, time_in_force=TimeInForce.GTC))
                print("Orden de compra enviada exitosamente")
            except Exception as e:
                print(f"Error al enviar orden de compra: {e}")
                
            try:
                orden_sp = alpar.submit_order(order_data=MarketOrderRequest(symbol=ticker, qty=cantidad_acciones, side=OrderSide.BUY, time_in_force=TimeInForce.GTC))
                print("Orden secundaria enviada exitosamente")
            except Exception as e:
                print(f"Error en la orden secundaria: {e}")

if __name__ == "__main__":
    TICKER_A_OPERAR = "AAPL"
    CANTIDAD = 1  # Ajusta la cantidad de acciones a tu gusto
    
    print("🤖 Bot de trading automático iniciado...")
    
    while True:
        try:
            print("\n🔄 Iniciando un nuevo ciclo de análisis...")
            # Aquí llamamos a tu función principal para que analice y ejecute la orden
            analisis_ia = analizar_mercado_con_deepseek("grafico_alpaca.png", "noticia_mercado.txt", TICKER_A_OPERAR)
            ejecutar_orden_absoluta(TICKER_A_OPERAR, analisis_ia, CANTIDAD)
            
        except Exception as e:
            print(f"❌ Ocurrió un error en este ciclo: {e}")
            
        # El bot esperará 5 minutos (300 segundos) antes de volver a analizar
        print("⏳ Esperando 5 minutos para el próximo análisis...")
        time.sleep(300)
