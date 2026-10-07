# Stage 3 — Contrato de arquitectura

Stage 3 no es una segunda aplicación Streamlit. Es la capa de integración que permite que TradeScanner y BotTradeScanner trabajen juntos sin duplicar conexiones de mercado ni mezclar responsabilidades.

## Componentes

1. TradeScanner: detecta y publica candidatos.
2. Market stream compartido: entrega datos de mercado al motor de velas.
3. BotTradeScanner: consume snapshots/candidatos y ejecuta la lógica LONG/SHORT y el control de riesgo.
4. webapp: servicios auxiliares de autenticación, API, billing y producto.

## Regla crítica

Debe existir un único entry point de Streamlit para el scanner: /app.py.

BotTradeScanner no debe depender de variables internas de Streamlit. La comunicación debe pasar por contratos explícitos, como MotorVelasBridge.

El objetivo es soportar múltiples usuarios sin crear una conexión WebSocket de Alpaca por usuario y sin duplicar el motor.

## Estado

La integración Scanner → MotorVelas → Bot LONG ya existe. El bridge limita la suscripción a 30 símbolos y puede utilizar el market stream compartido del scanner.
