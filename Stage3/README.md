# Stage 3 — Contrato de arquitectura

Stage 3 no es una segunda aplicación Streamlit. Es la capa de integración que permite que TradeScanner y BotTradeScanner trabajen juntos sin duplicar conexiones de mercado ni mezclar responsabilidades.

## Componentes

1. TradeScanner: detecta candidatos y muestra los resultados del scanner.
2. Market stream compartido: entrega datos de mercado al motor de velas.
3. BotTradeScanner: consume candidatos/snapshots y ejecuta la estrategia LONG actual junto con sus controles de riesgo.
4. webapp: servicios auxiliares de páginas, autenticación, cuenta y API de señales.

La estrategia SHORT no está implementada en esta fase y queda fuera del alcance actual.

## Regla crítica

Debe existir un único punto de entrada de Streamlit para el scanner: `/app.py`.

BotTradeScanner no debe depender de variables internas de Streamlit. La comunicación debe pasar por contratos explícitos, como `MotorVelasBridge`.

El objetivo es soportar múltiples usuarios sin crear una conexión WebSocket de Alpaca por usuario ni duplicar el motor.

## Estado

La integración del motor de velas compartido con el bot LONG existe. El puente puede utilizar el market stream del scanner y mantiene límites de suscripción configurados en el código. El publisher de señales de la web es un canal aparte: su entrega depende de que endpoint y secreto estén configurados en el entorno.

## Límites de esta fase

- Mantener `app.py` como punto de entrada único de Streamlit.
- No alterar filtros ni fórmulas del scanner desde la capa de integración.
- Mantener el bot en PAPER mientras no exista una autorización explícita para otra modalidad.
- No habilitar pagos ni ejecución LIVE como parte de la depuración documental.
