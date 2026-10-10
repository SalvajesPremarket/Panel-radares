# Integración Cuenta → Scanner → API → Bot

Esta capa rodea el motor existente. El punto de entrada Streamlit es `app.py`; la publicación de señales es opcional y no debe cambiar filtros, fórmulas ni resultados del scanner.

## Flujo actual

scanner Streamlit → publisher de señales → API privada de ingestión → servicio de señales → consumidores autorizados

El bot LONG usa su integración y motor de velas compartido dentro de `BotTradeScanner/`. La web auxiliar administra páginas, sesiones, cuentas y la API de señales. Estos flujos tienen responsabilidades separadas.

## Servicios existentes

- `webapp/server.py`: punto de entrada FastAPI que reúne páginas, autenticación, cuenta y API.
- `webapp/routes.py`: sirve las páginas HTML de inicio, login, registro, cuenta y scanner.
- `webapp/auth/server.py`: registro, inicio/cierre de sesión y validación de sesiones.
- `webapp/account/server.py`: datos de cuenta, control de acceso y recomendaciones.
- `webapp/api/server.py`: API privada de señales; protege la ingestión con una clave del servidor.
- `webapp/api/signal_service.py`: contrato y almacenamiento temporal de señales.
- `webapp/api/publisher.py`: publica señales finales del scanner cuando están configurados el endpoint y el secreto.
- `BotTradeScanner/integracion/live_motor_bridge.py`: puente del motor de velas para el bot.
- `BotTradeScanner/integracion/bot_long_realtime.py`: ciclo del bot LONG en tiempo real.
- `BotTradeScanner/riesgo/paper.py`: evaluación y controles de riesgo del flujo PAPER.

Los endpoints heredados `BotTradeScanner/api/server.py` y el adaptador `webapp/api/scanner_adapter.py` fueron retirados porque no formaban parte del flujo activo. No deben volver a referenciarse como servicios disponibles.

## Estado y límites

1. `app.py` permanece como único punto de entrada de Streamlit.
2. El publisher no altera el scanner; los fallos al publicar no deben interrumpir el ciclo.
3. La ingestión de señales requiere el secreto configurado en el servidor.
4. El bot y el scanner comparten datos mediante interfaces explícitas; no se deben duplicar conexiones de mercado por usuario.
5. La interfaz operativa actual presenta el bot en modo PAPER. No activar ejecución LIVE ni pagos como parte de esta limpieza.
6. Los cambios en filtros, fórmulas, estrategia o ejecución requieren una solicitud explícita y pruebas propias.

## Próximas verificaciones

Probar por separado autenticación, permisos, ingestión de señales e idempotencia. Confirmar que el publisher esté configurado en el entorno de despliegue antes de dar por hecho que las señales llegan a la API. La ejecución real contra un broker y la integración de pagos requieren proyectos separados y autorización explícita.

Las recomendaciones de usuarios nunca modifican automáticamente el motor.
