# Integración Cuenta → Scanner → API → Bot

Esta capa rodea el motor existente y no modifica app.py.

## Flujo

usuario autenticado → permisos → scanner → señal normalizada → API privada → bot → riesgo → paper

## Servicios

- webapp/auth/server.py: identidad y sesión.
- webapp/account/server.py: cuenta, acceso y recomendaciones.
- webapp/api/signal_service.py: contrato y almacenamiento temporal de señales.
- webapp/api/server.py: API privada de señales.
- webapp/bot/paper.py: evaluación de señales y controles de riesgo.
- webapp/bot/server.py: endpoints del bot.

## Estado actual

1. La cuenta controla el acceso.
2. Las señales tienen un esquema estable.
3. El bot puede evaluar señales en modo paper.
4. No existe ejecución real contra un broker.
5. No se exponen claves FMP, Alpaca, Telegram o Schwab.
6. app.py permanece intacto.

## Próximo paso

Conectar el motor real mediante un adaptador que publique señales normalizadas. Después añadiremos persistencia, idempotencia, paper trading con órdenes simuladas y, mucho más adelante, una capa de ejecución real con autorización explícita.

Las recomendaciones de usuarios nunca modifican automáticamente el motor.
