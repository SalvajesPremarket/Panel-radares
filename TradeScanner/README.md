# TradeScanner

Este directorio contiene el código principal del scanner comercial.

- `app.py`: aplicación Streamlit principal del TradeScanner.
- `webapp/`: módulos de autenticación, API, billing, recomendaciones y soporte del scanner.

El `app.py` de la raíz se conserva únicamente como lanzador para mantener el despliegue existente.

El robot de trading vive separado en `BotTradeScanner/` y no contiene su lógica dentro de este scanner.
