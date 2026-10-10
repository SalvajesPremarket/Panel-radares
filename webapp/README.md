# TradeScanner Web Platform

Esta carpeta contiene la capa web auxiliar, separada del motor principal de Streamlit.

## Entradas y despliegue

- `app.py` en la raíz: único punto de entrada del scanner Streamlit.
- `webapp/server.py`: aplicación FastAPI unificada para páginas, autenticación, cuenta y API.
- `webapp/routes.py`: sirve `index.html`, `login.html`, `register.html`, `account/dashboard.html` y `scanner/index.html`.
- `webapp/auth/`: autenticación y sesiones.
- `webapp/account/`: página y endpoints de cuenta.
- `webapp/api/`: API privada, señales, publisher y adaptador de integración.
- `webapp/billing/README.md`: documentación de billing previsto; no es un servicio de pago implementado.
- `webapp/product/` y `webapp/recommendations/`: documentación y esquema de recomendaciones.

La estrategia, el riesgo y la ejecución/paper del bot están en `BotTradeScanner/`, fuera de esta carpeta. No existe un paquete `webapp/bot/` ni un directorio `webapp/database/` en la estructura actual.

## Separación y seguridad

No mover ni reescribir `app.py` para agregar cuentas, pagos o el bot. El scanner y el bot deben comunicarse mediante contratos explícitos; el adaptador no debe cambiar fórmulas ni filtros por sí solo.

Las claves de mercado y broker no deben exponerse al navegador ni incluirse en archivos públicos. La ejecución real contra un broker requiere autorización explícita, controles de riesgo, auditoría e interruptor de emergencia. El bot actual debe tratarse como paper/validación hasta verificar la configuración de ejecución.

## Arranque de la Web App

Desde la raíz del repositorio:

```bash
pip install -r webapp/requirements.txt
uvicorn webapp.server:app --host 0.0.0.0 --port 8000
```

Configura `TRADESCANNER_SESSION_SECRET` antes de iniciar. En producción, usa HTTPS y revisa las variables secretas requeridas por cada servicio.
