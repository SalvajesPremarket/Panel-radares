# TradeScanner Web Platform

Esta carpeta contiene la capa web auxiliar, separada del motor principal de Streamlit.

## Entradas y despliegue

- `app.py` en la raíz: único punto de entrada del scanner Streamlit.
- `webapp/server.py`: aplicación FastAPI unificada para páginas, autenticación, cuenta y API.
- `webapp/routes.py`: sirve `index.html`, `login.html`, `register.html`, `account/dashboard.html` y `scanner/index.html`.
- `webapp/auth/`: autenticación y sesiones.
- `webapp/account/`: página y endpoints de cuenta.
- `webapp/api/`: API privada de señales, servicio de señales y publisher.
- `webapp/billing/README.md`: documentación de billing previsto; no es un servicio de pago implementado.
- `webapp/product/` y `webapp/recommendations/`: documentación y esquema de recomendaciones.

Los endpoints heredados `BotTradeScanner/api/server.py` y el adaptador `webapp/api/scanner_adapter.py` fueron retirados por no formar parte del flujo activo. La publicación existente está en `webapp/api/publisher.py`; no asumir que está operativa en producción sin verificar la configuración del endpoint y el secreto.

La estrategia, el riesgo y la ejecución PAPER del bot están en `BotTradeScanner/`, fuera de esta carpeta. No existe un paquete `webapp/bot/` ni un directorio `webapp/database/` en la estructura actual. Las señales normalizadas y el estado del simulador Paper manual se guardan en PostgreSQL cuando `DATABASE_URL` está configurada.

## Hosting objetivo: plataforma propia

El destino de este proyecto es el VPS propio descrito en `deploy/self-host/README.md`. Render es solo el alojamiento heredado que se mantiene durante la transición; no hacer nuevas mejoras de interfaz ni contratar recursos adicionales allí como parte de esta preparación.

No apagar la instancia heredada hasta que el VPS propio esté instalado y se haya validado el recorrido completo: TradeScanner → ingestión autenticada → TradeBot → decisión/salida Paper, además de persistencia, reinicios, copias de seguridad y HTTPS.

En la plataforma propia, Docker Compose conecta la web FastAPI y el scanner Streamlit con PostgreSQL por la red privada. Caddy es el único servicio que publica los puertos 80 y 443; el puerto de PostgreSQL y los puertos internos de las aplicaciones no deben exponerse públicamente. Las claves y dominios reales se configuran en `deploy/self-host/.env`, nunca en GitHub. El runtime automático Paper permanece desactivado por defecto.

Para la configuración, comprobaciones y procedimiento de respaldo/restauración, sigue la guía central `deploy/self-host/README.md`. La preparación en GitHub no significa que ya exista un VPS desplegado ni que la conexión de señales esté probada en producción.

## Separación y seguridad

No mover ni reescribir `app.py` para agregar cuentas, pagos o el bot. El scanner y el bot deben comunicarse mediante contratos explícitos; el publisher no debe cambiar fórmulas ni filtros por sí solo.

Las claves de mercado y broker no deben exponerse al navegador ni incluirse en archivos públicos. La interfaz operativa actual debe tratar el bot como PAPER. La ejecución real contra un broker requiere una etapa separada, controles de riesgo, auditoría e interruptor de emergencia. Los pagos no están habilitados por esta documentación.

## Arranque de la Web App

Desde la raíz del repositorio:

```bash
pip install -r webapp/requirements.txt
uvicorn webapp.server:app --host 0.0.0.0 --port 8000
```

Configura `TRADESCANNER_SESSION_SECRET` antes de iniciar. En producción, usa HTTPS y revisa las variables secretas requeridas por cada servicio.
