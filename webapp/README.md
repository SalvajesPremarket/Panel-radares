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

## Base de datos persistente en Render

La web usa PostgreSQL cuando la variable de entorno `DATABASE_URL` está configurada. SQLite queda reservado para desarrollo local. En Render, la aplicación ahora falla de forma explícita si falta `DATABASE_URL`, en lugar de guardar cuentas y sesiones en el disco efímero del servicio.

En el servicio Render `tradescanner-webapp`:

1. Abre **Environment** y comprueba que exista `DATABASE_URL`.
2. Debe apuntar a la base PostgreSQL de Render, preferiblemente mediante su **Internal Database URL** si ambos recursos están en la misma región.
3. No pegues la URL de conexión ni contraseñas en GitHub, archivos de código o mensajes públicos.
4. Comprueba el estado y la fecha de expiración de la base en el panel de Render. Las bases del plan gratuito pueden expirar; planifica una actualización o migración antes de la fecha indicada para no perder datos.

La base PostgreSQL debe estar activa y accesible antes de desplegar esta versión. No configures `DATABASE_URL` con una URL de ejemplo.

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
