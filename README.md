# TradeScanner + TradeBot

Plataforma integrada de análisis bursátil y simulación Paper. **La ejecución de órdenes reales permanece deshabilitada.**

## Componentes oficiales

- `app.py`: único punto de entrada de Streamlit para TradeScanner.
- `TradeScanner/`: construcción de velas, datos, caché y stream compartido.
- `BotTradeScanner/`: motor de velas, reglas LONG/SHORT, riesgo y simulador Paper.
- `webapp/`: web, autenticación, API de señales y TradeBot.
- `deploy/self-host/`: Docker Compose para preparar el VPS propio (web, scanner, PostgreSQL y HTTPS).

No crear una segunda aplicación Streamlit ni duplicar reglas del scanner dentro de la web. La integración debe usar interfaces explícitas entre scanner, API y bot.

## Desarrollo y validación

Requisitos: Python 3.11.

```bash
python -m pip install -r requirements.txt
python -m pip install -r webapp/requirements.txt
PYTHONPATH=. pytest -q
```

GitHub Actions valida los tests seleccionados, compila `app.py`, valida Docker Compose y construye las imágenes del despliegue propio.

## Preparar el VPS (sin contratar recursos)

La configuración está en [deploy/self-host/README.md](deploy/self-host/README.md). La propuesta todavía no se ha desplegado en un VPS ni requiere comprar recursos ahora.

- PostgreSQL solo está expuesto en la red privada de Docker.
- Caddy publica HTTPS para la web y el scanner.
- `TRADESCANNER_TRADEBOT_AUTO_PAPER=false` por defecto.
- Los estados de las estrategias LONG y SHORT Paper, las señales y el simulador manual tienen persistencia preparada; se deben validar los reinicios y la ruta completa con datos de mercado antes de depender de operación continua.
- La copia de seguridad de PostgreSQL debe guardarse fuera del VPS y probarse mediante restauración.
- Nunca subir `.env`, claves Alpaca ni secretos reales al repositorio.

## Seguridad y operación

1. Mantener todas las operaciones en Paper durante el desarrollo y las pruebas.
2. No habilitar órdenes reales como parte del despliegue inicial.
3. No borrar componentes antiguos por su nombre: revisar antes imports, rutas, workflows, scripts y documentación.
4. No comprar VPS/dominio ni activar costes recurrentes sin aprobación expresa.

La meta es llegar a la fecha prevista del primer pago con el código revisado y el plan de despliegue probado, para que después queden principalmente validación final y configuración del servidor.
