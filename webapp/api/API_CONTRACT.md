# TradeScanner API — Contrato inicial

## Propósito
Separar el motor Streamlit del sitio comercial y del bot.

## Principios
- API privada.
- Autenticación obligatoria.
- Autorización por cuenta.
- No devolver secretos.
- Identificadores y timestamps en cada evento.
- Auditoría para acciones sensibles.

## Endpoints previstos
### GET /api/v1/health
Estado general del servicio.

### GET /api/v1/scanner/status
Estado permitido del motor para la cuenta.

### GET /api/v1/signals
Señales disponibles para el usuario autorizado.

Parámetros: since, timeframe, symbol, limit.

### GET /api/v1/signals/{id}
Detalle de una señal.

### POST /api/v1/bot/decisions
Registra la decisión del bot.

### POST /api/v1/bot/orders
Registra una orden generada por el bot.

## Autenticación
La implementación final utilizará sesiones o tokens seguros y expirables. Las claves de mercado y broker nunca forman parte de la respuesta pública.

## Compatibilidad
El contrato debe permitir cambiar la implementación interna del scanner sin romper el bot, siempre que se mantenga el esquema de señal.