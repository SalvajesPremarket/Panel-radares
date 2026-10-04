# Account API

Endpoints previstos:

GET /account/me
Devuelve el resumen seguro de la cuenta autenticada.

GET /account/access
Indica si la cuenta puede utilizar scanner y bot.

GET /account/recommendations
Lista las recomendaciones propias.

POST /account/recommendations
Crea una recomendación.

Campos de recomendación:
- category
- title
- description
- symbol opcional
- context opcional

El backend asigna user_id, timestamp, estado y recommendation_id.

La API nunca devuelve secretos del motor.
