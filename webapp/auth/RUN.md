1. Instalar dependencias: pip install -r webapp/requirements.txt
2. Definir TRADESCANNER_SESSION_SECRET con un secreto largo y aleatorio.
3. Iniciar: uvicorn webapp.auth.server:app --host 127.0.0.1 --port 8001

Endpoints iniciales:
GET /health
POST /auth/register
POST /auth/login
POST /auth/logout
GET /auth/me

Esta es una base técnica, no un despliegue de producción. Antes de producción faltan HTTPS, verificación de correo, recuperación de contraseña, rate limiting, protección CSRF donde corresponda, gestión profesional de secretos y pruebas automatizadas.