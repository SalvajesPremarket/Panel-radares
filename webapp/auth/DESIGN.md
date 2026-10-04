# Authentication Design

## Objetivo
Implementar autenticación independiente del motor Streamlit.

## Flujo de registro
1. Usuario introduce nombre, email y contraseña.
2. Backend valida formato y unicidad del email.
3. Contraseña se almacena únicamente como hash seguro.
4. Se crea la cuenta con estado `trial`.
5. `trial_started_at` se establece al crear la cuenta.
6. `trial_ends_at` se calcula a un mes de calendario.
7. Se crea una sesión segura.
8. El usuario entra al panel sin tener que volver a registrarse.

## Flujo de acceso
1. Usuario introduce email y contraseña.
2. Backend verifica el hash.
3. Se crea una sesión nueva.
4. El navegador recibe únicamente una cookie de sesión segura.
5. El backend determina qué funciones puede utilizar según rol y estado.

## Estados
- `trial`: acceso durante el período de prueba.
- `active_monthly`: suscripción mensual activa.
- `active_annual`: suscripción anual activa.
- `expired`: prueba o suscripción vencida.
- `suspended`: acceso bloqueado por administración.
- `admin`: administrador general.

## Seguridad
- Nunca guardar contraseñas en texto plano.
- Nunca enviar hashes, claves de API o secretos al navegador.
- Cookies de sesión con Secure, HttpOnly y SameSite apropiado.
- Expiración y rotación de sesiones.
- Protección contra intentos repetidos de acceso.
- Auditoría de login, logout y cambios administrativos.

## Compatibilidad con scanner
La autenticación no debe controlar el refresh interno del scanner mediante navegación del navegador. El estado de cuenta se consulta desde el backend.

## Trial de 1 mes
El vencimiento se calcula en servidor. El cliente nunca decide si una cuenta sigue dentro del trial.
