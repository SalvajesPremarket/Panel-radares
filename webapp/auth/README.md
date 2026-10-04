# TradeScanner Auth

## Cuenta
Campos mínimos previstos: user_id, email, password_hash, display_name, role, account_status, trial_started_at, trial_ends_at, subscription_plan, subscription_status, created_at, last_login_at.

## Roles
### user
Acceso únicamente a las funciones contratadas.

### admin
Administrador general. Puede administrar usuarios, permisos, accesos gratuitos y configuración comercial.

Las funciones internas del motor, claves de APIs y secretos no se mostrarán al usuario normal.

## Prueba de 1 mes
Cada cuenta nueva puede recibir una única prueba de **1 mes**.

Durante el trial el usuario tendrá acceso al producto para evaluar el scanner, probar el flujo del bot y aportar recomendaciones para mejorar filtros, resultados y experiencia.

Al terminar la prueba:
- sin pago: acceso comercial bloqueado;
- con suscripción activa: acceso continúa;
- admin puede conceder acceso manual a una cuenta.

## Sesión
La sesión deberá sobrevivir a los cambios normales de pantalla y a los refresh del scanner. No debe utilizarse navegación que cree iframes o ventanas Streamlit anidadas.

## Próxima implementación
1. Backend de autenticación.
2. Hash seguro de contraseñas.
3. Cookies/sesiones seguras.
4. Verificación de correo.
5. Recuperación de contraseña.
6. Middleware de permisos.
7. Integración con billing.
