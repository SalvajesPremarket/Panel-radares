# TradeScanner Billing — No habilitado

## Estado actual

**La facturación está desactivada. No se cobran suscripciones ni se procesan pagos.**

Los importes y planes mencionados en este documento son únicamente una propuesta de producto para evaluar más adelante; no constituyen una oferta activa ni deben mostrarse como planes contratables mientras no exista autorización explícita para activar cobros.

| Plan propuesto | Precio de referencia | Estado |
|---|---:|---|
| Prueba | $0 por 1 mes | No automatizada por billing |
| Mensual | $28/mes | Desactivado |
| Anual | $270/año | Desactivado |

No integrar pasarela de pagos, webhooks, renovación automática ni bloqueo por falta de pago como parte de la limpieza actual. Mantener accesos y comportamiento existentes sin cambios comerciales hasta que se autorice expresamente esa implementación.

## Diseño futuro, pendiente de aprobación

Si se autoriza el desarrollo de billing en el futuro, el backend deberá conservar:
- fecha de inicio y vencimiento de la prueba;
- si la prueba ya fue utilizada;
- estado de cuenta y suscripción;
- eventos de facturación y acciones administrativas.

El administrador podrá necesitar controles para conceder acceso gratuito, suspender cuentas y revisar el estado de suscripción. Ninguna de estas funciones se considera activa por el hecho de estar documentada.

## Seguridad requerida para una futura integración

La confirmación de un pago deberá proceder del backend/proveedor mediante un mecanismo verificable. El navegador nunca podrá marcar una cuenta como pagada por sí mismo. Las claves y secretos del proveedor deben mantenerse fuera del repositorio y del navegador.

## Regla de cambio

La activación de cobros requiere una solicitud y aprobación explícitas, implementación separada y pruebas propias. Hasta entonces, este documento es solo una nota de diseño y no debe interpretarse como evidencia de que billing está implementado.
