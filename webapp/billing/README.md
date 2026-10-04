# TradeScanner Billing

## Planes actuales

| Plan | Precio | Acceso |
|---|---:|---|
| Trial | $0 | 1 mes |
| Mensual | $28/mes | Scanner + funciones contratadas |
| Anual | $270/año | Scanner + funciones contratadas |

## Regla del trial
Una cuenta nueva recibe un único período de prueba de un mes.

El backend debe conservar:
- fecha de inicio;
- fecha de vencimiento;
- si el trial ya fue utilizado;
- estado actual de la cuenta.

## Después del trial
Si no existe una suscripción activa, la cuenta pasa a `expired`.

El usuario conserva su cuenta y puede contratar posteriormente.

## Administración
El administrador general podrá:
- conceder acceso gratuito manual;
- suspender cuentas;
- consultar estado de suscripción;
- revisar fechas;
- registrar acciones administrativas.

## Próxima integración
El proveedor de pagos todavía no se fija en esta etapa. Billing se diseñará con una capa de abstracción para poder integrar posteriormente el proveedor elegido sin acoplarlo al scanner.

## Regla de seguridad
La confirmación de un pago debe venir del backend/proveedor mediante un mecanismo verificable. El navegador nunca debe poder marcar una cuenta como pagada por sí mismo.
