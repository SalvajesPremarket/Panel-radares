# Access Rules

## Trial
Una cuenta con estado `trial` puede utilizar las funciones incluidas durante el período vigente.

El vencimiento se calcula y verifica en servidor.

## Suscripción
`active_monthly` y `active_annual` permiten las funciones contratadas mientras el estado de pago sea válido.

## Expired
Una cuenta `expired` conserva acceso a su cuenta básica, pero no a las funciones comerciales protegidas.

## Suspended
Una cuenta `suspended` no puede utilizar scanner, bot ni funciones comerciales.

## Admin
El administrador general puede consultar y modificar permisos administrativos definidos por la plataforma.

## Principio de mínimo privilegio
El frontend nunca determina permisos. Cada endpoint protegido debe comprobar sesión, estado de cuenta y capacidad solicitada en backend.
