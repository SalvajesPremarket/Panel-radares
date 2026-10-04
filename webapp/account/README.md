# TradeScanner Account

## Objetivo

Panel privado para que cada usuario pueda consultar su cuenta sin acceder a secretos internos del sistema.

## Información visible

- Nombre.
- Email.
- Estado de cuenta.
- Fecha de inicio del trial.
- Fecha de finalización del trial.
- Días restantes.
- Plan contratado.
- Estado de suscripción.
- Estado de conexión del scanner.
- Estado del bot.
- Recomendaciones enviadas.

## Nunca visible al usuario normal

- FMP API key.
- Alpaca secret.
- Telegram token.
- Schwab secrets.
- Variables internas del motor.
- Credenciales de otros usuarios.
- Herramientas administrativas.

## Estados

trial → active_monthly / active_annual → expired

admin puede conceder acceso manual sin alterar la propiedad del usuario.

## Regla

El navegador recibe únicamente datos preparados para la cuenta. La autorización se decide en backend.
