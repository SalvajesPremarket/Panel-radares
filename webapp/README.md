# TradeScanner Web Platform

Esta carpeta contiene la arquitectura comercial que se construirá separada del motor actual.

## Principio principal

**No modificar ni mover `app.py` para implementar cuentas, pagos o el bot.**

El motor Streamlit existente seguirá siendo el núcleo del scanner mientras construimos alrededor de él servicios independientes.

## Módulos

- `landing/`: página pública y comercial.
- `auth/`: registro, inicio de sesión, sesiones y recuperación.
- `billing/`: planes, prueba de 7 días, suscripciones y estado de pago.
- `api/`: API privada para comunicar la plataforma con el scanner y el bot.
- `bot/`: estrategia, gestión de riesgo y conexión con brokers.
- `database/`: usuarios, suscripciones, permisos, configuraciones y auditoría.

## Estados de cuenta previstos

- `trial`: prueba de 7 días.
- `active_monthly`: suscripción mensual.
- `active_annual`: suscripción anual.
- `expired`: acceso vencido.
- `suspended`: cuenta suspendida.
- `admin`: administrador general.

## Seguridad

Las claves de FMP, Alpaca, Telegram, Schwab y otros brokers no deben enviarse al navegador ni quedar dentro del código público de la landing.

Las credenciales de broker del usuario deberán almacenarse de forma segura y utilizarse únicamente desde el backend autorizado.

## Flujo

Usuario -> Registro -> Trial/Pago -> Cuenta -> Scanner -> Señal API -> Bot -> Gestión de riesgo -> Broker

La ejecución real con dinero deberá requerir autorización explícita, controles de riesgo, registro de operaciones y un kill switch.
