# TradeScanner Web Platform

Esta carpeta contiene la arquitectura comercial que se construirá separada del motor actual.

## Principio principal
**No modificar ni mover `app.py` para implementar cuentas, pagos o el bot.**

El motor Streamlit existente seguirá siendo el núcleo del scanner mientras construimos alrededor de él servicios independientes.

## Módulos
- `landing/`: página pública y comercial.
- `auth/`: registro, inicio de sesión, sesiones y recuperación.
- `billing/`: planes, prueba de 1 mes, suscripciones y estado de pago.
- `api/`: API privada para comunicar la plataforma con el scanner y el bot.
- `bot/`: estrategia, gestión de riesgo y conexión con brokers.
- `database/`: usuarios, suscripciones, permisos, configuraciones y auditoría.
- `product/`: roadmap y documentación del producto.

## Estados de cuenta previstos
- `trial`: prueba de 1 mes.
- `active_monthly`: suscripción mensual.
- `active_annual`: suscripción anual.
- `expired`: acceso vencido.
- `suspended`: cuenta suspendida.
- `admin`: administrador general.

## Programa de mejora durante el trial
El primer mes será también una etapa de validación. Las observaciones del usuario se recopilarán y clasificarán en scanner, bot, UX, datos, rendimiento y seguridad. Antes de tocar el motor se documentará la propuesta y su impacto.

## Seguridad
Las claves de FMP, Alpaca, Telegram, Schwab y otros brokers no deben enviarse al navegador ni quedar dentro del código público de la landing.

Las credenciales de broker del usuario deberán almacenarse de forma segura y utilizarse únicamente desde el backend autorizado.

## Flujo
Usuario -> Registro -> Trial de 1 mes/Pago -> Cuenta -> Scanner -> Señal API -> Bot -> Gestión de riesgo -> Broker

La ejecución real con dinero deberá requerir autorización explícita, controles de riesgo, registro de operaciones y un kill switch.

## Siguiente construcción
1. Backend real de autenticación.
2. Modelo de usuario y sesión.
3. Middleware de acceso por estado de cuenta.
4. Panel de cuenta.
5. Billing.
6. API privada de señales.
7. Bot en paper trading.


## Arranque de la Web App

Desde la raíz del repositorio:

```bash
uvicorn webapp.server:app --host 0.0.0.0 --port 8000
```

La aplicación unificada sirve Landing, Registro, Login, Cuenta, autenticación, permisos y API privada bajo el mismo origen. Requiere `TRADESCANNER_SESSION_SECRET` y, en producción, HTTPS para las cookies seguras.
