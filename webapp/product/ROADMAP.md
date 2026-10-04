# TradeScanner — Roadmap de construcción

## Objetivo
Convertir TradeScanner en una plataforma comercial donde el usuario pueda conocer el producto sin registrarse, crear una cuenta, utilizar **1 mes completo de prueba**, aportar recomendaciones para mejorar scanner y bot, y después contratar $28/mes o $270/año.

## Etapas
### Fase 1 — Producto comercial
- Landing pública.
- Planes y precios.
- Trial de 1 mes.
- Documentación.
- Registro e inicio de sesión.
- Persistencia de sesión.

### Fase 2 — Cuenta y facturación
- Perfil.
- Estado de cuenta.
- Fecha de inicio y final del trial.
- Suscripción mensual/anual.
- Renovación y cancelación.
- Panel administrativo.

### Fase 3 — API del scanner
- Endpoint privado de señales.
- Estado del motor.
- Resultados normalizados.
- Timestamp e identificador único de señal.
- Autenticación y permisos.

### Fase 4 — Bot
- Recepción de señales.
- Filtros configurables.
- Gestión de riesgo.
- Paper trading.
- Registro de órdenes.
- Stop loss / take profit.
- Kill switch.
- Conexión con broker.

### Fase 5 — Mejora continua
El mes de prueba será también una etapa de validación del producto. Las recomendaciones del usuario se documentarán como cambios de producto antes de modificar el motor.

**Regla:** las mejoras al scanner se harán de forma aislada y verificable. `app.py` no se modifica como parte del desarrollo web.

## Seguridad
Nunca exponer claves FMP, Alpaca, Telegram, Schwab u otros secretos al navegador o al código público.