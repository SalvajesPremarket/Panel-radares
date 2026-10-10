# Integración Cuenta → Scanner → API → Bot

Esta capa rodea el motor existente. El punto de entrada Streamlit sigue siendo `app.py`; el adaptador no debe cambiar filtros ni fórmulas del scanner.

## Flujo previsto

usuario autenticado → permisos → señales normalizadas → API privada → bot → controles de riesgo → paper trading

## Servicios existentes

- `webapp/auth/server.py`: identidad y sesión.
- `webapp/account/server.py`: cuenta, acceso y recomendaciones.
- `webapp/api/signal_service.py`: contrato y almacenamiento temporal de señales.
- `webapp/api/server.py`: API privada de señales.
- `webapp/api/scanner_adapter.py`: adaptador preparado para publicar resultados de `ServicioScanner`; no se encontró una llamada desde `app.py` en esta auditoría.
- `BotTradeScanner/api/server.py`: endpoints de estado y decisiones del bot.
- `BotTradeScanner/riesgo/paper.py`: evaluación de señales y controles de riesgo en modo paper.

## Estado y límites

1. La capa de cuenta y los servicios de API están separados del punto de entrada Streamlit.
2. El servicio de señales ofrece un contrato común para la integración.
3. El bot evalúa señales en modo paper.
4. La ejecución real contra un broker no está implementada.
5. No conectar el adaptador automáticamente ni alterar filtros, fórmulas o el motor sin un cambio explícito y pruebas.
6. `app.py` permanece como punto de entrada principal.

## Próximos pasos

Primero probar la publicación de señales con datos controlados y verificar autenticación, permisos e idempotencia. Solo después integrar el adaptador explícitamente. La ejecución real requeriría una etapa separada y autorización explícita.

Las recomendaciones de usuarios nunca modifican automáticamente el motor.
