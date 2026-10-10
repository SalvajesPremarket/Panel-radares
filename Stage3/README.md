# Stage 3 — Arquitectura e integración

Esta carpeta conserva el contrato de arquitectura; no es una segunda aplicación y no debe duplicar código ejecutable.

## Componentes activos

1. **TradeScanner**: la aplicación Streamlit cuyo punto de entrada único es `/app.py`.
2. **Motor de mercado compartido**: entrega datos de mercado al motor de velas.
3. **BotTradeScanner**: estrategias LONG y SHORT en modo Paper, con controles de riesgo.
4. **webapp**: interfaz web, autenticación, cuentas, API privada de señales y TradeBot.
5. **PostgreSQL**: persistencia de cuentas, sesiones, señales y estado del simulador Paper manual.

## Despliegue propio

La configuración preparada está concentrada en `deploy/self-host/`:

- Docker Compose coordina web, scanner, PostgreSQL y proxy HTTPS.
- Los puertos internos de aplicación y la base de datos no se publican directamente.
- Caddy termina HTTPS para los subdominios del panel y del scanner.
- El runtime automático de TradeBot permanece desactivado por defecto; la ejecución real no forma parte del despliegue.

## Reglas que no deben romperse

- Mantener `app.py` como punto de entrada único de Streamlit.
- No cambiar filtros ni fórmulas de TradeScanner desde la capa web.
- Usar contratos explícitos entre Scanner, API y Bot.
- No crear conexiones de mercado independientes por cada usuario si puede compartirse el stream.
- Mantener las operaciones en Paper hasta completar pruebas de extremo a extremo y recibir autorización expresa para cualquier cambio de modalidad.

## Validaciones pendientes antes de darlo por listo

- Confirmar scanner → publicación → API → estrategia Paper con datos reales de mercado, sin órdenes reales.
- Probar recuperación de señales y del estado Paper manual tras reinicios.
- Definir recuperación segura para el estado del runtime automático y su posición Paper antes de depender de una operación continua.
- Probar copia de seguridad y restauración de PostgreSQL.
- Confirmar autenticación, permisos de usuarios, HTTPS, secretos y límites de recursos en el VPS.
- No eliminar módulos antiguos solo por su nombre: verificar primero que no existan importaciones, rutas, workflows o instrucciones activas que dependan de ellos.
