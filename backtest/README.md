# Backtests de la estrategia Bollinger

Este directorio reúne las utilidades necesarias para ejecutar backtests de la estrategia de Bandas de Bollinger contra datos de Binance USD‑M Futures. Los flujos cubren dos perfiles de salida:

- **TR**: backtest corto para trading intradía (carpeta `backtestTR`).
- **Histórico**: corridas extensas para análisis de largo plazo (carpeta `backtestHistorico`).

Ambos perfiles comparten el mismo motor (`run_backtest.py`) y las rutas/alineaciones de archivos se controlan desde `config.py` y variables de entorno.

## Requisitos previos

- Python 3.11+ recomendado (el proyecto usa pandas, numpy, plotly, etc.).
- Dependencias instaladas: `pip install -r requirements.txt`.
- Variables de entorno cargadas (por ejemplo `source .venv/bin/activate && set -a && source .env && set +a`) para que `velas.py`, `trade_logger.py` y los backtests reciban el símbolo, intervalos, credenciales y rutas de salida.
- Acceso HTTPS al endpoint de Binance (`BINANCE_UM_BASE_URL`, por defecto `https://fapi.binance.com`).

## Archivos clave

- `run_backtest.py`: motor que descarga velas, genera señales Bollinger, simula entradas/salidas y guarda resultados.
- `build_dashboard.py`: genera un dashboard HTML con métricas, gráfico y detalle de operaciones.
- `config.py`: define perfiles `tr` y `historico`, junto con las rutas de CSV/PNG/HTML (pueden sobrescribirse vía variables como `STRAT_BACKTEST_TRADES_PATH`).
- `backtestTR/` y `backtestHistorico/`: carpetas destino para cada perfil (CSV de trades, gráfico y dashboard).

## Ejecutar el backtest

1. **Activar entorno** (si aplica):
   ```bash
   source .venv/bin/activate
   set -a && source .env && set +a  # opcional pero recomendado
   ```

2. **Lanzar el backtest**:

   - Perfil TR (últimas semanas, salidas en `backtest/backtestTR/`):
     ```bash
     python backtest/run_backtest.py --profile tr --weeks 2
     ```
     Ajustá `--weeks` según la ventana que quieras analizar (si lo omitís, usa `BACKTEST_STREAM_BARS`).

   - Perfil Histórico (meses de datos, salidas en `backtest/backtestHistorico/`):
     ```bash
     python backtest/run_backtest.py --profile historico --months 6
     ```
     También podés fijar fechas exactas:
     ```bash
     python backtest/run_backtest.py --profile historico --start 2024-01-01T00:00:00Z --end 2024-06-30T23:59:59Z
     ```

   Durante la ejecución se imprime la comisión estimada en Binance, el rango temporal efectivo y un resumen con métricas (trades totales, win rate, PnL, drawdown, fees).

### Parámetros útiles

`run_backtest.py` acepta varias banderas para ajustar la corrida:

- `--stream-bars`: cantidad base de velas a descargar (default `BACKTEST_STREAM_BARS`).
- `--profile {tr,historico}`: selecciona el preset de rutas definido en `config.py`.
- `--trades-out / --plot-out`: rutas de salida personalizadas para CSV/PNG.
- `--weeks` o `--months`: rango relativo hacia atrás (usar solo uno).
- `--start / --end`: fechas ISO8601 (UTC) para rango absoluto.
- `--show`: abre la figura de Matplotlib al terminar si `matplotlib` está disponible.

> Nota: el motor usa las mismas Bandas de Bollinger configuradas para el watcher (`BB_LENGTH`, `BB_MULT`, `BB_DIRECTION`, `STREAM_INTERVAL`, etc.), por lo que cualquier cambio en `.env` impactará tanto las señales en vivo como el backtest.

## Visualización y dashboards

Una vez generado el CSV de trades podés construir el dashboard interactivo:

```bash
python backtest/build_dashboard.py --profile tr --price alerts_stream.csv
```

- `--profile` funciona igual que en el backtest; si no lo indicás usa el valor por defecto (`BACKTEST_PROFILE` o `tr`).
- `--trades` permite elegir un CSV alternativo (por ejemplo una corrida histórica guardada en otro directorio).
- `--price` es opcional, pero al pasar el CSV de precios (`alerts_stream.csv` u otro con columnas `Timestamp` y `Close`) se superpone la curva de precios con las entradas/salidas.
- `--html` define el destino del dashboard (default según preset).
- `--show` abre automáticamente el HTML en el navegador.

Los dashboards incluyen resumen estadístico, PnL acumulado, histograma de rendimiento y tablas con los últimos trades/operaciones. El archivo resultante se guarda en `backtest/backtestTR/dashboard.html` o `backtest/backtestHistorico/dashboard.html` según el perfil elegido.

### Listener para minuto exacto de fills

Si querés capturar el minuto exacto en que se ejecuta una orden pendiente, corré el listener dedicado (opera sobre el mismo `realtime_state.json` que usa el backtest en vivo):

```bash
python backtest/order_fill_listener.py --profile tr
```

- Monitorea las órdenes con `status=pending` y consulta velas de 1 minuto para detectar el primer cruce del precio objetivo.
- Actualiza el estado a `open` con el timestamp de esa vela (UTC) y mantiene la misma lógica de SL/TP definida por la estrategia.
- Parámetros opcionales:
  - `--poll-seconds` (default 15) ajusta la frecuencia de consulta.
  - `--tolerance` permite sumar una tolerancia absoluta al match del precio.
  - `--lookback-minutes` define la ventana de búsqueda al reconstruir la vela que ejecutó la orden.

Mantenelo corriendo junto al watcher de señales si necesitás una simulación intradía con precisión de minuto.

### Heartbeat / Verificador de vida

Para recibir un aviso cada 3 horas (o la frecuencia que definas) indicando si los procesos críticos siguen activos, podés usar el heartbeat incluido en la raíz del repo:

```bash
python heartbeat_monitor.py --interval-hours 3
```

- Por defecto chequea que estén corriendo `python watcher_alertas.py`, `python backtest/order_fill_listener.py` y `python estrategiaBollinger.py`. Podés ajustar la lista con la variable `HEARTBEAT_PROCESSES`, usando `;` o `,` como separador.
- El heartbeat reutiliza el mismo bot/configuración de Telegram (variables `TELEGRAM_BOT_TOKEN` y `TELEGRAM_CHAT_IDS`). A cada intervalo envía un mensaje con el resumen de estado.
- Para pruebas puntuales, agregá `--once` y solo mandará una notificación.

También podés lanzar el listener de comandos para responder manualmente en cualquier momento con `/estavivo` y obtener el mismo estado bajo demanda:

```bash
python telegram_bot_commands.py
```

- Reconoce `/start`, `/help`, `/estavivo`, `/ultimaalerta`, `/ultimatrade` y `/resumen`. Los tres últimos permiten consultar rápidamente la última señal, el último trade y un resumen de métricas del CSV configurado.
- El comando responde únicamente a los chats listados en `TELEGRAM_CHAT_IDS` (si está vacío, acepta a todos).

## Trading en exchanges (estructura preliminar)

El paquete `trading/` incorpora la base para ejecutar órdenes reales o simuladas, contemplando múltiples exchanges y usuarios:

- `trading/orders/models.py`: definiciones comunes (`OrderRequest`, `OrderResponse`, enums `OrderSide/OrderType/TimeInForce`).
- `trading/accounts/models.py`: descripciones de cuentas (`AccountConfig`, `ExchangeCredential`, ambientes testnet/live).
- `trading/accounts/manager.py`: carga configuraciones desde YAML/JSON y resuelve credenciales leyendo variables de entorno.
- `trading/exchanges/base.py`: interfaz `ExchangeClient` + `ExchangeRegistry` para registrar implementaciones por exchange.
- `trading/exchanges/binance.py`: cliente Binance en modo dry-run/testnet (no envía órdenes reales todavía).
- `trading/orders/executor.py`: orquestador que toma una señal genérica y la envía al exchange adecuado.

### Archivo de cuentas

Ejemplo (`trading/accounts/sample_accounts.yaml`):

```yaml
users:
  - id: diego
    label: Cuenta Diego
    exchanges:
      binance:
        api_key_env: DIEGO_BINANCE_API_KEY
        api_secret_env: DIEGO_BINANCE_API_SECRET
        environment: testnet
  - id: sofia
    label: Cuenta Sofia
    exchanges:
      binance:
        api_key_env: SOFIA_BINANCE_API_KEY
        api_secret_env: SOFIA_BINANCE_API_SECRET
        environment: live
```

Las variables de entorno `DIEGO_BINANCE_API_KEY`, etc., deben estar configuradas en el server (idealmente gestionadas como secretos).

### Uso básico en modo dry-run

```python
from trading.accounts.manager import AccountManager
from trading.orders.executor import OrderExecutor
from trading.orders.models import OrderRequest, OrderSide, OrderType

manager = AccountManager.from_file("accounts.yaml")
executor = OrderExecutor(manager)

order = OrderRequest(
    symbol="ETHUSDT",
    side=OrderSide.BUY,
    type=OrderType.MARKET,
    quantity=0.1,
)

response = executor.execute("diego", "binance", order, dry_run=True)
print(response.status, response.raw)
```

Mientras `dry_run=True` (o la cuenta esté marcada como `testnet`), no se envía la orden a Binance; se devuelve una respuesta simulada. Más adelante se agregará la llamada real a la API, controles de riesgo y manejo de posiciones.

### Configuración de cuentas y secretos

- Cada usuario/exchange hace referencia a variables de entorno (`api_key_env`, `api_secret_env`). En OCI podés declararlas en tu profile, usar un secret manager o exportarlas en el servicio (ej. `export DIEGO_BINANCE_API_KEY=...`).
- Validá rápidamente que todas las cuentas tengan sus claves disponibles:

  ```bash
  python scripts/validate_accounts.py --accounts trading/accounts/sample_accounts.yaml --verbose
  ```

  El script reporta las variables faltantes para que puedas cargarlas antes de habilitar la ejecución real.
- Recordá excluir `accounts.yaml` y `.env` con datos sensibles de tu repo público; mantenelos en el server (o en Vault) y solo referencialos desde las variables de entorno.

## Consejos y buenas prácticas

- Confirmá que `alerts_stream.csv` esté poblado si querés overlay de precios en el dashboard; el watcher `watcher_alertas.py` lo genera automáticamente.
- Para corridas históricas largas, aumentar `PAGINATE_PAGE_LIMIT` y `PAGE_SLEEP_SEC` puede acelerar las descargas sin exceder límites de Binance.
- Si necesitás replicar los resultados en otro equipo, copiá el `.env` (sin credenciales sensibles) y las carpetas `backtestTR/` / `backtestHistorico/`.
- El script maneja comisiones usando el `takerCommissionRate` que expone Binance; si falla la consulta, aplica el fallback 0.0005. Podés forzar una tarifa fija exportando `STRAT_FEE_RATE` antes de ejecutar el backtest.

Con estos pasos deberías poder generar y analizar tanto corridas recientes (TR) como estudios históricos completos de la estrategia.
