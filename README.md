# bot6rangos

Bot de trading basado en Range3 + Bollinger para ETHUSDT perpetual.

Este repositorio fue creado como un clon operativo de la estructura de `bot`,
pero con una estrategia nueva:

- velas cerradas de 30m,
- rango de 3 canales 25/50/25 sobre 200 velas,
- señales Bollinger por cruce de cierre,
- pendientes manejadas por el watcher,
- SL inicial 2%,
- trailing por umbrales de 1%,
- profit-lock al +3% con SL +0.5%,
- flip post-SL con TP 2% y SL 2%.

Por defecto no hay usuarios ni exchanges activos. Antes de operar hay que crear
`.env`, cargar credenciales reales, habilitar cuentas en
`trading/accounts/oci_accounts.yaml` y activar los servicios systemd.
