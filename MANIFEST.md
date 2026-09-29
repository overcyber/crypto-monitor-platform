# Manifest — Crypto Monitor Platform v4.3.4

Componentes principais:

- Binance/Coinbase WebSocket ingestors
- Kafka KRaft
- Flink SQL streaming
- Iceberg raw truth + Lakekeeper + Garage
- ClickHouse hot serving tier
- candle-worker
- book-worker / microstructure
- alert-worker
- monitor-api :8081
- analytics-api :8082
- Flink UI :8083
- replay-api :8084
- Grafana
- Telegram bot
- Web UI :8090 com avaliação + configuração
- reconciler stream × batch
- deterministic replay

Configurações editáveis pela Web UI:

- `config/markets.yaml`
- `config/alerts.yaml`
- `config/telegram.yaml`

Backups automáticos: `config/.web-ui-backups/`.
