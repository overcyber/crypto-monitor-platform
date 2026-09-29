# Aplicação v4.3.6

Na raiz do projeto:

```bash
unzip -o crypto-monitor-v4.3.6-history-timeout-fix.zip -d .
docker compose restart monitor-api web-ui
```

Teste:

```bash
curl -s 'http://localhost:8090/api/history/BTC?venue=binance&days=7' | jq '.[0:3]'
curl -s http://localhost:8081/v1/quotes | jq
```
