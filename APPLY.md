# Apply v4.3.7

```bash
unzip -o crypto-monitor-v4.3.7-alerts-reconcile-fix.zip -d .
./scripts/apply-v4.3.7.sh
docker compose restart telegram-bot reconciler
```

Test:

```bash
docker compose logs --tail=100 telegram-bot reconciler
curl -s http://localhost:8081/v1/reconciliation/latest?limit=20 | jq
```
