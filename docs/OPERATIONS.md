# Runbook operacional

## Comandos principais

```bash
make preflight
make up
make status
make smoke
make storage
make logs
make reload
make reload-flink
make backup-catalog
make down
```

## Estado dos containers

```bash
./scripts/compose-safe.sh ps
```

## Logs por serviço

```bash
./scripts/compose-safe.sh logs -f ingestor-binance
./scripts/compose-safe.sh logs -f flink-job-supervisor
./scripts/compose-safe.sh logs -f book-worker
./scripts/compose-safe.sh logs -f reconciler
./scripts/compose-safe.sh logs -f clickhouse
```

## Kafka

Listar tópicos:

```bash
./scripts/compose-safe.sh exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --list
```

Consumer groups:

```bash
./scripts/compose-safe.sh exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 --all-groups --describe
```

## Flink

UI:

```text
http://127.0.0.1:8083
```

Observar:

- job RUNNING;
- checkpoint failures;
- duração de checkpoint;
- backpressure;
- restart count.

Após editar SQL:

```bash
make reload-flink
```

## ClickHouse

```bash
curl -u market_app:<senha> \
  --data-binary 'SELECT count() FROM market.events_hot' \
  http://127.0.0.1:8123/
```

Tabelas grandes:

```sql
SELECT
  database,
  table,
  formatReadableSize(sum(bytes_on_disk)) AS size,
  sum(rows) AS rows
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY sum(bytes_on_disk) DESC;
```

## Garage

Use `make storage` para visão de filesystem. Para adicionar discos, veja `STORAGE.md`.

## Reconciliação com mismatch

Ver `RECONCILIATION.md`. Não reinicie/apague dados antes de identificar se a diferença é:

- lag Kafka;
- checkpoint atrasado;
- consumer ClickHouse parado;
- evento duplicado;
- diferença de parser/schema;
- janela ainda não estabilizada.

## API degradada

`/health` retorna `degraded` se ClickHouse não estiver disponível. Isso não implica automaticamente falha de ingestão raw.

## Parada controlada

```bash
make down
```

Para manutenção de Flink mais sensível, gere savepoint manualmente pela UI/CLI antes de alterações de runtime/connector.

## Atualização de código

```bash
git pull   # ou edite os arquivos no host
make validate
make reload
```

Nenhum rebuild é necessário se requirements/Dockerfiles não mudaram.
