# ADR-001 — Arquitetura de streaming, hot store e raw truth

**Status:** ACEITO  
**Data:** 2026-09-29  
**Escopo:** ingestão cripto em tempo real, analytics e retenção de grande volume.

## Decisão

Adotar a arquitetura híbrida:

```text
Binance/Coinbase WebSocket
        |
        v
      Kafka  (buffer/barramento)
        |
        v
      Flink
      /   \
     /     \
    v       v
Iceberg   Kafka cleaned
raw truth      |
    |          v
Garage S3   ClickHouse
Lakekeeper      |
    |           +--> monitor-api :8081
    |           +--> analytics-api :8082
    |           +--> Grafana
    |
    +--> reconciler stream x batch
    +--> replay-api :8084
```

ClickHouse **não** é a única cópia histórica. Iceberg é a fonte de verdade bruta e ClickHouse é o serving tier/hot store.

## Alternativa A — WebSocket → Kafka → ClickHouse → Grafana

### Vantagens

- menor número de serviços;
- menor consumo de memória;
- baixa latência;
- implantação mais simples;
- bom desenho para retenção curta e volumes moderados.

### Limitações para este projeto

- o banco hot vira também arquivo histórico;
- bugs de parser/normalização contaminam a principal cópia consultável;
- replay completo depende do histórico bruto ainda existir no ClickHouse;
- mudanças de schema exigem migrações grandes ou reingestão externa;
- manter L2/raw JSON por anos aumenta merges, disco e custo de operação;
- reconstruir ClickHouse após erro lógico fica mais difícil.

## Alternativa B — Kafka → Flink → ClickHouse + Iceberg

### Vantagens

- raw truth independente do serving tier;
- reprocessamento e replay determinísticos;
- ClickHouse pode usar TTL agressivo;
- Parquet/Iceberg é adequado a histórico bruto de longo prazo;
- evolução do schema hot não destrói dados originais;
- reconciliação independente detecta perdas/duplicações;
- permite substituir ClickHouse no futuro sem perder o feed histórico.

### Custos

- mais RAM/CPU;
- mais failure domains;
- checkpoints e manutenção Iceberg precisam ser monitorados;
- catálogo e object store precisam ser operados corretamente.

## Consistência escolhida

### Raw truth

O sink Iceberg fica associado aos checkpoints do Flink em `EXACTLY_ONCE`.

### Hot path

A saída `market.cleaned` usa `at-least-once` para evitar que a visibilidade no monitor dependa da conclusão de cada checkpoint.

A duplicação temporária é controlada por:

- `event_id` determinístico;
- `ReplacingMergeTree` no ClickHouse;
- consultas sensíveis com `FINAL`;
- reconciliação periódica contra Iceberg.

Esse desenho prioriza baixa latência no hot path sem reduzir a integridade do raw truth.

## Kafka não é arquivo histórico

Retenções padrão:

```text
market.raw       24 h
market.cleaned   72 h
```

Kafka existe para absorver desacoplamento e falhas temporárias. O histórico durável permanece no Iceberg.

## ClickHouse

Papel:

- consultas de baixa latência;
- monitoramento;
- Grafana;
- candles materializados;
- microestrutura derivada;
- alertas;
- resultados de reconciliação.

O hot store não retém `raw_payload`, `bids_json` nem `asks_json`.

## Iceberg

Papel:

- raw event envelope;
- payload integral da exchange;
- L2 snapshot/delta bruto;
- timestamps/sequence IDs;
- base para replay e recomputação batch.

Formato padrão:

```text
Parquet + Zstd
partition: event_date, venue
target data file: 256 MiB
```

## Object storage: Garage vs SeaweedFS

A entrega usa **Garage v2.4.1**.

Motivos:

1. S3-compatible e orientado a self-hosting;
2. deployment Docker versionado documentado;
3. leve para single-host e migrável para múltiplos nós;
4. separa metadata e data directories, permitindo SSD para metadata e storage de capacidade para objetos;
5. evita adotar SeaweedFS 4.47 como raw truth enquanto existe histórico recente de bug S3/tombstone associado a essa release.

Referências:

- Garage: https://github.com/deuxfleurs-org/garage
- Garage deployment: https://github.com/deuxfleurs-org/garage/blob/main-v2/doc/book/cookbook/real-world.md
- SeaweedFS issue 11366: https://github.com/seaweedfs/seaweedfs/issues/11366

## Catálogo Iceberg

Lakekeeper v0.13.6 + PostgreSQL persistente.

O object store não é usado como catálogo. Lakekeeper permanece autoritativo para namespaces/tabelas e metadados Iceberg.

Referências:

- https://docs.lakekeeper.io/getting-started/
- https://docs.lakekeeper.io/docs/latest/storage/

## Small files e manutenção

Flink/Iceberg executa:

- `RewriteDataFiles`;
- `ExpireSnapshots`;
- `DeleteOrphanFiles`.

Isso reduz small files e impede crescimento ilimitado de metadata sem introduzir Spark apenas para manutenção.

Referência: https://iceberg.apache.org/docs/latest/flink-maintenance/

## Reconciliação stream × batch

A reconciliação é obrigatória e não destrutiva.

Fluxo:

```text
Iceberg raw truth -> Arrow batches -> SQLite staging em disco
                                  \
                                   compare -> ClickHouse events_hot FINAL
```

Compara por `venue/symbol/event_type`:

- rows raw;
- unique `event_id` raw;
- quantity;
- notional;
- rows hot;
- unique `event_id` hot.

Mismatch é registrado no ClickHouse e Kafka. Reparos não são executados automaticamente.

## Replay determinístico

Iceberg é lido em ordem:

```text
event_time_ms, ingest_time_ms, event_id, sequence_id
```

O replay publica em tópicos próprios:

```text
market.replay
market.replay.microstructure
```

Isso permite reproduzir bugs/alterações de OFI/microprice/book sem contaminar produção.

## Código e dados no host

Código:

```text
${SRC_DIR} -> /opt/app/src:ro
```

Dados:

```text
KAFKA_DATA_DIR
CLICKHOUSE_DATA_DIR
GARAGE_META_DIR
GARAGE_DATA_DIR
LAKEKEEPER_PG_DATA_DIR
FLINK_CHECKPOINT_DIR
FLINK_SAVEPOINT_DIR
GRAFANA_DATA_DIR
RECONCILE_DATA_DIR
REPLAY_DATA_DIR
```

Mudanças em `src/` não exigem rebuild de imagem. Workers são reiniciados com `make reload`; APIs podem usar Uvicorn reload. Rebuild só é necessário quando Dockerfile ou dependências mudam.

## Consequência operacional

Em single-host, a stack é tolerante a reinício de processos, mas **não é HA física**. Para HA real devem ser distribuídos Kafka, Flink, ClickHouse, Postgres/Lakekeeper e Garage entre hosts distintos.
