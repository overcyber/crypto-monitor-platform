# Arquitetura v4

## 1. Decisão: ClickHouse-only vs Kafka + Flink + ClickHouse + Iceberg

### Opção A — WebSocket → Kafka → ClickHouse → Grafana

**Vantagens**

- menos serviços;
- menor consumo de RAM/CPU;
- menor número de failure domains;
- baixa latência entre feed e dashboard;
- operação simples em um host pequeno.

**Limitações para este projeto**

- ClickHouse precisa exercer simultaneamente os papéis de hot store e arquivo histórico;
- bugs de normalização podem contaminar a única cópia prática dos dados;
- alterações de schema/parsers são difíceis de reprocessar de forma reproduzível;
- recuperar uma partição hot perdida exige outra fonte de verdade;
- replay de L2 fica dependente de quanto histórico detalhado foi mantido no banco hot;
- retenção de raw JSON/L2 por anos aumenta muito custo de ClickHouse e merges.

### Opção B — Kafka → Flink → ClickHouse + Iceberg

**Custos**

- Flink, catálogo Iceberg e object store aumentam a operação;
- checkpoints e compactação precisam ser observados;
- existe uma camada de consistência a mais;
- requer mais RAM no host.

**Benefícios**

- raw truth independente do banco hot;
- schema serving pode evoluir sem destruir dados brutos;
- ClickHouse pode ter TTL agressivo;
- reprocessamento e replay tornam-se possíveis;
- reconciliação independente detecta perdas/duplicações;
- Parquet/Iceberg é mais adequado a histórico bruto de longo prazo;
- a camada hot pode ser reconstruída ou substituída no futuro.

## 2. Escolha

Para a expectativa explícita de crescimento considerável do banco, a opção B é a base da v4.

O detalhe importante é que **não se força exactly-once em toda a cadeia**:

```text
Kafka market.raw
      |
      +--> Iceberg: commit associado aos checkpoints Flink
      |
      +--> Kafka market.cleaned: at-least-once, baixa latência
                                    |
                                    +--> ClickHouse ReplacingMergeTree
```

O hot path usa `event_id` determinístico e tabelas `ReplacingMergeTree`. Duplicatas temporárias do caminho at-least-once são toleradas e consultas sensíveis usam `FINAL`.

Forçar saída Kafka transacional/exactly-once faria a visibilidade dos eventos depender da conclusão dos checkpoints. Para monitoramento, isso cria latência artificial sem aumentar a confiabilidade do raw truth.

## 3. Raw truth

`market.raw` existe no Kafka apenas como buffer temporário. O raw truth durável é:

```text
Iceberg table: lakehouse.market.raw_events
Object storage: Garage S3
Catalog: Lakekeeper REST
Format: Parquet + Zstd
Partitions: event_date, venue
```

Cada linha preserva:

- `event_id` determinístico;
- `source_id`;
- venue/channel/event_type/symbol;
- sequence IDs;
- timestamps da exchange e da ingestão;
- campos normalizados;
- bids/asks quando existem;
- `raw_payload` integral;
- metadata JSON.

O ClickHouse hot propositalmente **não** mantém `raw_payload`, `bids_json` ou `asks_json`.

## 4. Event identity e deduplicação

O produtor cria `event_id` estável a partir de atributos que identificam o evento. O mesmo evento reprocessado preserva sua identidade.

No serving tier:

```text
ReplacingMergeTree(ingest_time_ms)
```

permite que a versão mais nova substitua logicamente uma anterior com a mesma sorting key.

A reconciliação mede também `uniqExact(event_id)` e não apenas `count()`.

## 5. Microestrutura

O `book-worker` é stateful e separado do Flink SQL.

### Binance

```text
WebSocket conectado
   |
buffer de deltas pré-snapshot
   |
REST snapshot
   |
validação U/u
   |
apply delta
   |
gap? ---> market.control.resync ---> ingestor solicita novo snapshot
```

### Coinbase

O feed Level2 fornece `snapshot` e `l2update`; o worker aplica alterações de quantidade e remove nível quando quantidade = 0.

### Features calculadas

- best bid/ask;
- mid;
- spread e spread bps;
- microprice;
- microprice offset;
- imbalance L1/L5/L10/L20;
- weighted imbalance;
- depth a ±10 bps;
- OFI sequencial;
- TFI em janela de 60 s.

## 6. Candles materializados

Executar RSI/MACD/volatilidade lendo milhões de trades a cada requisição não escala. O `candle-worker` materializa candles de 1 minuto em `market.candles_1m`.

O Analytics agrega 1m para intervalos maiores. Isso desacopla custo de consulta do volume bruto de trades.

## 7. Storage

### Garage

Garage é o default S3-compatible. A entrega usa single-node para simplificar o host único; o catálogo Iceberg autoritativo continua sendo Lakekeeper. Para capacidade maior no mesmo host, aponte `GARAGE_DATA_DIR` para RAID/ZFS/LVM. Para redundância real, implante múltiplos nós Garage em hosts distintos. A escolha evita depender do SeaweedFS 4.47 como raw truth após um relato recente de inconsistência S3/tombstone; consulte `docs/VALIDATION.md`.

### Lakekeeper

Lakekeeper persiste apenas catálogo/metadados em PostgreSQL. Os objetos Parquet continuam no Garage.

### ClickHouse

Serving tier. Dados mais volumosos recebem TTL curto; candles têm TTL maior.

## 8. Maintenance Iceberg

A v4 usa recursos de maintenance do Iceberg/Flink:

- `RewriteDataFiles` para reduzir small files;
- `ExpireSnapshots` para limitar metadata/version history;
- `DeleteOrphanFiles` para remover objetos não referenciados após janela de segurança.

Expirar snapshots **não equivale** a remover os registros atuais da tabela.

## 9. Failure domains

| Falha | Impacto esperado |
|---|---|
| monitor-api | ingestão continua |
| analytics-api | ingestão e monitor continuam |
| Grafana | dados continuam |
| ClickHouse | raw truth continua em Iceberg; hot serving indisponível |
| Flink | `market.raw` acumula até retenção/capacidade Kafka |
| Kafka | ingestão para; raw já commitado no Iceberg continua |
| Garage | raw commits Iceberg param; hot path pode temporariamente continuar até limites operacionais |
| Lakekeeper | novos commits/consultas Iceberg ficam indisponíveis |
| reconciler | nenhuma perda de ingestão; integridade deixa de ser verificada |

## 10. Single-host vs HA

Esta entrega é otimizada para **single-host com discos persistentes**. Ela não finge fornecer HA distribuído:

- Kafka tem 1 broker/controller;
- Flink tem 1 JobManager;
- ClickHouse é single node;
- Lakekeeper Postgres é single node;
- Garage está em modo single-node/replication factor 1.

Para HA real, esses componentes precisam ser distribuídos em hosts distintos com replication/quorum apropriados.
