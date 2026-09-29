# Storage, crescimento e retenção

## 1. Princípio

A arquitetura evita guardar o mesmo payload bruto para sempre em todos os componentes.

```text
Kafka       = buffer curto
Iceberg     = raw truth de longo prazo
ClickHouse  = hot serving tier
candles_1m  = histórico analítico compacto
```

## 2. Dimensionamento inicial

O fator determinante é o número de eventos por segundo e o tamanho **comprimido** por evento no Iceberg.

Aproximação conservadora sem assumir compressão:

```text
bytes/dia = eventos_por_segundo × bytes_por_evento × 86400
```

Exemplos binários aproximados:

| Eventos/s | 500 B/evento | 1 KiB/evento | 2 KiB/evento |
|---:|---:|---:|---:|
| 100 | 4.0 GiB/d | 8.0 GiB/d | 16.1 GiB/d |
| 500 | 20.1 GiB/d | 40.2 GiB/d | 80.5 GiB/d |
| 1.000 | 40.2 GiB/d | 80.5 GiB/d | 160.9 GiB/d |
| 5.000 | 201.2 GiB/d | 402.3 GiB/d | 804.7 GiB/d |

Parquet + Zstd normalmente reduz esse volume, mas o fator real deve ser medido no seu feed; L2 JSON e cardinalidade dos campos mudam muito a taxa de compressão.

## 3. Headroom

Não dimensione discos para 100% de utilização. ClickHouse merges e Iceberg rewrites precisam de espaço temporário.

Recomendação operacional: manter **25–30% de espaço livre** nos discos de dados durante operação normal.

## 4. Layout recomendado de discos

Para um host com NVMe + HDD:

```text
NVMe 1  -> Kafka
NVMe 2  -> ClickHouse
SSD     -> PostgreSQL/Lakekeeper + Flink checkpoints
HDD/RAID -> Garage raw truth
```

Se houver apenas um SSD/NVMe, tudo funciona, mas a contenção de I/O entre Kafka, ClickHouse merges e Iceberg compaction pode aumentar latência.

## 5. Retenções

Configuração padrão:

```text
KAFKA_RAW_RETENTION_HOURS=24
KAFKA_CLEAN_RETENTION_HOURS=72
CLICKHOUSE_HOT_RETENTION_DAYS=30
CLICKHOUSE_CANDLE_RETENTION_DAYS=365
CLICKHOUSE_ALERT_RETENTION_DAYS=180
CLICKHOUSE_RECON_RETENTION_DAYS=365
```

### Iceberg

Não existe TTL de linhas raw na configuração padrão. Os seguintes parâmetros são de manutenção de metadata/files:

```text
ICEBERG_SNAPSHOT_MAX_AGE_SECONDS=604800
ICEBERG_MIN_SNAPSHOTS_TO_KEEP=24
ICEBERG_ORPHAN_MIN_AGE_SECONDS=259200
```

- `ExpireSnapshots`: remove snapshots antigos além da política;
- `DeleteOrphanFiles`: remove arquivos não mais referenciados após uma idade mínima;
- `RewriteDataFiles`: compacta small files.

Nenhum deles deve ser confundido com uma política de apagar o histórico atual da tabela raw.

## 6. ClickHouse

### `events_hot`

Tabela estreita. Sem raw JSON e sem arrays de depth. TTL curto.

### `microstructure_features`

Features derivadas, TTL igual ao hot por padrão.

### `candles_1m`

Muito menor que trades/L2, por isso default de 365 dias.

### `alerts` e `reconciliation_results`

Retenções independentes.

## 7. Crescimento do Garage

### Single-host

A entrega usa `--single-node`, portanto `GARAGE_DATA_DIR` deve apontar para um filesystem que possa crescer no host. Exemplos: mdraid, ZFS pool ou LVM sobre múltiplos discos.

```text
GARAGE_META_DIR=/mnt/ssd-meta/garage
GARAGE_DATA_DIR=/mnt/raw-raid/crypto-raw
```

Metadados devem preferencialmente ficar em SSD; objetos Parquet podem ficar em armazenamento de capacidade maior.

### Redundância/HA

Adicionar discos ao mesmo host aumenta capacidade, mas não elimina o host como failure domain. Para raw truth realmente redundante, use o modo multi-node do Garage em hosts físicos distintos e replication factor maior que 1. Essa migração é operacional e não exige mudar Iceberg, Lakekeeper, Flink ou o código Python: o endpoint S3 continua sendo a abstração.

## 8. Backup

### Lakekeeper/PostgreSQL

```bash
make backup-catalog
```

O catálogo é pequeno comparado ao raw data, mas é crítico para localizar snapshots/tabelas.

### Raw object store

A cópia dos diretórios Garage não substitui um plano de backup consistente em produção. Para dados realmente irrecuperáveis, use uma segunda cópia física/host ou replicação externa.

### ClickHouse

ClickHouse é reconstruível conceitualmente a partir do raw truth, mas esta versão **não executa auto-healing destrutivo**. Portanto backup de ClickHouse reduz RTO e continua recomendável.

## 9. Observabilidade de capacidade

```bash
make storage
```

Acompanhe pelo menos:

- filesystem `% used`;
- Kafka log size/lag;
- ClickHouse parts/merges;
- Iceberg file count e tamanho médio;
- duração de checkpoints Flink;
- duração de rewrite/maintenance;
- mismatch de reconciliação.
