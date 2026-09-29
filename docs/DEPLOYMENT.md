
## Seleção de mercados

A lista de criptos não é enviada por API. Edite `config/markets.yaml`, que é montado no container em `/opt/app/config/markets.yaml`. Alterações são detectadas pelos ingestors e provocam reconexão automática sem rebuild.

Exemplo mínimo:

```yaml
watchlist: [BTC, ETH, SOL, XRP]
sources:
  binance:
    enabled: true
    quote: USDT
    products: []
    channels: [depth, trade, book_ticker]
  coinbase:
    enabled: false
    quote: USD
    products: []
    channels: [ticker]
```

`products` não vazio substitui a derivação por `watchlist + quote`.

# Deployment

## 1. Preparação

```bash
cp .env.example .env
$EDITOR .env
make bootstrap
make preflight
```

Use caminhos absolutos quando os dados estiverem em discos dedicados:

```text
KAFKA_DATA_DIR=/mnt/nvme-kafka/crypto
CLICKHOUSE_DATA_DIR=/mnt/nvme-clickhouse/data
CLICKHOUSE_LOG_DIR=/mnt/nvme-clickhouse/logs
GARAGE_META_DIR=/mnt/ssd-meta/garage
GARAGE_DATA_DIR=/mnt/raw-raid/crypto-raw
LAKEKEEPER_PG_DATA_DIR=/mnt/ssd-meta/lakekeeper
FLINK_CHECKPOINT_DIR=/mnt/ssd-meta/flink/checkpoints
FLINK_SAVEPOINT_DIR=/mnt/ssd-meta/flink/savepoints
```

## 2. Build inicial

```bash
make build
```

As imagens Python copiam apenas os arquivos de requirements. O source code não entra na imagem.

A imagem Flink adiciona somente os JARs de Kafka/Iceberg necessários ao runtime.

## 3. Subida

```bash
make up
make status
make smoke
```

## 4. Mudanças sem rebuild

### Python

`./src` é bind-mounted. APIs fazem reload automático quando `API_RELOAD=true`.

Workers são processos long-lived; após alterar código:

```bash
make reload
```

O comando usa `./scripts/compose-safe.sh restart`, não `build`.

### Alert rules

Edite:

```text
config/alerts.yaml
```

O worker monitora `mtime` e recarrega as regras automaticamente.

### Flink SQL

Edite:

```text
infra/flink/jobs/01_streaming.sql.template
```

Depois:

```bash
make reload-flink
```

O job corrente é cancelado e o supervisor o recria a partir do arquivo bind-mounted.

### ClickHouse SQL

`infra/clickhouse/init/001_schema.sql` é reaplicado pelo `clickhouse-schema-sync` no startup. Para nova migration em uma instalação existente, prefira adicionar um SQL versionado em vez de editar destrutivamente uma tabela existente.

### Grafana

Dashboards e provisioning são bind-mounted. Reinicie apenas o Grafana se necessário:

```bash
./scripts/compose-safe.sh restart grafana
```

## 5. Quando rebuild é necessário

Rebuild apenas se mudar:

- `requirements/*.txt`;
- `docker/*.Dockerfile`;
- versão do Flink/Iceberg connector incorporada à imagem.

```bash
make build
```

## 6. Crescimento do raw storage

Em single-host, aponte `GARAGE_DATA_DIR` para um filesystem expansível no host (mdraid/ZFS/LVM). Isso não exige alteração de código nem rebuild.

```text
GARAGE_META_DIR=/mnt/ssd-meta/garage
GARAGE_DATA_DIR=/mnt/raw-raid/crypto-raw
```

Para redundância real, converta o Garage para deployment multi-node em hosts distintos; Iceberg/Lakekeeper continuam usando o mesmo endpoint S3 lógico.

## 7. Tools profile

```bash
./scripts/compose-safe.sh --profile tools up -d kafka-ui
```

## 8. Parada

```bash
make down
```

`./scripts/compose-safe.sh down` não remove os diretórios do host. **Não use `rm -rf data`** e não confunda esta arquitetura com named volumes efêmeros.

## 9. Atualização de imagem

Antes de mudar versões de Kafka/Flink/Iceberg/ClickHouse/Lakekeeper/Garage:

1. backup do catálogo Lakekeeper;
2. confirmar checkpoints/savepoints Flink;
3. verificar reconciliação sem divergências;
4. pin de imagem nova;
5. atualizar um componente por vez;
6. `make smoke` e observar Grafana/reconciliation.
