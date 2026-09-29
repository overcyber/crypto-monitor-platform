# Crypto Monitor Platform v4.3.3

Plataforma self-hosted de **monitoramento e análise de mercado cripto**, separada em microserviços e projetada para crescimento de volume.

A arquitetura deliberadamente separa três funções:

1. **monitoramento**: preço, histórico hot, alertas, watchlist e WebSocket de consumo;
2. **ingestão/microestrutura**: WebSocket das exchanges, Kafka, reconstrução de L2, OFI/microprice;
3. **analytics**: candles materializados, indicadores, volatilidade, correlação e regime.

O sistema **não executa ordens**.


## Quick start
 
```bash
cp .env.example .env
nano .env
make init-data
make bootstrap
make preflight
make pull
make build
make up
make smoke
```

O projeto usa um wrapper de Docker Compose com **paralelismo 1 por padrão**. Isso evita o panic `fatal error: concurrent map writes` observado em algumas versões do Compose durante pulls paralelos. `make up` baixa as imagens externas de forma serial e depois executa o start com `--pull never`.

> **Estrutura de dados (`data/`):** O conteúdo da pasta `data/` é ignorado no Git por segurança e tamanho. O comando `make init-data` (ou `make bootstrap`) cria automaticamente todas as pastas necessárias com permissões adequadas.
> **Lakekeeper:** os releases de container usam prefixo `v`. A versão padrão é `LAKEKEEPER_VERSION=v0.13.6`. Se estiver atualizando uma instalação anterior, ajuste também o seu `.env`; copiar um novo `.env.example` não altera o `.env` existente.

Documentação completa: [`docs/SYSTEM_GUIDE.md`](docs/SYSTEM_GUIDE.md) · [`docs/MAKE-COMMANDS.md`](docs/MAKE-COMMANDS.md) · [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) · [`CHANGELOG.md`](CHANGELOG.md).


## Uso diário

Depois de `make up`:

```bash
make status          # estado dos containers
make smoke           # validação ponta a ponta
make logs            # logs gerais
make telegram-logs   # logs do bot Telegram
make web-logs        # logs da Web UI
```

- Moedas monitoradas: `config/markets.yaml` ou comandos Telegram.
- Alertas técnicos: `config/alerts.yaml`.
- Alertas de preço/percentual: `config/telegram.yaml` ou Telegram.
- Interface principal: Web UI em `${WEB_UI_PORT:-8090}`.
- Dashboard técnico: Grafana em `${GRAFANA_PORT:-3000}`.
- Consulta: Monitor API `:8081`; Analytics API `:8082`.

## Arquitetura

```text
 Binance WS          Coinbase WS
     |                    |
     +------ ingestors ---+
                  |
             market.raw (Kafka, curto prazo)
                  |
                 Flink
            /             \
           /               \
 Iceberg raw truth      market.cleaned (Kafka)
 Garage S3 +              at-least-once
 Lakekeeper                   |
      |                        +----> ClickHouse hot
      |                        |          |
      |                        |          +--> monitor-api :8081
      |                        |          +--> candle-worker
      |                        |          +--> analytics-api :8082
      |                        |
      |                        +----> book-worker
      |                                  |
      |                            microstructure
      |                                  |
      |                              ClickHouse
      |
      +--> reconciler <---------- ClickHouse hot
      |
      +--> replay-api :8084 --> market.replay --> replay-book-worker

Grafana :3000 --> ClickHouse (usuário read-only)
Telegram bot <--> Kafka market.cleaned / market.alerts
Telegram bot --> config/markets.yaml + config/telegram.yaml
Web UI :8090 --> Monitor API + Analytics API + Flink + ClickHouse
```

### Por que esta arquitetura

A alternativa simples `WebSocket -> Kafka -> ClickHouse -> Grafana` tem menos componentes e baixa latência, mas transforma o banco hot em arquivo histórico e dificulta reconstruir o estado após bugs de parser, mudanças de schema ou perda de dados. Nesta versão:

- **Kafka** é buffer/barramento, não arquivo histórico;
- **Iceberg** é a verdade bruta durável;
- **ClickHouse** é o serving/hot store de baixa latência;
- **Flink** valida e normaliza o fluxo e grava o raw truth por checkpoint;
- **reconciliação stream x batch** verifica se a visão hot corresponde ao raw truth;
- **replay determinístico** reproduz eventos históricos em um tópico separado sem contaminar produção.

Detalhes: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).


## Escolher quais criptos monitorar — sem API

A captura **não depende de nenhuma API de configuração**. Edite diretamente o arquivo bind-mounted:

```text
config/markets.yaml
```

Exemplo:

```yaml
version: 1
reload_seconds: 5

watchlist:
  - BTC
  - ETH
  - SOL
  - XRP
  - DOGE

sources:
  binance:
    enabled: true
    quote: USDT
    products: []
    channels: [depth, trade, book_ticker]

  coinbase:
    enabled: true
    quote: USD
    products: []
    channels: [level2, matches, ticker]
```

Com `products: []`, os pares são derivados automaticamente:

```text
BTC + Binance/USDT   -> BTCUSDT
BTC + Coinbase/USD   -> BTC-USD
DOGE + Binance/USDT  -> DOGEUSDT
DOGE + Coinbase/USD  -> DOGE-USD
```

Se quiser controlar exatamente os pares de uma exchange, use `products`:

```yaml
sources:
  binance:
    enabled: true
    quote: USDT
    products: [BTCUSDT, PEPEUSDT, DOGEUSDT]
    channels: [trade, book_ticker]

  coinbase:
    enabled: false
    quote: USD
    products: []
    channels: [ticker]
```

Os ingestors verificam o arquivo a cada `reload_seconds` e **reconectam automaticamente quando a seleção muda**. Não é necessário rebuildar Docker nem usar `monitor-api`.

Para captura somente de preço/negócios na Binance, remova `depth`:

```yaml
channels: [trade, book_ticker]
```

Nesse modo o ingestor Binance não solicita snapshot REST do book; a captura de preço continua via WebSocket. Para analytics de microestrutura/OFI, mantenha `depth`.

O `monitor-api :8081` é apenas uma interface opcional de **consulta dos dados já capturados**. Você pode parar esse container e os ingestors, Kafka, Flink, Iceberg e ClickHouse continuam capturando e processando normalmente:

```bash
./scripts/compose-safe.sh stop monitor-api
```

## Interface Web

Abra:

```text
http://127.0.0.1:8090
```

A interface reúne preço, histórico, indicadores, volatilidade, regime, microestrutura, correlação, saúde dos serviços, jobs Flink, inventário ClickHouse, candles, alertas, reconciliação e configuração ativa.

```bash
make web-logs
curl -s http://127.0.0.1:8090/health | jq
```

Detalhes: [`docs/WEB-UI.md`](docs/WEB-UI.md).


### Configuração pela Web UI

A aba **Configuração** permite alterar diretamente, com validação e backup automático:

- `config/markets.yaml`: watchlist, exchanges, quote, produtos, canais, depth/snapshot;
- `config/alerts.yaml`: regras market/microstructure, operadores, limiar, cooldown e severidade;
- `config/telegram.yaml`: defaults, encaminhamento de alertas e alertas de preço/percentual.

Por padrão `WEB_UI_CONFIG_WRITE_ENABLED=true`. Para modo somente leitura, use `false`. Se `BIND_ADDRESS=0.0.0.0` (ou outro endereço não-loopback), defina também `WEB_UI_ADMIN_TOKEN` (`openssl rand -hex 32`) e informe-o na aba Configuração. Token/chat IDs do Telegram permanecem somente no `.env` e não são expostos na Web UI.

## Telegram

Configuração mínima:

```env
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=SEU_TOKEN
TELEGRAM_ALLOWED_CHAT_IDS=SEU_CHAT_ID
```

Fluxo de configuração/teste:

```bash
make telegram-discover
make telegram-restart
make telegram-test
make telegram-logs
```

Comandos principais: `/price BTC`, `/monitor BTC`, `/unmonitor BTC`, `/setpct BTC 2 3`, `/setprice BTC 86000 82000`, `/alerts`, `/test`.

Detalhes: [`docs/TELEGRAM.md`](docs/TELEGRAM.md).

## Persistência no host

Nenhum dado importante depende de volume Docker anônimo. Por padrão:

```text
./src                               código Python, bind mount read-only
./config                            regras/config; read-only nos workers e read-write apenas no web-ui
./data/kafka                        log Kafka
./data/clickhouse                   ClickHouse
./data/clickhouse-logs              logs ClickHouse
./data/garage/meta                   metadata Garage
./data/garage/data                   raw objects Iceberg/Parquet
./data/lakekeeper-postgres          catálogo Iceberg
./data/flink/checkpoints            checkpoints Flink
./data/flink/savepoints             savepoints Flink
./data/grafana                      Grafana
./data/reconcile                    staging local da reconciliação
./data/replay                       SQLite de jobs/staging de replay
```

Todos os caminhos podem apontar para discos diferentes editando apenas `.env`.

**Alterar `src/`, `config/`, SQL Flink, dashboards ou scripts não exige rebuild das imagens.** Rebuild só é necessário quando mudam dependências do sistema/Python ou Dockerfiles.

## Requisitos

- Linux com Docker Engine e Docker Compose v2;
- `curl` no host;
- para uma instalação single-host contínua: **16 GiB RAM ou mais** é uma recomendação prática; 8 GiB pode ser insuficiente com Flink + ClickHouse + Kafka + Grafana simultâneos;
- armazenamento deve ser dimensionado pelo throughput real. Consulte [`docs/STORAGE.md`](docs/STORAGE.md).

## Instalação

```bash
cp .env.example .env
```

Edite no mínimo:

```text
CLICKHOUSE_PASSWORD
CLICKHOUSE_GRAFANA_PASSWORD
GARAGE_RPC_SECRET
S3_ACCESS_KEY
S3_SECRET_KEY
LAKEKEEPER_PG_PASSWORD
LAKEKEEPER_ENCRYPTION_KEY
GRAFANA_ADMIN_PASSWORD
```

Depois:

```bash
make bootstrap
make preflight
make pull
make build
make up
```

Validação ao vivo:

```bash
make smoke
make status
```

URLs padrão, todas vinculadas a `127.0.0.1`:

| Serviço | URL |
|---|---|
| Web UI | `http://127.0.0.1:8090` |
| Monitor API | `http://127.0.0.1:8081` |
| Analytics API | `http://127.0.0.1:8082` |
| Flink UI | `http://127.0.0.1:8083` |
| Replay API | `http://127.0.0.1:8084` |
| Grafana | `http://127.0.0.1:3001` |
| ClickHouse HTTP | `http://127.0.0.1:8123` |
| Garage S3 | `http://127.0.0.1:8333` |
| Lakekeeper REST Catalog | `http://127.0.0.1:8181` |

Kafka UI é opcional:

```bash
./scripts/compose-safe.sh --profile tools up -d kafka-ui
```

em `http://127.0.0.1:8085`.

## Alterar código sem rebuild

APIs usam Uvicorn com reload quando:

```text
API_RELOAD=true
```

Para workers Python:

```bash
make reload
```

Isso apenas reinicia serviços que leem o código bind-mounted; não reconstrói imagens.

Após alterar `infra/flink/jobs/01_streaming.sql.template`:

```bash
make reload-flink
```

O supervisor detecta o job ausente e o submete novamente com o SQL atual montado do host.

Mudanças em `config/alerts.yaml` são recarregadas automaticamente pelo `alert-worker`.


## Bot Telegram

Ative no `.env`:

```env
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=<token-do-BotFather>
TELEGRAM_ALLOWED_CHAT_IDS=<seu-chat-id>
```

Depois:

```bash
make up
make telegram-logs
```

Comandos principais:

```text
/watchlist
/monitor BTC
/unmonitor BTC
/setpct BTC 2
/setpct BTC 2 3
/setprice BTC 86000 82000
/clearalert BTC
/alerts
/status
```

`/setpct` configura variação percentual para cima/baixo. `/setprice` configura teto e piso e adiciona o ativo à watchlist automaticamente. Alertas críticos do `alert-worker` (`severity: critical`) são encaminhados para o Telegram.

Detalhes: [`docs/TELEGRAM.md`](docs/TELEGRAM.md).

## Crescimento do armazenamento

O raw truth fica no Garage/Iceberg. Em um único host, a forma mais previsível de crescer é apontar `GARAGE_DATA_DIR` para um filesystem expansível, por exemplo mdraid, ZFS ou LVM:

```text
GARAGE_META_DIR=/mnt/ssd-meta/garage
GARAGE_DATA_DIR=/mnt/raw-raid/crypto-raw
```

A configuração single-node não oferece redundância por si só. Para HA real, migre o Garage para múltiplos nós/hosts e defina replication factor adequado.

## Retenção padrão

| Camada | Padrão | Papel |
|---|---:|---|
| Kafka `market.raw` | 24 h | buffer de transporte |
| Kafka `market.cleaned` | 72 h | buffer hot |
| ClickHouse eventos/microestrutura | 30 d | consultas hot |
| ClickHouse candles 1m | 365 d | analytics histórico compacto |
| ClickHouse alertas | 180 d | auditoria operacional |
| ClickHouse reconciliação | 365 d | integridade |
| Iceberg raw events | sem TTL de linhas | raw truth |
| Iceberg snapshots antigos | 7 d | metadados/versionamento, não os dados atuais |

A expiração de snapshots Iceberg **não significa apagar os eventos atuais da tabela**. Ela remove versões antigas não necessárias da tabela; a limpeza de arquivos órfãos é separada.

## Endpoints

### Monitoramento `:8081`

- `GET /health`
- `GET /v1/quote/{symbol}`
- `GET /v1/quotes`
- `GET /v1/history/{symbol}`
- `GET /v1/venues`
- `GET /v1/watchlist`
- `GET /v1/alerts`
- `GET /v1/reconciliation/latest`
- `WS /v1/stream/{symbol}`

### Analytics `:8082`

- `GET /health`
- `GET /v1/indicators/{symbol}`
- `GET /v1/volatility/{symbol}`
- `GET /v1/regime/{symbol}`
- `GET /v1/microstructure/{symbol}`
- `GET /v1/correlation`
- `GET /v1/analyze/{symbol}`

### Replay `:8084`

- `GET /health`
- `POST /v1/replay`
- `GET /v1/replay/{job_id}`

Detalhes e exemplos: [`docs/API.md`](docs/API.md).

## Reconciliação stream x batch

A cada execução, o reconciler:

1. seleciona uma janela já estabilizada;
2. lê o raw truth Iceberg em batches Arrow;
3. usa SQLite temporário no host para deduplicar/agregar sem carregar tudo em RAM;
4. consulta `events_hot FINAL` no ClickHouse;
5. compara contagem de eventos, quantidade e notional por `venue/symbol/event_type`;
6. grava o relatório no ClickHouse e em `market.reconcile.results`.

Não existe reparo automático destrutivo. O sistema **detecta e registra** a divergência; reparo é uma decisão operacional explícita.

Leia [`docs/RECONCILIATION.md`](docs/RECONCILIATION.md).

## Replay determinístico

O replay lê o Iceberg, ordena por:

```text
event_time_ms, ingest_time_ms, event_id, sequence_id
```

e publica em `market.replay`. Um `replay-book-worker` separado reconstrói o livro e publica `market.replay.microstructure`. Produção usa outros tópicos.

Leia [`docs/REPLAY.md`](docs/REPLAY.md).

## Testes

Sem Docker:

```bash
make validate
```

Dentro da imagem:

```bash
make docker-test
```

Com toda a stack rodando:

```bash
make smoke
```

## Documentação

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — decisão arquitetural e fluxo de consistência.
- [`docs/MARKETS.md`](docs/MARKETS.md) — seleção de criptos/exchanges/canais sem API.
- [`docs/ADR-001-STREAMING-STORAGE.md`](docs/ADR-001-STREAMING-STORAGE.md) — decisão formal ClickHouse-only vs Flink+Iceberg e storage.
- [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) — implantação e alterações sem rebuild.
- [`docs/STORAGE.md`](docs/STORAGE.md) — crescimento, retenção e dimensionamento.
- [`docs/RECONCILIATION.md`](docs/RECONCILIATION.md) — stream×batch.
- [`docs/REPLAY.md`](docs/REPLAY.md) — replay determinístico.
- [`docs/API.md`](docs/API.md) — endpoints.
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md) — runbook.
- [`docs/SECURITY.md`](docs/SECURITY.md) — hardening.
- [`docs/VALIDATION.md`](docs/VALIDATION.md) — o que foi efetivamente validado.
- [`docs/SOURCES.md`](docs/SOURCES.md) — fontes e decisões dependentes de versões externas.
- [`docs/MAKE-COMMANDS.md`](docs/MAKE-COMMANDS.md) — referência dos comandos `make` e do wrapper Compose serial.
- [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) — problemas conhecidos, Lakekeeper, Compose e startup.
- [`CHANGELOG.md`](CHANGELOG.md) — histórico de alterações.


## Correções de runtime v4.2.1

Esta distribuição completa inclui `src/common/markets.py`, correção da agregação horária no ClickHouse (`DateTime`), Kafka com `advertised.listeners` válido, healthcheck do ClickHouse com expansão correta das variáveis e criação idempotente do warehouse Lakekeeper.

## Regime: `insufficient_data`

O regime exige 60 barras no intervalo selecionado. A Web UI usa `1m` por padrão. Se consultar `1h` com poucas horas de histórico, o retorno será `insufficient_data` por definição.

```bash
curl -s 'http://localhost:8082/v1/regime/BTC?interval=1m&bars=300' | jq
```

### Web UI v4.3.3

- Ativo selecionável no cabeçalho a partir da watchlist/cotações.
- Histórico recente preserva o último gráfico válido durante erros transitórios.
- Pipeline/inventário usa sessões ClickHouse independentes para concorrência segura.
- Refresh: mercado/analytics rápido, alertas 15 s, pipeline 30 s.


## Correção de concorrência ClickHouse (v4.3.4)

Todas as consultas e inserts usam uma sessão ClickHouse dedicada por operação. Isso evita `Attempt to execute concurrent queries within the same session` em Monitor API, Analytics, Web UI, Candle Worker e Reconciler.


## Histórico Web UI (v4.3.6)

O gráfico **Histórico recente** possui seleção de período de 1, 3, 7, 14, 30, 60 ou 90 dias. A Web UI consulta candles agregados no ClickHouse e ajusta automaticamente a granularidade para evitar carregar milhões de trades no navegador.

Mensagens do console iniciadas por `chrome-extension://` ou `content_script.js` são produzidas por extensões instaladas no navegador. Elas não são emitidas pela Crypto Monitor Web UI. Para confirmar, abra a página em janela anônima sem extensões ou desative a extensão indicada pelo ID exibido no console.


## Correção v4.3.6

O histórico da Web UI evita colisões de alias no ClickHouse por meio de uma subquery com nomes `src_*`. As consultas de cotação não usam `FINAL` e trabalham sobre uma janela hot de 6 horas, reduzindo timeouts sem alterar a semântica de `argMax` para a cotação mais recente.


## Correção Flink 1.20 + Lakehouse Iceberg + Garage S3 (v4.3.7)

1. **Montagem de configuração no Supervisor:** `flink-job-supervisor` monta `infra/flink/conf/flink-conf.yaml`, garantindo que o SQL Client submeta os jobs ao `flink-jobmanager:8081` (evitando erro de `Connection refused`).
2. **Compatibilidade S3 Garage:** `01_streaming.sql.template` define `'s3.chunked-encoding-enabled' = 'false'` no catálogo Iceberg, prevenindo o erro `Invalid payload signature` retornado pelo Garage S3 quando o AWS SDK v2 envia payloads com assinaturas chunked.
3. **Java 17 Module Access:** `flink-conf.yaml` inclui `--add-opens=java.base/java.util=ALL-UNNAMED` em `env.java.opts.all` para permitir a serialização correta de snapshots do Iceberg pelo Kryo/Chill sem violação de encapsulamento.

