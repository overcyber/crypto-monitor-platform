# Guia Detalhado da Plataforma: Crypto Monitor Platform v4

Este documento descreve detalhadamente o funcionamento, a arquitetura, as integrações entre serviços, os ajustes críticos de runtime e as rotinas operacionais da plataforma.

---

## 1. Visão Geral e Princípios

A **Crypto Monitor Platform v4** é uma arquitetura de dados e análise de mercado de criptomoedas self-hosted e tolerante a falhas, projetada para processar centenas de milhões de eventos por dia com separação explícita entre:

1. **Camada de Transporte e Buffer:** Apache Kafka (Redpanda / KRaft mode).
2. **Camada de Processamento de Fluxo:** Apache Flink 1.20 (Streaming SQL e Lakehouse Connector).
3. **Camada da Verdade Bruta (Raw Truth):** Apache Iceberg (formato Parquet + Zstandard) armazenado em S3-compatível (Garage) com catálogo gerenciado via Lakekeeper REST Catalog.
4. **Camada Analítica de Baixa Latência (Hot Tier):** ClickHouse (ReplacingMergeTree particionado por data/venue com TTL agressivo para dados transientes).
5. **Workers de Especialidade:** Reconstrução de Livro L2 (microestrutura, spread, OFI, microprice), Agregação de Velas (OHLCV 1m/5m/1h) e Engine de Alertas.
6. **Camada de Apresentação:** Web UI (Porta 8090), Grafana (Porta 3001), APIs REST (Monitor `:8081`, Analytics `:8082`, Replay `:8084`) e Bot do Telegram.

> **Importante:** O sistema é estritamente analítico e descritivo; **não executa ordens de compra ou venda**.

---

## 2. Diagrama de Fluxo e Componentes

```text
       +-----------------------+     +-----------------------+
       |   Binance WebSocket   |     |   Coinbase WebSocket  |
       +-----------+-----------+     +-----------+-----------+
                   |                             |
                   +--------------+--------------+
                                  |
                                  v
                        [ Ingestors Python ]
                                  |
                                  v
                      Kafka: 'market.raw'
                                  |
                                  v
               +--------------------------------------+
               |    Apache Flink Streaming Cluster    |
               | (JobManager, TaskManager, Supervisor)|
               +------------------+-------------------+
                                  |
            +---------------------+---------------------+
            |                                           |
            v                                           v
[ Iceberg Raw Truth Sink ]                  Kafka: 'market.cleaned'
Lakekeeper REST Catalog + Garage S3              (At-Least-Once)
Format: Parquet (Zstd compression)                      |
            |                                           |
            v                                           +-----------------------+
  [ Lakehouse Storage ]                                 |                       |
(raw_events: retention durável)                         v                       v
            |                                  [ ClickHouse Hot Tier ]   [ book-worker ]
            |                                  market.events_hot          Reconstrução L2
            |                                           |                 OFI / Microprice
            +--------------+                            v                       |
                           |                   [ candle-worker ]                v
                           v                   Velas 1m, 5m, 1h           ClickHouse
                   [ reconciler ]                       |           (microstructure_features)
             Validação Stream x Batch                   v                       |
             Iceberg vs. ClickHouse            +--------------------------------+
                           |                   |
                           v                   v
                   [ replay-api ]       [ APIs & Interfaces ]
              Replay Determinístico     - Monitor API (:8081)
              Tópico: 'market.replay'   - Analytics API (:8082)
                                        - Web UI (:8090) & Grafana (:3001)
                                        - Telegram Bot (Alertas e Consulta)
```

---

## 3. Resolução Técnica de Runtime (Flink + Garage S3 + Java 17)

Durante o comissionamento do pipeline Flink v4, três comportamentos críticos foram solucionados e consolidados no repositório:

### 3.1. Resolução de Conexão do SQL Client (`flink-job-supervisor`)
- **Problema:** O container `flink-job-supervisor` não montava o arquivo `flink-conf.yaml`. Ao executar `sql-client.sh -f /tmp/01_streaming.sql`, o Flink 1.20 recorria à configuração default da imagem (`config.yaml`), cujo endereço REST é `0.0.0.0:8081`. Como o JobManager está em outro container (`flink-jobmanager:8081`), o cliente recebia `java.net.ConnectException: Connection refused`.
- **Solução:** Montagem explícita de `./infra/flink/conf/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:ro` no serviço `flink-job-supervisor` do `docker-compose.yml`.

### 3.2. Compatibilidade S3 Garage com AWS SDK v2
- **Problema:** Durante o flush/commit dos arquivos Parquet para o bucket Iceberg no Garage S3, o AWS SDK v2 utilizava por padrão *chunked transfer encoding* (`aws-chunked` com assinaturas intermediárias de payload). O Garage S3 rejeitava a requisição com:
  `InvalidRequestException: Bad request: Invalid payload signature (Status Code: 400)`.
- **Solução:** Configuração explícita do catálogo Iceberg no SQL do Flink ([`infra/flink/jobs/01_streaming.sql.template`](file:///system/crypto-monitor-platform/crypto-monitor-platform-v4.0.0/infra/flink/jobs/01_streaming.sql.template)):
  ```sql
  's3.chunked-encoding-enabled' = 'false',
  's3.path-style-access' = 'true'
  ```

### 3.3. Encapsulamento de Módulos no Java 17 (Kryo / Chill)
- **Problema:** Ao registrar checkpoints periódicos com subtasks de escrita do Iceberg, o serializador Kryo/Chill tentava acessar via reflexão o array interno `java.util.Arrays$ArrayList.a`. No Java 17, a ausência de permissões modulares provocava:
  `InaccessibleObjectException: module java.base does not "opens java.util" to unnamed module`.
- **Solução:** Inclusão de `env.java.opts.all` em [`infra/flink/conf/flink-conf.yaml`](file:///system/crypto-monitor-platform/crypto-monitor-platform-v4.0.0/infra/flink/conf/flink-conf.yaml) contendo:
  `--add-opens=java.base/java.util=ALL-UNNAMED` e as respectivas aberturas para `java.lang`, `java.net`, `java.io`, `java.nio`.

---

## 4. Estrutura de Diretórios e Persistência

Nenhum dado produtivo reside em volumes anônimos. A raiz `./data` organiza as partições de storage:

| Diretório | Serviço | Descrição |
|---|---|---|
| `./data/kafka` | Kafka | Segmentos de log das filas `market.raw` e `market.cleaned` |
| `./data/clickhouse` | ClickHouse | Dados colunares indexados das tabelas hot |
| `./data/clickhouse-logs` | ClickHouse | Logs de servidor e erros de execução |
| `./data/garage/meta` | Garage | Metadados do motor de armazenamento de objetos S3 |
| `./data/garage/data` | Garage | Blocos Parquet duráveis do Apache Iceberg |
| `./data/garage/config` | Garage | Arquivo de runtime `garage.toml` renderizado no bootstrap |
| `./data/lakekeeper-postgres` | Postgres | Base transacional de metadados do Lakekeeper REST Catalog |
| `./data/flink/checkpoints` | Flink | Checkpoints de estado do streaming job |
| `./data/flink/savepoints` | Flink | Savepoints operacionais manuais ou de upgrade |
| `./data/grafana` | Grafana | Preferências, usuários e dashboards locais |
| `./data/reconcile` | Reconciler | Staging SQLite temporário da reconciliação stream x batch |
| `./data/replay` | Replay | Banco de jobs SQLite para reprodução de eventos históricos |
| `./data/telegram` | Telegram Bot | Estado e offset de mensagens do bot |

### Criação via Makefile
O conteúdo binário de `./data` é omitido do Git via `.gitignore`. Para recriar a estrutura completa após clonar o repositório:
```bash
make init-data
```
O comando `make bootstrap` invoca `make init-data` automaticamente antes de renderizar os arquivos de configuração.

---

## 5. Endpoints das APIs

### Monitor API (`http://127.0.0.1:8081`)
- `GET /health`: Estado da API e conectividade com ClickHouse.
- `GET /v1/quotes`: Lista as cotações mais recentes de todos os pares monitorados.
- `GET /v1/quote/{symbol}`: Cotação atual detalhada (ex: `BTC`, `BTCUSDT`, `ETH`).
- `GET /v1/history/{symbol}`: Amostra histórica recente de trades e livros.
- `GET /v1/venues`: Resumo de venues ativos, volume de linhas e última ingestão.
- `GET /v1/watchlist`: Lista de ativos e pares derivados ativos.
- `GET /v1/alerts`: Histórico de alertas disparados pelo engine de regras.
- `GET /v1/reconciliation/latest`: Resultados das últimas conferências stream x batch.
- `WS /v1/stream/{symbol}`: Stream WebSocket em tempo real para consumo de clientes.

### Analytics API (`http://127.0.0.1:8082`)
- `GET /health`: Estado operacional do serviço.
- `GET /v1/analyze/{symbol}`: Visão analítica completa (técnica, volatilidade, regime e microestrutura).
- `GET /v1/indicators/{symbol}`: Indicadores técnicos clássicos (RSI 14, MACD, Bandas de Bollinger, ATR, Anomalia de Volume). Suporta `?interval=1m` e `?bars=300`.
- `GET /v1/volatility/{symbol}`: Estimadores de volatilidade (Realizada, EWMA, Parkinson, Garman-Klass, Rogers-Satchell).
- `GET /v1/regime/{symbol}`: Detecção descritiva de regime e pontos de mudança (CUSUM, Page-Hinkley, ADWIN).
- `GET /v1/microstructure/{symbol}`: Métricas de microestrutura (Spread bps, Imbalance L1/L5/L10/L20, OFI, TFI 60s, Microprice).
- `GET /v1/correlation`: Matriz de correlação de Pearson/Spearman e Beta contra o benchmark (BTCUSDT).

### Replay API (`http://127.0.0.1:8084`)
- `GET /health`: Estado do serviço e SQLite de jobs.
- `POST /v1/replay`: Inicia uma tarefa de reprodução histórica a partir do Iceberg.
- `GET /v1/replay/{job_id}`: Consulta progresso, contagem de eventos e status do job.

### Web UI (`http://127.0.0.1:8090`)
Interface reativa em navegador com monitoramento de pipeline, visualização de livros, indicadores gráficos, painel de saúde de containers e gestão de configurações.

---

## 6. Comandos de Manutenção e Auditoria

```bash
make status         # Lista saúde de todos os 21 containers
make smoke          # Executa 16 testes de validação end-to-end
make storage        # Exibe ocupação de disco por partição e componente
make reload         # Recarrega workers Python sem rebuild
make reload-flink   # Re-submete job de streaming após ajustes no SQL
make telegram-logs  # Acompanha logs do bot Telegram
make web-logs       # Acompanha logs da interface Web
```
