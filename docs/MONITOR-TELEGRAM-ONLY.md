# Guia: Execução Leve (Monitor de Preços + Alertas Telegram) & Diagnóstico do Monero (XMR)

Este documento detalha como operar a plataforma **Crypto Monitor** em modo leve, executando **exclusivamente** os serviços necessários para capturar preços em tempo real, consultar cotações e disparar alertas via Telegram, além de documentar a análise técnica da ausência de dados do **Monero (XMR)**.

---

## 1. Por que rodar apenas o Monitor + Alertas Telegram?

No modo completo (`make up`), o sistema executa 18 serviços incluindo:
- Reconstrução completa de Order Book L2 de 1.000 níveis (`book-worker`)
- Agregação contínua de candles multi-resolução (`candle-worker`)
- API analítica quant com RSI, MACD, Bollinger e modelos de mudança de regime (`analytics-api`)
- Motor de auditoria e reconciliação Lakehouse (`reconciler`)
- Motor de replay determinístico histórico (`replay-api` e `replay-book-worker`)
- Motor de regras de anomalias estatísticas (`alert-worker`)
- Dashboards analíticos Grafana (`grafana`)

Para operações focadas puramente em **acompanhamento de preço e alertas do Telegram**, todos esses componentes podem permanecer desligados, resultando em:
- **Redução de ~70% no uso de memória RAM** (~1.5 GB a 2 GB economizados).
- **Redução drástica no uso de CPU** (elimina os cálculos contínuos de microestrutura e modelos quantitativos).
- **Menor consumo de disco e rede**.

---

## 2. Arquitetura do Modo Leve

```
┌─────────────────────────────────┐
│     Exchanges (WebSocket)       │
│      Binance & Coinbase         │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│  ingestor-binance / coinbase   │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│       Kafka: market.raw         │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│       Flink Streaming           │  ◄── Lakekeeper + Garage S3
│ (market-raw-to-iceberg-and-clean)│
└────────────────┬────────────────┘
                 │
                 ├────────────────────────────────┐
                 │                                │
                 ▼                                ▼
┌─────────────────────────────────┐   ┌─────────────────────────────┐
│      Kafka: market.cleaned      │   │    ClickHouse (events_hot)  │
└────────────────┬────────────────┘   └──────────────┬──────────────┘
                 │                                   │
                 ▼                                   ▼
┌─────────────────────────────────┐   ┌─────────────────────────────┐
│          telegram-bot           │   │         monitor-api         │
│  (alertas de preço em push e    │──►│     (porta :8081 - cotações │
│   avaliação contínua de regras) │   │      rápidas para /price)   │
└─────────────────────────────────┘   └─────────────────────────────┘
```

### Serviços Ativos no Modo Leve
| Serviço | Função |
| :--- | :--- |
| `kafka` & `kafka-init` | Barramento de mensagens em tempo real |
| `garage` | Object storage S3 leve local (necessário para o catálogo Iceberg) |
| `lakekeeper-*` | Catálogo REST Iceberg (utilizado pelo Flink SQL) |
| `flink-*` | Limpeza, enriquecimento e roteamento dos eventos (`market.cleaned`) |
| `clickhouse-*` | Armazenamento colunar rápido das cotações em tempo real |
| `ingestor-binance` | Conexão WebSocket com a Binance para cotações em tempo real |
| `ingestor-coinbase` | Conexão WebSocket com a Coinbase para cotações em tempo real |
| `monitor-api` | API REST (:8081) com endpoint `/v1/quote/{symbol}` |
| `telegram-bot` | Bot interativo do Telegram com envio automático de alertas |
| *(Opcional)* `web-ui` | Interface web em http://localhost:8090 |

---

## 3. Como Subir o Sistema (Monitor + Telegram)

### 3.1 Via Makefile (Recomendado)

Para subir **somente** o monitor e o bot do Telegram:
```bash
make up-monitor
```

Caso queira incluir também a interface Web UI (`:8090`):
```bash
./scripts/up-monitor.sh --with-ui
```

### 3.2 Via Docker Compose Direto
```bash
./scripts/compose-safe.sh up -d \
  kafka kafka-init \
  garage lakekeeper-db lakekeeper-migrate lakekeeper lakekeeper-bootstrap lakekeeper-warehouse \
  clickhouse clickhouse-schema-sync clickhouse-retention-init \
  flink-jobmanager flink-taskmanager flink-job-supervisor \
  ingestor-binance ingestor-coinbase \
  monitor-api telegram-bot
```

### 3.3 Verificando o Estado dos Serviços
```bash
docker compose ps
```

---

## 4. Otimização de Canais em `config/markets.yaml` (Somente Preço)

Para economizar largura de banda e não baixar snapshots pesados de livros de ofertas das corretoras, configure `config/markets.yaml` apenas com canais de negócios e tickers:

```yaml
version: 1
reload_seconds: 5

watchlist:
  - BTC
  - ETH
  - SOL

sources:
  binance:
    enabled: true
    quote: USDT
    products: []
    channels:
      - trade
      - book_ticker
    depth_interval: 100ms
    snapshot_limit: 1000

  coinbase:
    enabled: true
    quote: USD
    products: []
    channels:
      - matches
      - ticker
```

> [!TIP]
> Ao alterar `config/markets.yaml`, os ingestors aplicam a mudança instantaneamente via **hot reload** sem necessidade de reiniciar nenhum container.

---

## 5. Configuração e Comandos do Telegram Bot

### 5.1 Configuração no `.env`
Certifique-se de que o bot está habilitado no `.env`:
```env
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN="SEU_TOKEN_DO_BOTFATHER"
TELEGRAM_ALLOWED_CHAT_IDS="SEU_CHAT_ID"
```

Para descobrir o seu chat ID:
```bash
make telegram-discover
```

### 5.2 Teste de Conexão
```bash
make telegram-test
```

### 5.3 Comandos Disponíveis no Telegram
- `/price BTC` — Exibe o preço atual do Bitcoin com timestamp e latência (`age_seconds`).
- `/status` — Exibe o estado operacional do bot e a lista de moedas monitoradas.
- `/watchlist` — Lista de ativos monitorados no bot.
- `/monitor BTC` — Adiciona ativo à watchlist do bot.
- `/unmonitor BTC` — Remove ativo da watchlist do bot.
- `/setpct BTC 2.0` — Dispara alerta se o BTC variar ±2%.
- `/setprice BTC 90000 80000` — Dispara alerta se o BTC atingir teto de $90.000 ou piso de $80.000.
- `/alerts` — Lista as regras de alerta de preço ativas.
- `/clearalert BTC` — Remove regras de preço configuradas para o BTC.

---

## 6. Diagnóstico: Por que o XMR (Monero) não está sendo monitorado?

Se você adicionar `XMR` à sua `watchlist` em `config/markets.yaml`, notará que nenhuma cotação ou negócio é registrado para ele. O motivo foi verificado tecnicamente na integração:

### 6.1 Na Coinbase
- **Situação:** A Coinbase **nunca listou o Monero (XMR)** devido a restrições de compliance com regulamentações norte-americanas para moedas focadas em privacidade (*privacy coins*).
- **Comportamento Técnico:**
  - Requisição REST para `https://api.exchange.coinbase.com/products/XMR-USD` retorna **404**:
    ```json
    {"message": "NotFound"}
    ```
  - Ao tentar assinar o feed WebSocket da Coinbase com `XMR-USD`:
    ```json
    {"type": "error", "message": "Failed to subscribe", "reason": "XMR-USD is not a valid product"}
    ```

### 6.2 Na Binance
- **Situação:** A Binance **deslistou oficialmente o Monero (XMR) em 20 de fevereiro de 2024 às 03:00 UTC**.
- **Comportamento Técnico:**
  - A consulta ao order book na Binance (`/api/v3/depth?symbol=XMRUSDT`) retorna um livro 100% vazio:
    ```json
    {"lastUpdateId": 2806825214, "bids": [], "asks": []}
    ```
  - A última transação registrada na Binance para `XMRUSDT` ocorreu em `1708397999933` (20/02/2024 às 02:59:59 UTC).
  - O WebSocket da Binance (`xmrusdt@trade` / `xmrusdt@bookTicker`) permanece mudo, sem enviar nenhum evento.

### 6.3 Conclusão e Recomendação para o XMR
Como a plataforma consome exclusivamente da **Binance** e **Coinbase**, nenhum dos dois provedores integrados comercializa Monero atualmente.

- **Recomendação imediata:** Remova `- XMR` de `config/markets.yaml` (mantendo ex: `BTC`, `ETH`, `SOL`), evitando erros de subscrição no conector da Coinbase.
- **Para monitorar XMR futuramente:** É necessário criar um novo conector de ingestão (por exemplo, `src/ingestors/kraken.py` ou `src/ingestors/kucoin.py`), pois a Kraken e a KuCoin ainda mantêm pares de negociação ativos para o Monero.
