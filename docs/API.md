# APIs

## Convenções

- símbolos Binance: `BTCUSDT`, `ETHUSDT`;
- símbolos Coinbase: `BTC-USD`, `ETH-USD`;
- `venue` em minúsculas (`binance`, `coinbase`);
- timestamps de evento em UTC e milissegundos quando há campo `_ms`.

## Monitor API — `:8081`

É propositalmente livre de rotas analíticas.

### Health

```bash
curl http://127.0.0.1:8081/health
```

### Quote

```bash
curl 'http://127.0.0.1:8081/v1/quote/BTCUSDT?venue=binance'
```

### Quotes

```bash
curl 'http://127.0.0.1:8081/v1/quotes?venue=binance&limit=100'
```

### Histórico hot

```bash
curl 'http://127.0.0.1:8081/v1/history/BTCUSDT?venue=binance&event_type=trade&limit=1000'
```

### Watchlist

```bash
curl http://127.0.0.1:8081/v1/watchlist
```

### Alertas

```bash
curl 'http://127.0.0.1:8081/v1/alerts?symbol=BTCUSDT&limit=50'
```

### Reconciliação

```bash
curl 'http://127.0.0.1:8081/v1/reconciliation/latest?limit=50'
```

### WebSocket

```text
ws://127.0.0.1:8081/v1/stream/BTCUSDT?venue=binance&poll_ms=1000
```

O endpoint WebSocket lê a visão hot do ClickHouse e envia nova cotação quando `event_time_ms` muda. Para centenas/milhares de consumidores simultâneos, uma futura fanout layer dedicada é preferível a polling por cliente.

## Analytics API — `:8082`

### Indicadores

```bash
curl 'http://127.0.0.1:8082/v1/indicators/BTCUSDT?venue=binance&interval=1h&bars=300'
```

Retorna RSI14, MACD, Bollinger, ATR14 e anomalia de volume.

### Volatilidade

```bash
curl 'http://127.0.0.1:8082/v1/volatility/BTCUSDT?venue=binance&interval=1h&bars=300'
```

Inclui realized, EWMA e estimadores OHLC como Parkinson/Garman-Klass/Rogers-Satchell quando os dados permitem.

### Regime

```bash
curl 'http://127.0.0.1:8082/v1/regime/BTCUSDT?venue=binance&interval=1h&bars=300'
```

A resposta é descritiva e não é previsão de preço.

### Microestrutura

```bash
curl 'http://127.0.0.1:8082/v1/microstructure/BTCUSDT?venue=binance'
```

### Correlação

```bash
curl 'http://127.0.0.1:8082/v1/correlation?benchmark=BTCUSDT&symbols=ETHUSDT,SOLUSDT&venue=binance&interval=1h&bars=300'
```

Usa retornos, não preços crus.

### Análise agregada

```bash
curl 'http://127.0.0.1:8082/v1/analyze/BTCUSDT?venue=binance&interval=1h&bars=300'
```

## Replay API — `:8084`

Consulte [`REPLAY.md`](REPLAY.md).

## Swagger/OpenAPI

FastAPI publica documentação automaticamente:

```text
http://127.0.0.1:8081/docs
http://127.0.0.1:8082/docs
http://127.0.0.1:8084/docs
```
