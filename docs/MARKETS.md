# Configuração das criptos monitoradas

A ingestão não usa API para escolher ativos. A seleção é feita diretamente em `config/markets.yaml`, montado do host nos containers.

## Exemplo básico

```yaml
version: 1
reload_seconds: 5

watchlist:
  - BTC
  - ETH
  - SOL
  - XRP

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

  kraken:
    enabled: true
    quote: USD
    products: []
    channels: [ticker, trade, book]
```

Com `products: []`, os pares são derivados automaticamente:

- Binance: `BTC` + `USDT` → `BTCUSDT`;
- Coinbase: `BTC` + `USD` → `BTC-USD`;
- Kraken: `XMR` + `USD` → `XMR/USD` (suporta Monero nativamente via WebSocket v2).

## Escolher pares explicitamente

```yaml
watchlist: []
sources:
  binance:
    enabled: true
    quote: USDT
    products: [BTCUSDT, DOGEUSDT, PEPEUSDT]
    channels: [trade, book_ticker]
  coinbase:
    enabled: false
    quote: USD
    products: []
    channels: [ticker]
```

`products` não vazio tem precedência sobre `watchlist`.

## Somente preço

Para Binance, use:

```yaml
channels: [trade, book_ticker]
```

Assim não é solicitado snapshot REST do order book. O preço e negócios continuam vindo por WebSocket.

Para Coinbase:

```yaml
channels: [matches, ticker]
```

## Microestrutura/OFI

Para reconstrução de L2 e OFI, mantenha:

```yaml
# Binance
channels: [depth, trade, book_ticker]

# Coinbase
channels: [level2, matches, ticker]
```

## Hot reload

Os ingestors verificam o arquivo a cada `reload_seconds`. Quando a seleção muda, a conexão WebSocket é recriada com os novos pares/canais.

Não é necessário:

- rebuild da imagem;
- reiniciar Kafka/Flink/ClickHouse;
- usar `monitor-api`;
- enviar configuração por HTTP.

O `monitor-api :8081` serve apenas para consultar dados já ingeridos e pode ser parado sem interromper captura:

```bash
./scripts/compose-safe.sh stop monitor-api
```

## Compatibilidade antiga

Se `config/markets.yaml` não existir, ainda há fallback para as variáveis antigas `BINANCE_SYMBOLS` e `COINBASE_PRODUCTS`. Elas são mantidas apenas para migração; novos deployments devem usar `config/markets.yaml`.
