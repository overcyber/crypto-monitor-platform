# Telegram Bot

O container `telegram-bot` recebe alertas de preço e alertas críticos e permite alterar watchlist/regras sem rebuild.

## 1. Criar o bot

Crie o bot no `@BotFather` e copie o token.

No `.env`:

```env
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=123456:SEU_TOKEN
TELEGRAM_ALLOWED_CHAT_IDS=
```

Envie `/start` para o bot e descubra o chat ID:

```bash
docker compose stop telegram-bot
make telegram-discover
```

Coloque o ID encontrado no `.env`:

```env
TELEGRAM_ALLOWED_CHAT_IDS=123456789
```

Para mais de um chat:

```env
TELEGRAM_ALLOWED_CHAT_IDS=123456789,987654321
```

## 2. Iniciar/recarregar

```bash
make telegram-restart
make telegram-logs
```

## 3. Testar a API do Telegram

```bash
make telegram-test
```

Esse teste valida `getMe` e envia uma mensagem de teste para cada chat permitido.

No próprio Telegram:

```text
/test
/price BTC
/status
```

## Comandos

```text
/status
/test
/price BTC
/watchlist
/monitor BTC
/unmonitor BTC
/setpct BTC 2
/setpct BTC 2 3
/setprice BTC 86000 82000
/clearalert BTC
/alerts
/help
```

`/setpct BTC 2 3`: alerta quando BTC sobe 2% ou cai 3% em relação ao preço-base. Após disparar, o preço do disparo vira a nova base.

`/setprice BTC 86000 82000`: alerta ao cruzar `>= 86000` ou `<= 82000`.

`/monitor DOGE`: adiciona DOGE à `config/markets.yaml`; os ingestors fazem hot reload.

## Alertas críticos

O bot consome `market.alerts`. Por padrão encaminha somente `critical`:

```yaml
forward_alerts:
  enabled: true
  severities: [critical]
```

Para incluir warnings:

```yaml
severities: [critical, warning]
```

## Persistência

- `config/telegram.yaml`: regras de preço;
- `config/markets.yaml`: watchlist;
- `data/telegram/state.json`: preço-base, cooldown e estado de cruzamento;
- somente IDs em `TELEGRAM_ALLOWED_CHAT_IDS` podem executar comandos.

## Diagnóstico de token

Os scripts `make telegram-discover` e `make telegram-test` validam primeiro `getMe`, removem aspas/CR do valor lido do `.env` e mostram a descrição retornada pelo Telegram quando o token ou chat ID são inválidos.
