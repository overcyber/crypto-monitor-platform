# Web UI

A `web-ui` é a interface consolidada para avaliação e configuração da plataforma.

## Acesso

```text
http://127.0.0.1:8090
```

Se o servidor for remoto, use túnel SSH quando `BIND_ADDRESS=127.0.0.1`.

## Avaliação

A interface mostra preços, histórico, indicadores, volatilidade, regime, microestrutura, correlação, Flink, Kafka, ClickHouse, distribuição dos eventos, candles, alertas e reconciliação.

As datas operacionais são exibidas em `America/Sao_Paulo`.

## Configuração

A aba **Configuração** grava os arquivos do host diretamente:

- `config/markets.yaml`: watchlist, Binance/Coinbase, quote, produtos explícitos, canais, depth interval e snapshot limit;
- `config/alerts.yaml`: regras de alerta, source, venue, símbolo, event type, field, operador, valor, cooldown e severidade;
- `config/telegram.yaml`: defaults, forwarding e alertas de preço/percentual.

Os serviços que já suportam hot reload percebem as alterações sem rebuild.

Antes de cada gravação a Web UI cria backup em:

```text
config/.web-ui-backups/
```

Controle de escrita:

```env
WEB_UI_CONFIG_WRITE_ENABLED=true
```

Se `BIND_ADDRESS` for diferente de loopback (`0.0.0.0`, IP da LAN etc.), configure obrigatoriamente:

```bash
openssl rand -hex 32
```

```env
WEB_UI_ADMIN_TOKEN=<valor-gerado>
```

A interface pedirá esse token para operações de gravação. Ele é mantido apenas em `sessionStorage` no navegador. Use `WEB_UI_CONFIG_WRITE_ENABLED=false` para modo somente leitura. Segredos (`TELEGRAM_BOT_TOKEN`, chat IDs e demais credenciais do `.env`) não são expostos nem gravados pela Web UI.

## Concorrência ClickHouse

A Web UI serializa suas consultas através de um lock próprio porque `clickhouse-connect` não permite consultas concorrentes na mesma sessão. Isso evita:

```text
ProgrammingError: Attempt to execute concurrent queries within the same session
```

## Regime

O classificador precisa de 60 candles no intervalo escolhido. A interface inicia em `1m` para disponibilizar regime mais rapidamente e mostra cobertura `disponíveis/requeridos` quando ainda não há histórico suficiente.

## Estabilidade e seleção de ativo (v4.3.3)

- **Ativo** é um `select`, populado pela `watchlist` e pelas cotações disponíveis.
- Mercado/analytics atualizam no ciclo rápido; pipeline em 30 s; alertas em 15 s.
- O gráfico de histórico mantém o último conjunto válido quando uma consulta transitória falha, evitando desaparecer.
- Consultas de inventário/storage/candles/microestrutura usam sessões ClickHouse independentes por query para permitir concorrência segura.
- Mensagens `chrome-extension://...` no console são causadas por extensões do navegador. Teste em janela anônima ou desative a extensão correspondente para confirmar.
