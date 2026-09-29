
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




