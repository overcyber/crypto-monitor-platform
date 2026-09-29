# Validação da entrega

## O que é testado offline

`make validate` executa:

- `compileall` do Python;
- unit tests;
- parsers Binance/Coinbase;
- geração/roundtrip de eventos e IDs estáveis;
- sequência/gap do L2 Binance;
- microprice, imbalance, OFI e deduplicação;
- indicadores/volatilidade/regime;
- alert rules;
- separação de rotas monitor vs analytics;
- YAML do compose;
- XML ClickHouse;
- presença de host bind mounts;
- garantia de que Dockerfiles Python não copiam `src`;
- garantia de que `.dockerignore` exclui `data/`;
- configuração Flink Iceberg + hot at-least-once;
- schema ClickHouse hot slim;
- persistência de alertas/reconciliation/candles;
- object store Garage S3 + catálogo Lakekeeper externo;
- datasource Grafana read-only;
- selftest interno.

## Teste dentro da imagem

```bash
make docker-test
```

## Teste integrado ao vivo

Depois de `make up`:

```bash
make smoke
```

O smoke verifica:

- monitor API;
- analytics API;
- replay API;
- Flink UI;
- ClickHouse;
- Lakekeeper;
- Garage S3;
- Grafana;
- tópico `market.raw`;
- tabela `events_hot`.

## Limitação do ambiente onde este pacote foi produzido

O ambiente de geração desta entrega não possui daemon/CLI Docker disponível. Portanto foi possível validar estaticamente o Compose, compilar/testar o código e verificar contratos/versões externas, mas **não executar aqui um `./scripts/compose-safe.sh up` real**.

Por essa razão a entrega inclui `preflight`, `smoke`, healthchecks e `docker-test` para validação determinística no host de destino. Não trate a ausência de Docker no ambiente de geração como um teste de integração bem-sucedido.
