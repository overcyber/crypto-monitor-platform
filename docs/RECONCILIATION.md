# Reconciliação stream × batch

## Objetivo

A camada hot foi deliberadamente otimizada para latência e usa um caminho at-least-once. A camada raw usa Iceberg como fonte durável. A reconciliação verifica continuamente se ambas representam a mesma janela lógica.

## Fluxo

```text
Iceberg raw_events                         ClickHouse events_hot FINAL
      |                                               |
      +--> Arrow batches                              |
      |                                               |
      +--> SQLite staging (host, disco)               |
              |                                       |
              +--> dedup event_id                     |
              +--> group venue/symbol/type            |
              |                                       |
              +---------------- compare --------------+
                                  |
                        reconciliation_results
                                  |
                        market.reconcile.results
```

## Por que SQLite temporário

Uma janela grande não deve ser carregada em uma lista Python. O reconciler lê PyIceberg em batches e usa SQLite apenas como workspace local para:

- chavear `event_id`;
- deduplicar;
- agregar contagem/volume/notional.

O arquivo fica em `RECONCILE_DATA_DIR`, montado no host.

## Janela

```text
RECONCILE_INTERVAL_SECONDS=300
RECONCILE_LOOKBACK_MINUTES=15
RECONCILE_SETTLE_DELAY_SECONDS=45
```

Em uma execução às 12:00:00 com delay 45 s e lookback 15 min:

```text
window_end   = 11:59:15
window_start = 11:44:15
```

O delay reduz falso mismatch por eventos ainda em trânsito/checkpoint.

## Métricas comparadas

Por `venue`, `symbol`, `event_type`:

- raw rows;
- raw unique event IDs;
- raw quantity;
- raw notional;
- hot rows;
- hot unique event IDs;
- hot quantity;
- hot notional.

Tolerâncias:

```text
RECONCILE_TOLERANCE_COUNT=0
RECONCILE_TOLERANCE_VOLUME_PCT=0.0001
RECONCILE_TOLERANCE_NOTIONAL_PCT=0.0001
```

## Interpretação

- `OK`: métricas dentro das tolerâncias;
- `MISMATCH`: divergência detectada.

Consulte:

```bash
curl 'http://127.0.0.1:8081/v1/reconciliation/latest?limit=100'
```

Ou no Grafana.

## Por que não há auto-repair

Reparo automático pode transformar uma falha transitória, atraso de checkpoint ou bug no reconciler em uma escrita destrutiva. A v4 escolhe:

```text
detectar -> registrar -> alertar -> investigar -> reparar explicitamente
```

O raw truth preserva os dados necessários para reprocessamento posterior.

## Diagnóstico de mismatch

1. confirme lag do Kafka;
2. confirme estado/checkpoints do Flink;
3. confirme que ClickHouse Kafka engine continua consumindo;
4. compare `raw_unique_events` com `hot_unique_events`;
5. verifique gaps/resync do book separadamente — book features não fazem parte da contagem raw/hot principal;
6. aumente `settle_delay` temporariamente se o mismatch for exclusivamente atraso;
7. nunca apague o raw truth para “igualar” o hot.
