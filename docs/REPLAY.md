# Replay determinístico

## Objetivo

Replay serve para reproduzir exatamente uma janela do raw truth em um caminho isolado, permitindo validar:

- reconstrução de livro;
- gap handling;
- OFI;
- microprice;
- mudanças em algoritmos de features;
- regressões entre versões.

## Isolamento

```text
Iceberg raw_events
       |
   replay-api
       |
  market.replay
       |
replay-book-worker
       |
market.replay.microstructure
```

O replay **não publica em `market.cleaned`** e não modifica `events_hot`.

## Ordenação

Antes de publicar, os registros são materializados em SQLite temporário no host e ordenados por:

```text
event_time_ms ASC,
ingest_time_ms ASC,
event_id ASC,
sequence_id ASC
```

Isto torna o replay estável para o mesmo snapshot/tabela e janela.

## Persistência de jobs

`REPLAY_DATA_DIR/jobs.sqlite` usa WAL. Status e progresso sobrevivem ao restart da API. Jobs que estavam `running` durante um crash são marcados `interrupted` ao reiniciar.

## Exemplo

```bash
curl -X POST http://127.0.0.1:8084/v1/replay \
  -H 'Content-Type: application/json' \
  -d '{
    "start_ms": 1780000000000,
    "end_ms": 1780000600000,
    "venue": "binance",
    "symbol": "BTCUSDT",
    "speed": 0
  }'
```

`speed=0` envia tão rápido quanto o producer permite. Um valor positivo preserva proporcionalmente o tempo entre eventos.

Consultar:

```bash
curl http://127.0.0.1:8084/v1/replay/<job_id>
```

## Limite

```text
REPLAY_MAX_EVENTS=500000
REPLAY_BATCH_SIZE=5000
```

O limite impede uma solicitação acidental de anos de L2 por uma API síncrona de controle. Para reprocessamento massivo, use jobs batch dedicados em vez de aumentar ilimitadamente esse endpoint.
