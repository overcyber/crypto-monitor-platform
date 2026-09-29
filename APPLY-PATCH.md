# Aplicar patch v4.1.1 sobre v4.1.0

> Faça backup do `.env` antes. O patch **não inclui** seu `.env` real e não deve sobrescrevê-lo.

## 1. Extraia por cima da árvore v4.1.0

```bash
unzip crypto-monitor-v4.1.1-compose-fix.zip -d /tmp/crypto-v411-patch
cp -a /tmp/crypto-v411-patch/. /system/crypto-monitor-platform-v4.1.0/
cd /system/crypto-monitor-platform-v4.1.0
```

## 2. Corrija o `.env` existente

O seu `.env` antigo provavelmente ainda contém:

```env
LAKEKEEPER_VERSION=0.13.6
```

Altere para:

```env
LAKEKEEPER_VERSION=v0.13.6
COMPOSE_PARALLEL_LIMIT=1
```

O `.env.example` novo não altera seu `.env` automaticamente.

## 3. Valide e suba

```bash
make preflight
make pull
make build
make up
make status
make smoke
```

`make pull` e `make up` usam `scripts/compose-safe.sh`, que passa `--parallel 1` ao Docker Compose. `make up` também sobe com `--pull never`, evitando o segundo pull implícito.
