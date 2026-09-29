# Troubleshooting

## `fatal error: concurrent map writes`

Sintoma típico:

```text
fatal error: concurrent map writes
github.com/docker/compose/v2/pkg/compose.(*composeService).pullRequiredImages.func1.1()
pull.go:328
```

O panic ocorre dentro do próprio Docker Compose durante pull concorrente de imagens. Não indica corrupção em Kafka, ClickHouse, Flink, Garage ou nos dados do projeto.

### Correção incorporada no projeto

Todos os comandos Compose passam por:

```text
scripts/compose-safe.sh
```

que executa:

```text
./scripts/compose-safe.sh --parallel ${COMPOSE_PARALLEL_LIMIT:-1} ...
```

Além disso, `make up` separa o pull do start:

```text
pull --ignore-buildable
up -d --no-build --pull never
```

Assim o `up` não dispara um segundo pull concorrente.

Use normalmente:

```bash
make up
```

ou, para baixar imagens antes:

```bash
make pull
```

Não é necessário usar `docker system prune` nem apagar `data/` para esse erro.

---

## `manifest for quay.io/lakekeeper/catalog:0.13.6 not found`

Lakekeeper publica os releases com prefixo `v` no tag de container.

Correto:

```env
LAKEKEEPER_VERSION=v0.13.6
```

Incorreto:

```env
LAKEKEEPER_VERSION=0.13.6
```

O Compose do projeto usa:

```text
quay.io/lakekeeper/catalog:${LAKEKEEPER_VERSION:-v0.13.6}
```

O `make preflight` também rejeita versões sem o `v`, evitando que a falha só apareça durante o pull.

Se você está atualizando uma instalação anterior, confira seu `.env`, porque ele não é substituído automaticamente por `.env.example`:

```bash
grep '^LAKEKEEPER_VERSION=' .env
```

Se mostrar `0.13.6`, altere para:

```env
LAKEKEEPER_VERSION=v0.13.6
```

Depois:

```bash
make pull
make up
```

---

## `No such image: postgres:17`

`postgres:17` é uma imagem válida. Este erro normalmente aparece como efeito cascata quando um pull anterior aborta antes de baixar PostgreSQL.

Depois de corrigir o tag Lakekeeper:

```bash
make pull
make up
```

Não troque PostgreSQL por outra versão apenas por causa desta mensagem.

---

## Senhas padrão com `BIND_ADDRESS=0.0.0.0`

O preflight bloqueia deliberadamente a exposição externa quando detecta segredos de exemplo.

Altere no `.env` os valores de:

```text
CLICKHOUSE_PASSWORD
CLICKHOUSE_GRAFANA_PASSWORD
GARAGE_RPC_SECRET
S3_ACCESS_KEY
S3_SECRET_KEY
LAKEKEEPER_PG_PASSWORD
LAKEKEEPER_ENCRYPTION_KEY
GRAFANA_ADMIN_PASSWORD
```

O `GARAGE_RPC_SECRET` deve ter exatamente 64 caracteres hexadecimais.

---

## `open-files limit is 1024`

É um aviso, não uma falha de startup. Para streaming contínuo é recomendável elevar o limite para 65535 ou mais.

Sessão atual:

```bash
ulimit -n 65535
```

Para persistência, configure os limites no systemd/PAM do host de acordo com a distribuição.

---

## Container `unhealthy`, `restarting` ou `exited`

Primeiro:

```bash
make status
```

Depois veja logs:

```bash
./scripts/compose-safe.sh logs --tail=200 NOME_DO_SERVICO
```

Para seguir os logs:

```bash
./scripts/compose-safe.sh logs -f NOME_DO_SERVICO
```

Não apague `data/` antes de identificar o serviço que falhou.

---

## Verificação depois do startup

```bash
make status
make smoke
```

`make smoke` verifica mais que healthchecks: ele testa o fluxo real até ClickHouse e tenta ler o raw truth pelo Iceberg/Lakekeeper/object storage.
