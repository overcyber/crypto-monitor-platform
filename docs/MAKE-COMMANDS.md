# Comandos `make`

Todos os comandos Docker Compose do projeto passam por `scripts/compose-safe.sh`.
Por padrão, o wrapper usa `--parallel 1` para evitar o panic conhecido de algumas versões do Docker Compose durante pulls concorrentes (`fatal error: concurrent map writes`).

A variável pode ser alterada no `.env`:

```env
COMPOSE_PARALLEL_LIMIT=1
```

## Primeiro deploy

```bash
cp .env.example .env
nano .env
make bootstrap
make preflight
make build
make up
make smoke
```

`make up` executa, nesta ordem:

1. `bootstrap-host.sh`;
2. `preflight.sh`;
3. `./scripts/compose-safe.sh --parallel 1 pull --ignore-buildable`;
4. `./scripts/compose-safe.sh --parallel 1 up -d --no-build --pull never`.

O passo 3 baixa somente imagens de registry. O passo 4 não tenta puxar novamente nem reconstruir as imagens Python/Flink locais.

## Comandos disponíveis

| Comando | Função |
|---|---|
| `make bootstrap` | prepara diretórios bind-mounted do host e renderiza configuração Garage |
| `make preflight` | valida Docker/Compose, caminhos, segredos e formato do tag Lakekeeper |
| `make pull` | baixa imagens externas de forma serial |
| `make build` | constrói as imagens locais do projeto com Compose serializado |
| `make up` | bootstrap + preflight + pull serial + `up --pull never` |
| `make up-monitor` | inicia apenas o pipeline leve: monitor de preços + alertas do Telegram |
| `make down` | para/remover containers e rede; não apaga os diretórios `data/` |
| `make reload` | reinicia apenas microserviços Python que usam código bind-mounted |
| `make reload-flink` | reinicia/submete novamente o job Flink usando o SQL montado do host |
| `make status` | executa `./scripts/compose-safe.sh ps` pelo wrapper seguro |
| `make storage` | mostra uso dos diretórios persistentes |
| `make test` | executa os testes Python locais |
| `make docker-test` | executa testes dentro do profile Docker `test` |
| `make validate` | valida Python, testes, YAML, XML, Grafana, shell scripts e invariantes |
| `make smoke` | valida o pipeline vivo WebSocket -> Kafka -> Flink -> ClickHouse/Iceberg |
| `make backup-catalog` | faz dump compactado do PostgreSQL do Lakekeeper |
| `make logs` | segue logs de todos os serviços |

## Alterar código sem rebuild

O código Python é bind-mounted de `./src` dentro dos containers. Depois de editar o código:

```bash
make reload
```

Não use `make build` para alterações comuns em `src/`.

Rebuild só é necessário quando mudam:

- Dockerfiles;
- `requirements/*.txt`;
- pacotes do sistema operacional instalados nas imagens;
- artefatos Java/JAR do runtime Flink.

## Alterar criptos monitoradas

Edite:

```text
config/markets.yaml
```

Os ingestors fazem hot-reload da seleção. Não é necessário chamar API, rebuildar imagem ou reiniciar a plataforma inteira.
