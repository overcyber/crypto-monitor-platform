# Segurança

## 1. Bind padrão

A configuração default usa:

```text
BIND_ADDRESS=127.0.0.1
```

Portanto APIs, ClickHouse HTTP, Grafana, S3 e Lakekeeper não são publicados para a rede externa por padrão.

## 2. Senhas

Troque todos os valores `change-me-*` antes de expor qualquer porta. O preflight falha se detectar senhas de exemplo com `BIND_ADDRESS` não-local.

## 3. Kafka

O compose single-host usa PLAINTEXT. Não exponha `9092` na Internet. Para acesso remoto real, configure TLS/SASL ou coloque Kafka em uma rede privada/VPN.

## 4. Lakekeeper

A entrega usa:

```text
LAKEKEEPER__AUTHZ_BACKEND=allowall
```

porque o catálogo fica bindado apenas em loopback e dentro da bridge Docker. Para ambiente multiusuário/remoto, configure autenticação/autorização suportada pelo Lakekeeper e TLS no proxy frontal.

## 5. Garage S3

As credenciais são passadas por environment. Para produção mais rígida, use secrets manager/Docker secrets em vez de `.env` legível por todos os usuários do host.

## 6. Grafana

A datasource utiliza `grafana_reader`, usuário ClickHouse read-only. Grafana não recebe as credenciais de escrita do app.

## 7. Permissões de diretório

`scripts/bootstrap-host.sh` usa `chmod a+rwx` para evitar incompatibilidade de UID entre containers em instalação single-host. Isso é uma conveniência de bootstrap, não a política ideal de um servidor multiusuário.

Em produção, substitua por ownership/ACL específicos aos UIDs efetivos dos containers e remova permissões globais de escrita.

## 8. APIs

As APIs não implementam autenticação application-level. Por isso devem permanecer em loopback/rede privada ou atrás de reverse proxy com TLS + autenticação.

## 9. Raw truth

O raw payload pode conter identificadores fornecidos pelas exchanges. Trate o bucket Iceberg como dado operacional sensível e limite leitura a administradores/serviços analíticos.
