# Fontes técnicas verificadas para a v4

Verificação realizada em 29/09/2026. O objetivo desta lista é registrar as decisões que dependem de comportamento/versão externa.

## Streaming e storage

- Apache Kafka 4.3.x: https://kafka.apache.org/downloads
- Apache Flink 1.20: https://nightlies.apache.org/flink/flink-docs-release-1.20/
- Apache Iceberg Flink: https://iceberg.apache.org/docs/latest/flink/
- Apache Iceberg Flink writes/Sink V2: https://iceberg.apache.org/docs/latest/flink-writes/
- Apache Iceberg maintenance: https://iceberg.apache.org/docs/latest/flink-maintenance/
- Apache Iceberg S3 FileIO: https://iceberg.apache.org/docs/latest/aws/
- Lakekeeper engines/REST catalog: https://docs.lakekeeper.io/docs/latest/engines/
- Lakekeeper S3-compatible storage: https://docs.lakekeeper.io/docs/latest/storage/
- Garage documentation: https://garagehq.deuxfleurs.fr/documentation/
- Garage S3 compatibility: https://github.com/deuxfleurs-org/garage/blob/main-v2/doc/book/reference-manual/s3-compatibility.md
- Garage single-node/default bucket: https://github.com/deuxfleurs-org/garage/blob/main-v2/doc/book/quick-start/_index.md

## Por que Garage e não SeaweedFS 4.47 como default

SeaweedFS continua tecnicamente interessante e a release 4.47 é recente, mas durante a revisão final foi encontrado um bug report reproduzido em 4.47 no qual metadata do filer podia apontar para um chunk tombstoned e continuar aparentemente legível até vacuum, criando risco de perda silenciosa tardia:

- https://github.com/seaweedfs/seaweedfs/issues/11366

O issue foi fechado em correção posterior, mas 4.47 continuava sendo a release publicada que havia sido verificada durante esta entrega. Para uma camada designada **raw truth**, a v4 prefere não depender dessa release.

Garage é S3-compatible, está ativo, é usado em produção pelo próprio projeto desde 2020 e documenta explicitamente deployment versionado em Docker. A v4 usa `dxflrs/garage:v2.4.1`.

## Exchanges

- Binance Spot WebSocket streams / Diff Depth: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
- Binance REST depth snapshot: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
- Coinbase Exchange WebSocket channels: https://docs.cdp.coinbase.com/exchange/websocket-feed/channels

## Serving/visualização

- ClickHouse: https://clickhouse.com/docs
- Grafana ClickHouse datasource: https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/

## Observação

As URLs acima são documentação/referências externas. O pacote é deliberadamente pinado em versões no `docker-compose.yml`/Dockerfiles para evitar que `latest` altere o comportamento silenciosamente.
