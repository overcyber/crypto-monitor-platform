import unittest, yaml
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

class InfraTest(unittest.TestCase):
    def test_compose_services_and_host_mounts(self):
        d=yaml.safe_load((ROOT/'docker-compose.yml').read_text());sv=d['services']
        for name in ['kafka','flink-jobmanager','flink-taskmanager','garage','lakekeeper','clickhouse','grafana','monitor-api','analytics-api','reconciler','replay-api','candle-worker','replay-book-worker','alert-worker','telegram-bot','web-ui']:
            self.assertIn(name,sv)
        txt=(ROOT/'docker-compose.yml').read_text()
        for mount in ['${SRC_DIR:-./src}:/opt/app/src:ro','${CLICKHOUSE_DATA_DIR:-./data/clickhouse}:/var/lib/clickhouse','${GARAGE_DATA_DIR:-./data/garage/data}:/var/lib/garage/data','${KAFKA_DATA_DIR:-./data/kafka}:/var/lib/kafka/data','${FLINK_CHECKPOINT_DIR:-./data/flink/checkpoints}:/opt/flink/checkpoints']:
            self.assertIn(mount,txt)

    def test_python_images_do_not_copy_source(self):
        for name in ['python-core.Dockerfile','python-analytics.Dockerfile']:
            text=(ROOT/'docker'/name).read_text().lower();self.assertNotIn('copy src',text)

    def test_dockerignore_excludes_large_bind_data(self):
        text=(ROOT/'.dockerignore').read_text()
        self.assertIn('data/',text);self.assertIn('src/',text)

    def test_flink_truth_is_exactly_once_but_hot_is_low_latency(self):
        t=(ROOT/'infra/flink/jobs/01_streaming.sql.template').read_text()
        self.assertIn("'execution.checkpointing.mode' = 'EXACTLY_ONCE'",t)
        self.assertIn("'sink.delivery-guarantee' = 'at-least-once'",t)
        self.assertIn("'key.fields' = 'routing_key'",t)
        self.assertIn("'value.fields-include' = 'EXCEPT_KEY'",t)
        self.assertIn('lakehouse.market.raw_events',t)
        self.assertIn('EXECUTE STATEMENT SET',t)
        self.assertIn("'flink-maintenance.rewrite.enabled' = 'true'",t)
        self.assertIn("'flink-maintenance.expire-snapshots.enabled' = 'true'",t)
        self.assertIn("'flink-maintenance.delete-orphan-files.enabled' = 'true'",t)
        self.assertIn("'write.distribution-mode' = 'hash'",t)

    def test_clickhouse_hot_is_slim_and_reconciled(self):
        t=(ROOT/'infra/clickhouse/init/001_schema.sql').read_text()
        self.assertIn('ReplacingMergeTree',t);self.assertIn('reconciliation_results',t);self.assertIn('market.cleaned',t)
        hot=t.split('CREATE TABLE IF NOT EXISTS market.events_hot',1)[1].split('CREATE TABLE IF NOT EXISTS market.cleaned_kafka',1)[0]
        self.assertNotIn('raw_payload',hot)
        self.assertNotIn('bids_json',hot)
        self.assertIn('PARTITION BY toYYYYMMDD(event_time)',hot)

    def test_candles_are_materialized(self):
        schema=(ROOT/'infra/clickhouse/init/001_schema.sql').read_text()
        data=(ROOT/'src/analytics_api/data.py').read_text()
        compose=(ROOT/'docker-compose.yml').read_text()
        self.assertIn('market.candles_1m',schema)
        self.assertIn('FROM market.candles_1m FINAL',data)
        self.assertIn('candle-worker:',compose)

    def test_monitor_quote_and_candle_datetime_regressions(self):
        monitor=(ROOT/'src/monitor_api/main.py').read_text()
        candle=(ROOT/'src/candle_worker/service.py').read_text()
        analytics=(ROOT/'src/analytics_api/data.py').read_text()
        env=(ROOT/'.env.example').read_text()
        self.assertNotIn('max(event_time_ms) AS event_time_ms', monitor)
        self.assertIn('max(ingest_time_ms) AS latest_ingest_time_ms', monitor)
        self.assertIn('tuple(ingest_time_ms, event_time_ms)', monitor)
        self.assertIn('resolve_product', monitor)
        self.assertNotIn('toUnixTimestamp64Milli(toStartOfMinute(event_time))', candle)
        self.assertIn('toUnixTimestamp(toStartOfMinute(event_time)) * 1000', candle)
        self.assertIn('resolve_product', analytics)
        self.assertIn('DISPLAY_TIMEZONE=America/Sao_Paulo', env)

    def test_object_store_is_garage_and_catalog_is_external(self):
        compose=(ROOT/'docker-compose.yml').read_text()
        self.assertIn('dxflrs/garage:v2.4.1',compose)
        self.assertIn('--single-node',compose)
        self.assertIn('--default-access-key',compose)
        self.assertIn('GARAGE_DEFAULT_BUCKET',compose)
        self.assertNotIn('seaweedfs',compose.lower())
        self.assertNotIn('quay.io/minio/minio',compose)

    def test_lakekeeper_is_current_bugfix_release(self):
        compose=(ROOT/'docker-compose.yml').read_text()
        self.assertIn('LAKEKEEPER_VERSION:-v0.13.6',compose)


    def test_compose_wrapper_serializes_operations(self):
        wrapper=(ROOT/'scripts/compose-safe.sh').read_text()
        env=(ROOT/'.env.example').read_text()
        self.assertIn('COMPOSE_PARALLEL_LIMIT:-1', wrapper)
        self.assertIn('docker compose --parallel', wrapper)
        self.assertIn('COMPOSE_PARALLEL_LIMIT=1', env)

    def test_up_pulls_serially_then_never_repulled(self):
        up=(ROOT/'scripts/up.sh').read_text()
        self.assertIn('compose-safe.sh pull --ignore-buildable', up)
        self.assertIn('compose-safe.sh up -d --no-build --pull never', up)

    def test_lakekeeper_release_tag_includes_v_prefix(self):
        compose=(ROOT/'docker-compose.yml').read_text()
        env=(ROOT/'.env.example').read_text()
        preflight=(ROOT/'scripts/preflight.sh').read_text()
        self.assertIn('LAKEKEEPER_VERSION:-v0.13.6', compose)
        self.assertIn('LAKEKEEPER_VERSION=v0.13.6', env)
        self.assertIn('missing the required leading v', preflight)

    def test_alerts_are_persisted_and_config_is_host_mounted(self):
        schema=(ROOT/'infra/clickhouse/init/001_schema.sql').read_text()
        compose=(ROOT/'docker-compose.yml').read_text()
        self.assertIn('CREATE TABLE IF NOT EXISTS market.alerts',schema)
        self.assertIn('market.alerts',schema)
        self.assertIn('./config:/opt/app/config:ro',compose)


    def test_telegram_bot_is_host_configurable_and_persistent(self):
        compose=(ROOT/'docker-compose.yml').read_text()
        env=(ROOT/'.env.example').read_text()
        self.assertIn('telegram-bot:', compose)
        self.assertIn('./config:/opt/app/config:rw', compose)
        self.assertIn('${TELEGRAM_DATA_DIR:-./data/telegram}:/opt/app/data/telegram', compose)
        self.assertIn('TELEGRAM_ENABLED=false', env)
        self.assertIn('TELEGRAM_ALLOWED_CHAT_IDS=', env)

    def test_clickhouse_healthcheck_expands_credentials(self):
        compose=(ROOT/'docker-compose.yml').read_text()
        self.assertIn('clickhouse-client --user "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD"', compose)
        self.assertIn('./infra/clickhouse/users.d:/etc/clickhouse-server/users.d:rw', compose)

    def test_lakekeeper_init_is_idempotent(self):
        compose=(ROOT/'docker-compose.yml').read_text()
        script=(ROOT/'scripts/lakekeeper-init.sh').read_text()
        self.assertIn('CatalogAlreadyBootstrapped', compose)
        self.assertIn('CreateWarehouseStorageProfileOverlap', script)

    def test_grafana_uses_readonly_clickhouse_user(self):
        ds=(ROOT/'infra/grafana/provisioning/datasources/clickhouse.yml').read_text()
        users=(ROOT/'infra/clickhouse/users.d/market.xml').read_text()
        self.assertIn('username: grafana_reader',ds)
        self.assertIn('<readonly>1</readonly>',users)

class DurabilityAndReplayTest(unittest.TestCase):
    def test_clickhouse_retention_is_layer_specific(self):
        text=(ROOT/'scripts/clickhouse-retention.sh').read_text()
        self.assertIn('CLICKHOUSE_HOT_RETENTION_DAYS', text)
        self.assertIn('CLICKHOUSE_CANDLE_RETENTION_DAYS', text)
        self.assertIn('CLICKHOUSE_ALERT_RETENTION_DAYS', text)
        self.assertIn('CLICKHOUSE_RECON_RETENTION_DAYS', text)
        self.assertNotEqual(text.find('CLICKHOUSE_HOT_RETENTION_DAYS'), text.find('CLICKHOUSE_CANDLE_RETENTION_DAYS'))

    def test_reconciliation_is_batched_disk_staged_and_non_destructive(self):
        text=(ROOT/'src/reconcile/service.py').read_text()
        self.assertIn('to_arrow_batch_reader', text)
        self.assertIn('sqlite3.connect', text)
        self.assertIn('uniqExact(event_id)', text)
        self.assertIn('market.events_hot FINAL', text)
        self.assertNotIn('ALTER TABLE market.events_hot DELETE', text)
        self.assertNotIn('TRUNCATE TABLE', text)

    def test_replay_is_persistent_and_isolated(self):
        replay=(ROOT/'src/replay/main.py').read_text()
        compose=(ROOT/'docker-compose.yml').read_text()
        self.assertIn('PRAGMA journal_mode=WAL', replay)
        self.assertIn('market.replay', replay)
        self.assertIn('event_time_ms,ingest_time_ms,event_id,seq', replay.replace(' ',''))
        self.assertIn('replay-book-worker:', compose)
        self.assertIn('${REPLAY_DATA_DIR:-./data/replay}:/opt/app/data/replay', compose)

    def test_garage_config_is_host_rendered_not_baked_with_secret(self):
        tpl=(ROOT/'infra/garage/garage.toml.template').read_text()
        bootstrap=(ROOT/'scripts/bootstrap-host.sh').read_text()
        compose=(ROOT/'docker-compose.yml').read_text()
        self.assertIn('__GARAGE_RPC_SECRET__', tpl)
        self.assertIn('GARAGE_RPC_SECRET', bootstrap)
        self.assertIn('${GARAGE_CONFIG_FILE:-./data/garage/config/garage.toml}:/etc/garage.toml:ro', compose)
        self.assertNotRegex(tpl, r'rpc_secret\s*=\s*"[0-9a-fA-F]{64}"')
