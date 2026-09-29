from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def test_markets_module_present():
    assert (ROOT / 'src/common/markets.py').is_file()

def test_analytics_datetime_hour_conversion_uses_datetime_function():
    text = (ROOT / 'src/analytics_api/data.py').read_text()
    assert 'toUnixTimestamp(toStartOfInterval(open_time, INTERVAL {iv})) * 1000 AS open_time_ms' in text
    assert 'toUnixTimestamp64Milli(toStartOfInterval(open_time' not in text

def test_clickhouse_healthcheck_expands_env_vars():
    text = (ROOT / 'docker-compose.yml').read_text()
    assert 'clickhouse-client --user "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD"' in text

def test_kafka_external_default_is_routable_name():
    env = (ROOT / '.env.example').read_text()
    assert 'KAFKA_EXTERNAL_HOST=localhost' in env

def test_lakekeeper_overlap_is_idempotent():
    text = (ROOT / 'scripts/lakekeeper-init.sh').read_text()
    assert 'CreateWarehouseStorageProfileOverlap' in text


def test_clickhouse_clients_are_not_shared_between_threads():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    src = (root / 'src/common/clickhouse.py').read_text()
    assert '@lru_cache' not in src
    assert 'c = client()' in src
    assert 'c.close()' in src
    candle = (root / 'src/candle_worker/service.py').read_text()
    recon = (root / 'src/reconcile/service.py').read_text()
    assert 'from src.common.clickhouse import insert, rows' in candle
    assert 'ch_insert(' in recon


def test_web_ui_history_cache_is_per_asset_and_uses_ingest_order():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    html = (root / 'src/web_ui/index.html').read_text()
    assert 'const historyCache=new Map()' in html
    assert 'key=`${venue}:${sym}:${days}`' in html
    assert 'time_ms||a.ingest_time_ms||a.event_time_ms' in html
    assert 'let lastQuotes=[]' in html
