#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

python3 -m compileall -q src tests
python3 -m unittest discover -s tests -v

python3 - <<'PY'
import pathlib, yaml
p = pathlib.Path('docker-compose.yml')
yaml.safe_load(p.read_text())
print('docker-compose.yml YAML: OK')
PY

python3 - <<'PY'
from xml.etree import ElementTree as ET
for f in ['infra/clickhouse/config.d/market.xml','infra/clickhouse/users.d/market.xml']:
    ET.parse(f)
print('ClickHouse XML: OK')
PY

python3 -m src.selftest.main

for f in scripts/*.sh; do bash -n "$f"; done
echo 'Shell scripts syntax: OK'

python3 - <<'PY'
import json, pathlib, yaml
json.loads(pathlib.Path('infra/grafana/dashboards/market-overview.json').read_text())
yaml.safe_load(pathlib.Path('infra/grafana/provisioning/dashboards/default.yml').read_text())
yaml.safe_load(pathlib.Path('infra/grafana/provisioning/datasources/clickhouse.yml').read_text())
yaml.safe_load(pathlib.Path('config/alerts.yaml').read_text())
yaml.safe_load(pathlib.Path('config/markets.yaml').read_text())
yaml.safe_load(pathlib.Path('config/telegram.yaml').read_text())
print('Grafana JSON/YAML + alert rules + market/telegram YAML: OK')
PY

python3 - <<'PY'
from pathlib import Path
compose=Path('docker-compose.yml').read_text()
for forbidden in ('/opt/app/data/clickhouse', '/opt/app/data/kafka'):
    if forbidden in compose:
        raise SystemExit(f'unexpected container-only persistent path: {forbidden}')
if 'COPY src' in Path('docker/python-core.Dockerfile').read_text() or 'COPY src' in Path('docker/python-analytics.Dockerfile').read_text():
    raise SystemExit('Python source was baked into an image')
print('Host-mount/no-source-bake invariants: OK')
PY
