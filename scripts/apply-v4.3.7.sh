#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
from pathlib import Path
import yaml
p=Path('config/telegram.yaml')
if p.exists():
    d=yaml.safe_load(p.read_text()) or {}
    fw=d.setdefault('forward_alerts',{})
    fw.setdefault('enabled', True)
    sev=[str(x).lower() for x in fw.get('severities',[]) if str(x).strip()]
    for x in ('warning','critical'):
        if x not in sev: sev.append(x)
    fw['severities']=sev
    tmp=p.with_suffix('.yaml.tmp')
    tmp.write_text(yaml.safe_dump(d,sort_keys=False,allow_unicode=True))
    tmp.replace(p)
    print('telegram severities:', ','.join(sev))
else:
    print('config/telegram.yaml not found; skipped')
PY
