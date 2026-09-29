#!/usr/bin/env bash
set -euo pipefail
[[ -f .env ]] || { echo 'FAIL .env not found'; exit 1; }

read_env() {
  python3 - "$1" <<'PY'
import sys
key=sys.argv[1]
for raw in open('.env', encoding='utf-8-sig'):
    line=raw.strip()
    if not line or line.startswith('#') or '=' not in line:
        continue
    k,v=line.split('=',1)
    if k.strip()!=key:
        continue
    v=v.strip().rstrip('\r')
    if len(v)>=2 and v[0]==v[-1] and v[0] in "\"'":
        v=v[1:-1]
    print(v.strip())
    break
PY
}

TOKEN="$(read_env TELEGRAM_BOT_TOKEN)"
[[ -n "$TOKEN" ]] || { echo 'FAIL TELEGRAM_BOT_TOKEN empty'; exit 1; }
BASE="https://api.telegram.org/bot${TOKEN}"

echo 'Validando token Telegram...'
GETME="$(curl -sS "${BASE}/getMe")"
printf '%s' "$GETME" | python3 -c '
import json,sys
d=json.load(sys.stdin)
if not d.get("ok"):
    print("FAIL Telegram getMe: {}".format(d.get("description", d)))
    raise SystemExit(1)
r=d.get("result",{})
print("PASS Telegram getMe @{}".format(r.get("username","?")))
'

echo 'Envie /start para o bot antes deste comando. Se telegram-bot estiver rodando, pare-o temporariamente.'
UPDATES="$(curl -sS "${BASE}/getUpdates")"
printf '%s' "$UPDATES" | python3 -c '
import json,sys
x=json.load(sys.stdin)
if not x.get("ok"):
    print("FAIL Telegram getUpdates: {}".format(x.get("description", x)))
    raise SystemExit(1)
seen=set()
for u in x.get("result",[]):
    m=u.get("message") or u.get("edited_message") or {}
    c=m.get("chat") or {}
    cid=c.get("id")
    if cid is not None and cid not in seen:
        seen.add(cid)
        name=c.get("title") or c.get("username") or c.get("first_name") or "?"
        print("chat_id={} type={} name={}".format(cid,c.get("type","?"),name))
if not seen:
    print("Nenhum chat encontrado; envie /start ao bot e tente novamente.")
'
