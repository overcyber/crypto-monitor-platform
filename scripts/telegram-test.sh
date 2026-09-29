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
CHATS="$(read_env TELEGRAM_ALLOWED_CHAT_IDS)"
[[ -n "$TOKEN" ]] || { echo 'FAIL TELEGRAM_BOT_TOKEN empty'; exit 1; }
[[ -n "$CHATS" ]] || { echo 'FAIL TELEGRAM_ALLOWED_CHAT_IDS empty'; exit 1; }
BASE="https://api.telegram.org/bot${TOKEN}"

GETME="$(curl -sS "${BASE}/getMe")"
printf '%s' "$GETME" | python3 -c '
import json,sys
d=json.load(sys.stdin)
if not d.get("ok"):
    print("FAIL Telegram getMe: {}".format(d.get("description", d)))
    raise SystemExit(1)
print("PASS Telegram getMe @{}".format(d.get("result",{}).get("username","?")))
'

IFS=',' read -ra IDS <<<"$CHATS"
for raw in "${IDS[@]}"; do
  id="$(echo "$raw" | xargs)"; [[ -n "$id" ]] || continue
  RESP="$(curl -sS -X POST "${BASE}/sendMessage" --data-urlencode "chat_id=${id}" --data-urlencode "text=Crypto Monitor: teste Telegram OK")"
  printf '%s' "$RESP" | python3 -c '
import json,sys
d=json.load(sys.stdin)
if not d.get("ok"):
    print("FAIL Telegram sendMessage: {}".format(d.get("description", d)))
    raise SystemExit(1)
'
  echo "PASS test message chat_id=${id}"
done
