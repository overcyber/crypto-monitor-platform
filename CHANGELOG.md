# v4.3.7

- Telegram: preserves existing price rules and adds `warning` + `critical` to forwarded system severities.
- Reconciliation: window uses `ingest_time_ms` on both Iceberg and ClickHouse.
- Reconciliation: every run writes a `reconcile_run` heartbeat, including empty windows.
