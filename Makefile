.PHONY: init-data bootstrap preflight pull build up up-monitor down reload reload-flink status storage test docker-test validate smoke backup-catalog logs telegram-logs telegram-test telegram-discover telegram-restart web-logs clean-pyc

init-data:
	mkdir -p data/kafka data/clickhouse data/clickhouse-logs data/garage/meta data/garage/data data/garage/config data/lakekeeper-postgres data/flink/checkpoints data/flink/savepoints data/grafana data/reconcile data/replay data/telegram
	chmod -R a+rwx data 2>/dev/null || true

bootstrap: init-data
	./scripts/bootstrap-host.sh

preflight:
	./scripts/preflight.sh

pull:
	./scripts/pull.sh

build:
	./scripts/build.sh

up:
	./scripts/up.sh

up-monitor:
	./scripts/up-monitor.sh

down:
	./scripts/down.sh

reload:
	./scripts/reload-code.sh

reload-flink:
	./scripts/reload-flink-job.sh

status:
	./scripts/status.sh

storage:
	./scripts/storage-report.sh

test:
	python3 -m unittest discover -s tests -v

docker-test:
	./scripts/compose-safe.sh --profile test run --rm tests

validate:
	./scripts/validate.sh

smoke:
	./scripts/live-smoke-test.sh

backup-catalog:
	./scripts/backup-catalog.sh

logs:
	./scripts/compose-safe.sh logs -f --tail=200

telegram-logs:
	./scripts/compose-safe.sh logs -f --tail=200 telegram-bot

telegram-test:
	./scripts/telegram-test.sh

telegram-discover:
	./scripts/telegram-discover.sh

telegram-restart:
	./scripts/telegram-restart.sh

web-logs:
	./scripts/compose-safe.sh logs -f --tail=200 web-ui

clean-pyc:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
