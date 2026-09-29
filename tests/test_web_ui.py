import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class WebUiTest(unittest.TestCase):
    def test_web_ui_service_and_port(self):
        compose = (ROOT / 'docker-compose.yml').read_text()
        env = (ROOT / '.env.example').read_text()
        self.assertIn('web-ui:', compose)
        self.assertIn('src.web_ui.main:app', compose)
        self.assertIn('${WEB_UI_PORT:-8090}:8090', compose)
        self.assertIn('WEB_UI_PORT=8090', env)

    def test_web_ui_exposes_required_views(self):
        main = (ROOT / 'src/web_ui/main.py').read_text()
        html = (ROOT / 'src/web_ui/index.html').read_text()
        for route in ['/api/system', '/api/quotes', '/api/analyze/{symbol}', '/api/alerts', '/api/reconciliation', '/api/data-distribution']:
            self.assertIn(route, main)
        for label in ['Mercado', 'Analytics', 'Pipeline', 'Alertas', 'Configuração']:
            self.assertIn(label, html)
        self.assertIn('WEB_UI_SYSTEM_CACHE_SECONDS', main)

    def test_telegram_operational_scripts_exist(self):
        make = (ROOT / 'Makefile').read_text()
        service = (ROOT / 'src/telegram_bot/service.py').read_text()
        for target in ['telegram-test:', 'telegram-discover:', 'telegram-restart:', 'web-logs:']:
            self.assertIn(target, make)
        self.assertIn('/price BTC', service)
        self.assertIn('cmd == "/test"', service)
        self.assertIn('cmd == "/price"', service)


if __name__ == '__main__':
    unittest.main()

class WebUiRegimeDefaultsTest(unittest.TestCase):
    def test_web_ui_defaults_to_one_minute_and_shows_regime_coverage(self):
        root=Path(__file__).resolve().parents[1]
        html=(root/'src/web_ui/index.html').read_text()
        self.assertIn('<option selected>1m</option>', html)
        self.assertIn('available_candles', html)
        self.assertIn('required_candles', html)

class WebUiConfigWriteTest(unittest.TestCase):
    def test_clickhouse_queries_are_serialized_and_config_is_writable(self):
        root = Path(__file__).resolve().parents[1]
        main = (root / 'src/web_ui/main.py').read_text()
        compose = (root / 'docker-compose.yml').read_text()
        env = (root / '.env.example').read_text()
        self.assertIn('def _ch_sync(query: str)', main)
        self.assertIn('clickhouse_connect.get_client(', main)
        self.assertIn('client.close()', main)
        self.assertIn('./config:/opt/app/config:rw', compose)
        self.assertIn('WEB_UI_CONFIG_WRITE_ENABLED=true', env)
        self.assertIn('WEB_UI_ADMIN_TOKEN=', env)
        self.assertIn('ADMIN_TOKEN_REQUIRED', main)

    def test_configuration_crud_routes_and_editors_exist(self):
        root = Path(__file__).resolve().parents[1]
        main = (root / 'src/web_ui/main.py').read_text()
        html = (root / 'src/web_ui/index.html').read_text()
        for route in ['/api/config/markets', '/api/config/alerts', '/api/config/telegram']:
            self.assertIn(route, main)
        for label in ['Watchlist', 'Regras de alerta do sistema', 'Alertas de preço / percentual', 'Salvar mercados', 'Salvar alertas', 'Salvar Telegram']:
            self.assertIn(label, html)
        self.assertIn('.web-ui-backups', main)

    def test_distribution_uses_brasilia_human_timestamps(self):
        root = Path(__file__).resolve().parents[1]
        main = (root / 'src/web_ui/main.py').read_text()
        self.assertIn("America/Sao_Paulo", main)
        self.assertIn('latest_event_time', main)


class WebUiStabilityTest(unittest.TestCase):
    def test_asset_is_select_and_chart_preserves_valid_history(self):
        root = Path(__file__).resolve().parents[1]
        html = (root / 'src/web_ui/index.html').read_text()
        self.assertIn('<select id="symbol">', html)
        self.assertIn('populateAssets', html)
        self.assertTrue('lastHistory' in html or 'historyCache' in html)
        self.assertIn('Mantendo último gráfico válido', html)
        self.assertIn('setInterval(refreshSystem,30000)', html)
        self.assertIn('setInterval(refreshAlerts,15000)', html)

    def test_clickhouse_ui_uses_independent_sessions(self):
        root = Path(__file__).resolve().parents[1]
        main = (root / 'src/web_ui/main.py').read_text()
        self.assertNotIn('from src.common.clickhouse import rows', main)
        self.assertIn('def _ch_sync(query: str)', main)
        self.assertIn('client.close()', main)
        self.assertIn('_cache_locks', main)
