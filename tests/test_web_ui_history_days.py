from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_history_period_selector_and_direct_clickhouse_history():
    html = (ROOT / 'src/web_ui/index.html').read_text()
    main = (ROOT / 'src/web_ui/main.py').read_text()
    assert 'id="historyDays"' in html
    for value in ('1','3','7','14','30','60','90'):
        assert f'value="{value}"' in html
    assert 'days: int = Query(1, ge=1, le=90)' in main
    assert 'FROM market.candles_1m FINAL' in main
    assert 'FROM market.events_hot FINAL' in main
    assert 'await asyncio.to_thread(_ch_sync, query)' in main
    assert 'await asyncio.to_thread(_ch_sync, fallback)' in main
    assert "timeZone:'America/Sao_Paulo'" in html
