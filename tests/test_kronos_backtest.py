# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Kronos backtest: date grid, horizons and resume."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from analysis.cli import run_kronos_backtest as bt

GRID = ['2026-03-02', '2026-03-09', '2026-03-16']


def _row(date, fc=0.01):
    return {'date': date, 'ic': 0.1, 'n_forecast': 2, 'n_actual': 2,
            'forecasts': {'AAA': fc, 'BBB': -fc}, 'actual': {'AAA': 0.05, 'BBB': -0.02}}


def _stub(monkeypatch, computed):
    monkeypatch.setattr(bt, 'get_evaluation_dates', lambda *a, **k: GRID)
    monkeypatch.setattr(bt, 'load_all_ohlcv', lambda db: {'AAA': None, 'BBB': None})
    monkeypatch.setattr(bt, 'build_predictor', lambda *a, **k: None)

    def forecast(pred, ohlcv, date, lookback, pred_len):
        computed.append((date, pred_len))
        return {'AAA': 0.02, 'BBB': -0.01}
    monkeypatch.setattr(bt, 'forecast_5d_returns', forecast)
    monkeypatch.setattr(bt, 'get_actual_5d_returns',
                        lambda db, date, syms, horizon: {'AAA': 0.1, 'BBB': 0.0})


def test_output_file_per_horizon():
    assert bt.output_filename(5) == 'backtest_kronos.json'
    assert bt.output_filename(20) == 'backtest_kronos_20d.json'


def test_off_grid_cached_rows_dropped_and_sorted(tmp_path, monkeypatch):
    # Cache from an older sampling: one off-grid date overlapping the grid's
    # holding periods, rows out of order.
    cached = [_row('2026-03-16'), _row('2026-03-04'), _row('2026-03-02'), _row('2026-03-09')]
    (tmp_path / 'backtest_kronos.json').write_text(
        json.dumps({'params': {'pred_len': 5}, 'dates': cached}))
    computed = []
    _stub(monkeypatch, computed)
    result = bt.run_backtest('db', 'm', 't', output_dir=str(tmp_path))
    assert computed == []
    assert [r['date'] for r in result['dates']] == GRID
    assert [p['date'] for p in result['portfolios']['top1']['per_date']] == GRID
    assert result['params']['step_days'] == 5 and result['params']['pred_len'] == 5


def test_other_horizon_never_reuses_5day_cache(tmp_path, monkeypatch):
    # Even if a 10-day file somehow held 5-day rows, they are not reused.
    (tmp_path / 'backtest_kronos_10d.json').write_text(
        json.dumps({'params': {'pred_len': 5}, 'dates': [_row(d) for d in GRID]}))
    (tmp_path / 'backtest_kronos.json').write_text('{"sentinel": 1}')
    computed = []
    _stub(monkeypatch, computed)
    result = bt.run_backtest('db', 'm', 't', output_dir=str(tmp_path), pred_len=10)
    assert computed == [(d, 10) for d in GRID]
    assert result['params']['pred_len'] == 10 and result['params']['step_days'] == 10
    # The 5-day file the web page reads is untouched
    assert json.loads((tmp_path / 'backtest_kronos.json').read_text()) == {'sentinel': 1}
    # Resuming the same horizon recomputes nothing
    computed.clear()
    bt.run_backtest('db', 'm', 't', output_dir=str(tmp_path), pred_len=10)
    assert computed == []
