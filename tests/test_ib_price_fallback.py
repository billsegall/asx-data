# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""/api/ib/price: official close outside trading hours, and our own last
close as a fallback when IB has no price."""
import datetime
import os
import sys
from unittest.mock import patch

import pytest

ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.join(ROOT, 'stockdb'))
sys.path.insert(0, os.path.join(ROOT, 'backend'))
# api.py opens the DB at import; its default path is relative to backend/
os.environ.setdefault('DATABASE', os.path.abspath(os.path.join(ROOT, 'stockdb', 'stockdb.db')))
if not os.path.exists(os.environ['DATABASE']):
    pytest.skip('stockdb.db not present', allow_module_level=True)
api = pytest.importorskip('api')


@pytest.fixture
def client(monkeypatch):
    monkeypatch.delenv('IB_API_KEY', raising=False)
    return api.app.test_client()


@pytest.fixture
def market_open(monkeypatch):
    monkeypatch.setattr(api, '_asx_session_open', lambda now=None: True)


def syd(iso):
    from zoneinfo import ZoneInfo
    return datetime.datetime.fromisoformat(iso).replace(tzinfo=ZoneInfo('Australia/Sydney'))


def test_ib_down_uses_eod_close(client, market_open):
    with patch.object(api, '_ib_connect', side_effect=ConnectionRefusedError('gateway down')), \
         patch.object(api, '_latest_eod', return_value=('2026-11-06', 0.0040001)):
        rv = client.get('/api/ib/price/VR1O')
    assert rv.status_code == 200
    assert rv.get_json() == {'symbol': 'VR1O', 'price': 0.004, 'source': 'eod', 'date': '2026-11-06'}


def test_ib_down_and_no_eod_is_503(client, market_open):
    with patch.object(api, '_ib_connect', side_effect=ConnectionRefusedError('gateway down')), \
         patch.object(api, '_latest_eod', return_value=None):
        rv = client.get('/api/ib/price/NOPE9')
    assert rv.status_code == 503
    assert rv.get_json() == {'error': 'gateway down'}


def test_eod_endpoint_uses_shared_lookup(client):
    with patch.object(api, '_latest_eod', return_value=('2026-11-06', 202.04)):
        rv = client.get('/api/eod/CSL')
    assert rv.get_json() == {'symbol': 'CSL', 'date': '2026-11-06', 'close': 202.04}
    with patch.object(api, '_latest_eod', return_value=None):
        assert client.get('/api/eod/NOPE9').status_code == 404


@pytest.mark.parametrize('now, expected', [
    ('2026-11-08 19:30', '2026-11-06'),  # Sunday -> Friday
    ('2026-11-09 09:59', '2026-11-06'),  # Monday pre-open -> Friday
    ('2026-11-09 16:14', '2026-11-06'),  # Monday before the auction is final
    ('2026-11-09 16:15', '2026-11-09'),  # Monday after the close
    ('2026-12-29 08:00', '2026-12-24'),  # skips Christmas + Boxing Day substitute
    ('2027-01-01 12:00', '2026-12-31'),  # New Year's Day holiday
])
def test_last_completed_session(now, expected):
    assert api._last_completed_session(syd(now)).isoformat() == expected


@pytest.mark.parametrize('now, is_open', [
    ('2026-11-09 09:59', False),
    ('2026-11-09 10:00', True),
    ('2026-11-09 16:14', True),
    ('2026-11-09 16:15', False),
    ('2026-11-08 12:00', False),  # Sunday
    ('2026-12-25 12:00', False),  # holiday
])
def test_session_open(now, is_open):
    assert api._asx_session_open(syd(now)) is is_open


def test_closed_market_uses_official_close_without_ib(client, monkeypatch):
    monkeypatch.setattr(api, '_asx_session_open', lambda now=None: False)
    monkeypatch.setattr(api, '_last_completed_session', lambda now=None: datetime.date(2026, 11, 6))
    with patch.object(api, '_ib_connect') as conn, \
         patch.object(api, '_latest_eod', return_value=('2026-11-06', 202.04)):
        rv = client.get('/api/ib/price/CSL')
    conn.assert_not_called()
    assert rv.get_json() == {'symbol': 'CSL', 'price': 202.04, 'source': 'eod', 'date': '2026-11-06'}


def test_closed_market_before_eod_load_asks_ib(client, monkeypatch):
    """16:15 until the 17:00 EOD load: today's close isn't in the table yet,
    so IB's last trade is the newer price."""
    monkeypatch.setattr(api, '_asx_session_open', lambda now=None: False)
    monkeypatch.setattr(api, '_last_completed_session', lambda now=None: datetime.date(2026, 11, 9))
    with patch.object(api, '_ib_connect', side_effect=ConnectionRefusedError('down')) as conn, \
         patch.object(api, '_latest_eod', return_value=('2026-11-06', 202.04)):
        client.get('/api/ib/price/CSL')
    conn.assert_called_once()


class TestKey:
    """X-API-Key gate on /api/ib/price and /api/eod when IB_API_KEY is set."""

    @pytest.fixture
    def keyed(self, monkeypatch):
        monkeypatch.setenv('IB_API_KEY', 's3cret')
        return api.app.test_client()

    @pytest.mark.parametrize('path', ['/api/eod/CSL', '/api/ib/price/CSL'])
    def test_missing_wrong_and_non_ascii_rejected(self, keyed, path):
        assert keyed.get(path).status_code == 401
        assert keyed.get(path, headers={'X-API-Key': 'wrong'}).status_code == 401
        assert keyed.get(path, headers={'X-API-Key': 's3cr\xe9t'}).status_code == 401

    def test_query_key_no_longer_accepted(self, keyed):
        assert keyed.get('/api/eod/CSL?key=s3cret').status_code == 401

    def test_right_key_accepted(self, keyed):
        with patch.object(api, '_latest_eod', return_value=('2026-11-06', 12.5)):
            assert keyed.get('/api/eod/CSL', headers={'X-API-Key': 's3cret'}).status_code == 200
