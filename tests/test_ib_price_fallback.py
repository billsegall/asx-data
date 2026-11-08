# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""/api/ib/price falls back to our own last close when IB has no price."""
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


def test_ib_down_uses_eod_close(client):
    with patch.object(api, '_ib_connect', side_effect=ConnectionRefusedError('gateway down')), \
         patch.object(api, '_latest_eod', return_value=('2026-11-06', 0.0040001)):
        rv = client.get('/api/ib/price/VR1O')
    assert rv.status_code == 200
    assert rv.get_json() == {'symbol': 'VR1O', 'price': 0.004, 'source': 'eod', 'date': '2026-11-06'}


def test_ib_down_and_no_eod_is_503(client):
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
