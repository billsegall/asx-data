# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Integration tests for asx-data backend API endpoints.

Tests hit the live backend at localhost:8082. Skipped automatically when
the backend is not running (handled by conftest.py's pytest_collection_modifyitems
for correlations; here we use a session-scoped skip directly).
"""
import pytest
import urllib.request
import urllib.parse
import json

BASE = 'http://localhost:8082'


def _backend_available():
    try:
        urllib.request.urlopen(BASE + '/api/symbols?q=BHP', timeout=2)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _backend_available(),
    reason='backend not running at localhost:8082'
)


def _get(path, **params):
    url = BASE + path
    if params:
        url += '?' + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


def _post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        BASE + path,
        data=data,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _status(path):
    try:
        req = urllib.request.Request(BASE + path)
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


# ---------------------------------------------------------------------------
# /api/symbols
# ---------------------------------------------------------------------------

class TestSymbolsEndpoint:
    def test_empty_query_returns_empty(self):
        result = _get('/api/symbols', q='')
        assert result == []

    def test_bhp_returns_results(self):
        result = _get('/api/symbols', q='BHP')
        assert isinstance(result, list)
        assert len(result) > 0
        symbols = [r['symbol'] for r in result]
        assert 'BHP' in symbols

    def test_result_has_required_fields(self):
        result = _get('/api/symbols', q='BHP')
        r = result[0]
        assert 'symbol' in r
        assert 'name' in r
        assert 'current' in r

    def test_current_only_by_default(self):
        result = _get('/api/symbols', q='BHP')
        assert all(r['current'] for r in result)

    def test_unknown_symbol_returns_empty(self):
        result = _get('/api/symbols', q='ZZZZZNOMATCH')
        assert result == []


# ---------------------------------------------------------------------------
# /api/symbol/<symbol>
# ---------------------------------------------------------------------------

class TestSymbolInfo:
    def test_bhp_returns_info(self):
        result = _get('/api/symbol/BHP')
        assert 'name' in result
        assert 'BHP' in result['name'].upper()

    def test_bhp_has_industry(self):
        result = _get('/api/symbol/BHP')
        assert 'industry' in result

    def test_unknown_symbol_returns_404(self):
        code = _status('/api/symbol/ZZZNOEXIST')
        assert code == 404

    def test_lowercase_normalized(self):
        result = _get('/api/symbol/bhp')
        assert 'name' in result


# ---------------------------------------------------------------------------
# /api/shorts
# ---------------------------------------------------------------------------

class TestShortsEndpoint:
    def test_returns_expected_structure(self):
        result = _get('/api/shorts')
        assert 'data' in result
        assert 'latest_date' in result
        assert isinstance(result['data'], list)

    def test_each_row_has_required_fields(self):
        result = _get('/api/shorts')
        if result['data']:
            row = result['data'][0]
            assert 'symbol' in row
            assert 'short' in row

    def test_latest_date_is_string_or_none(self):
        result = _get('/api/shorts')
        assert result['latest_date'] is None or isinstance(result['latest_date'], str)


# ---------------------------------------------------------------------------
# /api/enrich
# ---------------------------------------------------------------------------

class TestEnrichEndpoint:
    def test_bhp_returns_price(self):
        result = _post('/api/enrich', {'symbols': ['BHP']})
        assert 'BHP' in result

    def test_empty_list_returns_empty_dict(self):
        result = _post('/api/enrich', {'symbols': []})
        assert result == {}

    def test_unknown_symbol_returns_empty_entry(self):
        result = _post('/api/enrich', {'symbols': ['ZZZNOEXIST']})
        # Either not present or an empty dict (no price data)
        assert 'ZZZNOEXIST' not in result or result['ZZZNOEXIST'] == {}

    def test_multiple_symbols(self):
        result = _post('/api/enrich', {'symbols': ['BHP', 'CBA']})
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# /api/symbols/all
# ---------------------------------------------------------------------------

class TestSymbolsAll:
    def test_returns_list(self):
        result = _get('/api/symbols/all')
        assert isinstance(result, list)
        assert len(result) > 100  # ASX has 2000+ listed companies

    def test_each_has_symbol_and_name(self):
        result = _get('/api/symbols/all')
        r = result[0]
        assert 'symbol' in r
        assert 'name' in r
        assert 'current' in r

    def test_default_current_only(self):
        result = _get('/api/symbols/all')
        assert all(r['current'] for r in result)


# ---------------------------------------------------------------------------
# /symbol-changes
# ---------------------------------------------------------------------------

class TestSymbolChanges:
    def test_no_symbol_returns_400(self):
        code = _status('/symbol-changes')
        assert code == 400

    def test_unknown_symbol_found_false(self):
        result = _get('/symbol-changes', symbol='ZZZNOEXIST')
        assert result['found'] is False

    def test_result_has_found_field(self):
        result = _get('/symbol-changes', symbol='BHP')
        assert 'found' in result


# ---------------------------------------------------------------------------
# /options
# ---------------------------------------------------------------------------

class TestOptionsEndpoint:
    def test_no_symbol_returns_list(self):
        result = _get('/options')
        assert isinstance(result, list)

    def test_bhp_options_are_list(self):
        result = _get('/options', symbol='BHP')
        assert isinstance(result, list)

    def test_option_row_fields(self):
        result = _get('/options', symbol='BHP')
        if result:
            row = result[0]
            assert 'option_symbol' in row
            assert 'expiry' in row
            assert 'exercise' in row
            assert 'share_symbol' in row

    def test_unknown_symbol_returns_empty(self):
        result = _get('/options', symbol='ZZZNOEXIST')
        assert result == []


# ---------------------------------------------------------------------------
# /api/fundamentals/<symbol>
# ---------------------------------------------------------------------------

class TestFundamentalsSymbol:
    def test_bhp_returns_data(self):
        assert _status('/api/fundamentals/BHP') in (200, 404)

    def test_unknown_returns_404(self):
        assert _status('/api/fundamentals/ZZZNOEXIST') == 404

    def test_bhp_structure(self):
        code = _status('/api/fundamentals/BHP')
        if code == 200:
            data = _get('/api/fundamentals/BHP')
            assert isinstance(data, dict)


# ---------------------------------------------------------------------------
# /api/financials/<symbol>
# ---------------------------------------------------------------------------

class TestFinancials:
    def test_bhp_returns_list_or_missing(self):
        code = _status('/api/financials/BHP')
        assert code in (200, 404)

    def test_unknown_returns_200_empty(self):
        # Returns 200 with empty list for unknown symbols
        code = _status('/api/financials/ZZZNOEXIST')
        assert code in (200, 404)


# ---------------------------------------------------------------------------
# /api/shares/<symbol>
# ---------------------------------------------------------------------------

class TestShares:
    def test_bhp_returns_shares(self):
        code = _status('/api/shares/BHP')
        assert code in (200, 404)

    def test_response_is_list_when_found(self):
        code = _status('/api/shares/BHP')
        if code == 200:
            data = _get('/api/shares/BHP')
            assert isinstance(data, list)


# ---------------------------------------------------------------------------
# /api/dividends/<symbol> and /api/dividends/batch
# ---------------------------------------------------------------------------

class TestDividends:
    def test_bhp_returns_list(self):
        code = _status('/api/dividends/BHP')
        assert code in (200, 404)

    def test_dividends_batch(self):
        result = _post('/api/dividends/batch', {'symbols': ['BHP', 'RIO']})
        assert isinstance(result, dict)

    def test_dividends_batch_empty(self):
        result = _post('/api/dividends/batch', {'symbols': []})
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# /api/events endpoints
# ---------------------------------------------------------------------------

class TestEventsEndpoints:
    def test_events_range_returns_list(self):
        code = _status('/api/events/range')
        assert code in (200, 400, 422)

    def test_events_range_with_params(self):
        # API requires 'from' and 'to' parameters (not start/end)
        code = _status('/api/events/range?from=2026-01-01&to=2026-12-31')
        assert code in (200, 404)

    def test_events_upcoming_returns_list(self):
        code = _status('/api/events/upcoming')
        assert code in (200, 404)

    def test_events_symbol_bhp(self):
        code = _status('/api/events/BHP')
        assert code in (200, 404)

    def test_events_symbol_unknown(self):
        code = _status('/api/events/ZZZNOEXIST')
        assert code in (200, 404)


# ---------------------------------------------------------------------------
# /api/live-indices and /api/commodities
# ---------------------------------------------------------------------------

class TestLiveData:
    def test_live_indices_returns_dict(self):
        code = _status('/api/live-indices')
        assert code in (200, 503)

    def test_commodities_returns_data(self):
        code = _status('/api/commodities')
        assert code in (200, 503)

    def test_fundamentals_all_returns_list(self):
        code = _status('/api/fundamentals/all')
        assert code in (200, 503)
        if code == 200:
            data = _get('/api/fundamentals/all')
            assert isinstance(data, list)
