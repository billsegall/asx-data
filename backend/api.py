#! /usr/bin/env python3
# Stock data REST API — backend service for asx-data. Deploy via deploy.sh.
# Runs on harri alongside asx-announcements. No user auth; internal on Tailscale.
# Frontend calls this instead of importing stockdb directly.

import bisect, datetime, json, math, os, sqlite3, time
import yfinance as yf
from flask import Flask, jsonify, request, abort, send_file, make_response

# stockdb is on PYTHONPATH (../stockdb when running locally, /stockdb in Docker)
import stockdb

app = Flask(__name__)

@app.after_request
def add_cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

DATABASE = os.environ.get('DATABASE', '../stockdb/stockdb.db')

stocks = stockdb.StockDB(DATABASE, False)


## Utility

def millify(n):
    millnames = ['', ' Thousand', ' Million', ' Billion', ' Trillion']
    n = float(n)
    millidx = max(0, min(len(millnames) - 1, int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3))))
    return '{:.0f}{}'.format(n / 10 ** (3 * millidx), millnames[millidx])


def date2human(ts):
    return datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d')


## Quote cache (live prices from Yahoo Finance)

_quote_cache = {}
_QUOTE_TTL   = 300  # seconds


## Enrichment cache

_enrich_cache = {}
_ENRICH_TTL   = 300  # seconds


def _enrich_batch(symbols):
    """Return dict of symbol -> metrics. Batched queries, 5-minute cache per symbol."""
    if not symbols:
        return {}

    now_ts = time.time()
    result = {}
    stale  = []
    for s in symbols:
        entry = _enrich_cache.get(s)
        if entry and now_ts - entry[0] < _ENRICH_TTL:
            result[s] = entry[1]
        else:
            result[s] = {}
            stale.append(s)

    if not stale:
        return result

    placeholders = ','.join('?' * len(stale))
    c   = stocks.cursor()
    now = datetime.datetime.now()

    # Query 1: last 30 trading days for price, 1d change, 1w return
    c.execute(f'SELECT MAX(date) FROM endofday WHERE symbol IN ({placeholders})', stale)
    max_eod_date = c.fetchone()[0] or 0
    recent_cutoff = max_eod_date - 30 * 86400
    c.execute(f'''
        SELECT symbol, date, close, volume FROM endofday
        WHERE symbol IN ({placeholders}) AND date >= ?
        ORDER BY symbol, date DESC
    ''', stale + [recent_cutoff])
    recent_by_sym = {}
    for row in c.fetchall():
        recent_by_sym.setdefault(row[0], []).append((row[1], row[2], row[3]))

    week_cutoff = (now - datetime.timedelta(days=7)).timestamp()
    for sym, rows in recent_by_sym.items():
        price = rows[0][1]
        result[sym]['price']  = price
        result[sym]['volume'] = rows[0][2]
        if len(rows) >= 2:
            prev = rows[1][1]
            result[sym]['change_1d']     = round(price - prev, 4)
            result[sym]['change_1d_pct'] = round((price - prev) / prev * 100, 2) if prev else None
        candidates = [r for r in rows if r[0] <= week_cutoff] or rows
        ref = candidates[-1][1]
        if ref:
            result[sym]['change_1w_pct'] = round((price - ref) / ref * 100, 2)

    # Query 2: monthly closes for period returns (1m, 3m, 6m, 1y, 3y, 5y)
    lookbacks = {
        'change_1m_pct': now - datetime.timedelta(days=31),
        'change_3m_pct': now - datetime.timedelta(days=92),
        'change_6m_pct': now - datetime.timedelta(days=183),
        'change_1y_pct': now - datetime.timedelta(days=365),
        'change_3y_pct': now - datetime.timedelta(days=365 * 3),
        'change_5y_pct': now - datetime.timedelta(days=365 * 5),
    }
    cutoff_ts = min(dt.timestamp() for dt in lookbacks.values())
    c.execute(f'''
        SELECT symbol, date, close FROM endofmonth
        WHERE symbol IN ({placeholders}) AND date >= ?
        ORDER BY symbol, date ASC
    ''', stale + [cutoff_ts])
    monthly = {}
    for row in c.fetchall():
        monthly.setdefault(row[0], []).append((row[1], row[2]))

    for sym, rows in monthly.items():
        price = result[sym].get('price')
        if not price or not rows:
            continue
        dates = [r[0] for r in rows]
        for key, target_dt in lookbacks.items():
            idx = bisect.bisect_left(dates, target_dt.timestamp())
            ref_price = rows[idx][1] if idx < len(rows) else (rows[-1][1] if rows else None)
            if ref_price:
                result[sym][key] = round((price - ref_price) / ref_price * 100, 2)

    # Query 3: 52-week high/low
    cutoff_52w = (now - datetime.timedelta(days=365)).timestamp()
    c.execute(f'''
        SELECT symbol, MAX(high), MIN(low) FROM endofday
        WHERE symbol IN ({placeholders}) AND date >= ?
        GROUP BY symbol
    ''', stale + [cutoff_52w])
    for row in c.fetchall():
        result[row[0]]['high_52w'] = row[1]
        result[row[0]]['low_52w']  = row[2]

    # Query 4: latest short %
    c.execute(f'''
        SELECT s.symbol, s.short FROM shorts s
        INNER JOIN (
            SELECT symbol, MAX(date) as max_date FROM shorts
            WHERE symbol IN ({placeholders})
            GROUP BY symbol
        ) m ON s.symbol = m.symbol AND s.date = m.max_date
    ''', stale)
    for row in c.fetchall():
        result[row[0]]['short_pct'] = row[1]

    # Query 5: name, industry, mcap
    c.execute(f'''
        SELECT symbol, name, industry, shares FROM symbols
        WHERE symbol IN ({placeholders})
    ''', stale)
    for row in c.fetchall():
        result[row[0]]['name']     = row[1]
        result[row[0]]['industry'] = row[2]
        price = result[row[0]].get('price')
        if row[3] and price:
            result[row[0]]['mcap'] = millify(row[3] * price)

    for s in stale:
        _enrich_cache[s] = (now_ts, result[s])

    return result


## Routes

@app.route('/api/stock/<symbol>')
def api_stock(symbol):
    symbol = symbol.strip().upper()
    start_str = request.args.get('start')
    end_str   = request.args.get('end')

    try:
        start_ts = time.mktime(time.strptime(start_str, '%Y%m%d')) if start_str else 0
    except Exception:
        start_ts = 0
    try:
        end_ts = time.mktime(time.strptime(end_str, '%Y%m%d')) if end_str else time.time()
    except Exception:
        end_ts = time.time()

    c = stocks.cursor()
    if not c.execute('SELECT 1 FROM endofday WHERE symbol = ? LIMIT 1', (symbol,)).fetchone():
        abort(404)

    name, industry, shares = stocks.LookupSymbol(symbol)
    mcap = None
    if shares:
        row = stocks.cursor().execute(
            'SELECT close FROM endofday WHERE symbol = ? ORDER BY date DESC LIMIT 1', (symbol,)
        ).fetchone()
        if row:
            mcap = shares * row[0]

    c.execute(
        'SELECT date, open, high, low, close, volume FROM endofday '
        'WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date ASC',
        (symbol, start_ts, end_ts)
    )
    ohlcv = [[int(r[0]) * 1000, r[1], r[2], r[3], r[4], r[5]] for r in c.fetchall()]

    c.execute(
        'SELECT date, close FROM endofday '
        'WHERE symbol = "XAO" AND date >= ? AND date <= ? ORDER BY date ASC',
        (start_ts, end_ts)
    )
    xao = [[int(r[0]) * 1000, r[1]] for r in c.fetchall()]

    c.execute(
        'SELECT date, short FROM shorts '
        'WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date ASC',
        (symbol, start_ts, end_ts)
    )
    shorts_data = [[int(r[0]) * 1000, r[1]] for r in c.fetchall()]

    try:
        c.execute(
            'SELECT date*1000, ratio, event_type, description FROM corporate_events '
            'WHERE symbol = ? ORDER BY date ASC',
            (symbol,)
        )
        splits = [{'date': r[0], 'ratio': r[1], 'type': r[2], 'description': r[3]}
                  for r in c.fetchall()]
    except Exception:
        splits = []

    return jsonify({
        'symbol': symbol,
        'info': {
            'name': name,
            'industry': industry,
            'mcap': millify(mcap) if mcap else None,
        },
        'ohlcv': ohlcv,
        'xao': xao,
        'shorts': shorts_data,
        'splits': splits,
    })


@app.route('/api/symbols')
def api_symbols():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify([])
    c = stocks.cursor()
    pattern = q.upper() + '%'
    like    = '%' + q.upper() + '%'
    c.execute('''
        SELECT symbol, name FROM symbols
        WHERE symbol LIKE ? OR upper(name) LIKE ?
        ORDER BY CASE WHEN symbol LIKE ? THEN 0 ELSE 1 END, symbol
        LIMIT 10
    ''', (pattern, like, pattern))
    return jsonify([{'symbol': r[0], 'name': r[1]} for r in c.fetchall()])


@app.route('/api/shorts')
def api_shorts():
    c = stocks.cursor()
    c.execute('''SELECT s.symbol, max(s.date), s.short, sym.name
                 FROM shorts s LEFT JOIN symbols sym ON s.symbol = sym.symbol
                 WHERE length(s.symbol) = 3
                 GROUP BY s.symbol ORDER BY s.date DESC, s.short DESC''')
    rows = [{'symbol': r[0], 'date': date2human(r[1]), 'short': r[2], 'name': r[3] or ''} for r in c.fetchall()]
    lc = stocks.cursor()
    lc.execute('SELECT max(date) FROM shorts')
    latest = lc.fetchone()[0]
    return jsonify({'data': rows, 'latest_date': date2human(latest) if latest else None})


@app.route('/api/enrich', methods=['POST'])
def api_enrich():
    """Batch enrichment: POST {"symbols": ["BHP", "CBA", ...]}
    Returns dict of symbol -> metrics (price, change%, mcap, shorts, etc.)."""
    data = request.get_json(force=True) or {}
    symbols = data.get('symbols', [])
    if not isinstance(symbols, list) or len(symbols) > 500:
        abort(400)
    symbols = [s.strip().upper() for s in symbols if isinstance(s, str)]
    return jsonify(_enrich_batch(symbols))


@app.route('/api/symbol/<symbol>')
def api_symbol_info(symbol):
    """Quick lookup: name, industry, mcap for a single symbol (used by stock page)."""
    symbol = symbol.strip().upper()
    c = stocks.cursor()
    if not c.execute('SELECT 1 FROM endofday WHERE symbol = ? LIMIT 1', (symbol,)).fetchone():
        abort(404)
    name, industry, shares = stocks.LookupSymbol(symbol)
    mcap = None
    if shares:
        row = stocks.cursor().execute(
            'SELECT close FROM endofday WHERE symbol = ? ORDER BY date DESC LIMIT 1', (symbol,)
        ).fetchone()
        if row:
            mcap = shares * row[0]
    return jsonify({'name': name, 'industry': industry, 'mcap': millify(mcap) if mcap else None})


@app.route('/signals')
def signals_page():
    return send_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'signals.html'))

@app.route('/portfolio')
def portfolio_page():
    return send_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'portfolio.html'))

@app.route('/api/analysis/portfolio')
def api_analysis_portfolio():
    data = _load_analysis_file('portfolio_backtest.json')
    if data is None:
        return jsonify({'error': 'No portfolio backtest results available'}), 404
    return jsonify(data)


## Analysis endpoints (serve pre-computed results from analysis/results/)

ANALYSIS_RESULTS_DIR = os.environ.get('ANALYSIS_RESULTS_DIR', '../analysis/results')


def _load_analysis_file(filename: str):
    """Load a JSON file from the analysis results directory."""
    path = os.path.join(ANALYSIS_RESULTS_DIR, filename)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


@app.route('/api/analysis/signals')
def api_analysis_signals():
    """Signal rankings. ?signal=short_trend&industry=Gold&top=50"""
    signal_name = request.args.get('signal', 'short_trend')
    industry_filter = request.args.get('industry', '').strip()
    top_n = min(int(request.args.get('top', 50)), 500)

    data = _load_analysis_file(f'predictions_{signal_name}.json')
    if data is None:
        # Try generic signal file
        data = _load_analysis_file(f'signal_{signal_name}.json')
    if data is None:
        return jsonify({'error': f'No results for signal: {signal_name}'}), 404

    predictions = data.get('predictions') or data.get('scores', [])
    if industry_filter:
        predictions = [p for p in predictions if industry_filter.lower() in p.get('industry', '').lower()]
    predictions = predictions[:top_n]

    return jsonify({
        'signal': signal_name,
        'generated_at': data.get('generated_at'),
        'n_total': data.get('n_symbols', len(predictions)),
        'results': predictions,
    })


@app.route('/api/analysis/signal/<symbol>')
def api_analysis_signal_symbol(symbol):
    """Per-symbol signal scores across all signals."""
    symbol = symbol.strip().upper()
    result = {'symbol': symbol, 'signals': {}}

    for fname in os.listdir(ANALYSIS_RESULTS_DIR) if os.path.isdir(ANALYSIS_RESULTS_DIR) else []:
        if not (fname.startswith('predictions_') and fname.endswith('.json')):
            continue
        data = _load_analysis_file(fname)
        if not data:
            continue
        predictions = data.get('predictions', [])
        match = next((p for p in predictions if p.get('symbol') == symbol), None)
        if match:
            signal_name = data.get('signal', fname)
            result['signals'][signal_name] = {
                'score': match.get('score'),
                'generated_at': data.get('generated_at'),
            }

    return jsonify(result)


@app.route('/api/analysis/backtest')
def api_analysis_backtest():
    """Latest backtest reports for all signals."""
    reports = {}
    if not os.path.isdir(ANALYSIS_RESULTS_DIR):
        return jsonify({'error': 'No results directory'}), 404

    for fname in os.listdir(ANALYSIS_RESULTS_DIR):
        if fname.startswith('backtest_') and fname.endswith('.json'):
            data = _load_analysis_file(fname)
            if data:
                signal_name = data.get('signal_name', fname)
                reports[signal_name] = data

    if not reports:
        return jsonify({'error': 'No backtest results available'}), 404

    return jsonify(reports)


@app.route('/api/analysis/discovery')
def api_analysis_discovery():
    """Full IC sweep results — all (feature, lag) pairs."""
    import csv
    csv_path = os.path.join(ANALYSIS_RESULTS_DIR, 'ic_sweep_results.csv')
    if not os.path.exists(csv_path):
        return jsonify({'error': 'No IC sweep results available'}), 404

    rows = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                'feature': row.get('feature'),
                'lag': int(row.get('lag', 0)),
                'mean_ic': float(row.get('mean_ic', 0) or 0),
                'std_ic': float(row.get('std_ic', 0) or 0),
                'ic_ir': float(row.get('ic_ir', 0) or 0),
                't_stat': float(row.get('t_stat', 0) or 0),
                'p_value': float(row.get('p_value', 1) or 1),
                'n': int(float(row.get('n', 0) or 0)),
                'fdr_significant': row.get('fdr_significant', 'False') == 'True',
                'fdr_corrected_p': float(row.get('fdr_corrected_p', 1) or 1),
            })

    return jsonify({'results': rows, 'n': len(rows)})

@app.route('/discovery')
def discovery_page():
    return send_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'discovery.html'))


@app.route('/api/quote/<symbol>')
def api_quote(symbol):
    """Live price from Yahoo Finance, cached for 5 minutes."""
    symbol = symbol.strip().upper()
    now_ts = time.time()
    entry = _quote_cache.get(symbol)
    if entry and now_ts - entry[0] < _QUOTE_TTL:
        return jsonify(entry[1])

    yf_ticker = '^AORD' if symbol == 'XAO' else f'{symbol}.AX'
    try:
        fi = yf.Ticker(yf_ticker).fast_info
        price = fi.last_price
        prev_close = fi.previous_close
        if not price:
            abort(503)
        data = {
            'symbol':     symbol,
            'price':      round(float(price), 3),
            'prev_close': round(float(prev_close), 3) if prev_close else None,
            'change':     round(float(price - prev_close), 3) if prev_close else None,
            'change_pct': round(float((price - prev_close) / prev_close * 100), 2) if prev_close else None,
        }
    except Exception:
        abort(503)

    _quote_cache[symbol] = (now_ts, data)
    return jsonify(data)
