#! /usr/bin/env python3
# Stock data REST API — backend service for asx-data.
# Runs on harri alongside asx-announcements. No user auth; internal on Tailscale.
# Frontend calls this instead of importing stockdb directly.

import bisect, datetime, json, math, os, sqlite3, time
from flask import Flask, jsonify, request, abort

# stockdb is on PYTHONPATH (../stockdb when running locally, /stockdb in Docker)
import stockdb

app = Flask(__name__)

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
