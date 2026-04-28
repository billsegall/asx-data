#! /usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
# Stock data REST API — backend service for asx-data. Deploy via deploy.sh.
# No user auth — internal network only.
# Frontend calls this instead of importing stockdb directly.

import bisect, datetime, json, math, os, signal, sqlite3, threading, time
from concurrent.futures import ThreadPoolExecutor
import requests
import yfinance as yf
from flask import Flask, jsonify, request, abort, make_response, Response

# stockdb is on PYTHONPATH (../stockdb when running locally, /stockdb in Docker)
import stockdb

app = Flask(__name__)

@app.after_request
def add_cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

DATABASE     = os.environ.get('DATABASE', '../stockdb/stockdb.db')
FRONTEND_URL = os.environ.get('FRONTEND_URL', '')
VOLUME_CONFIG_FILE = os.path.dirname(DATABASE) + '/volume_config.json'

stocks = stockdb.StockDB(DATABASE, False)

# Load volume config
def _load_volume_config():
    """Load volume bracket configuration from JSON file."""
    try:
        with open(VOLUME_CONFIG_FILE) as f:
            return json.load(f)
    except Exception as e:
        app.logger.warning('Failed to load volume config: %s', e)
        return None

volume_config = _load_volume_config()

# Signal handler to reload volume config on SIGHUP
def _reload_volume_config(signum, frame):
    global volume_config
    volume_config = _load_volume_config()
    app.logger.info('Reloaded volume config')

signal.signal(signal.SIGHUP, _reload_volume_config)


def _migrate_and_refresh_currency():
    """Add current column if missing, then mark symbols with no EOD in the past year as old."""
    c = stocks.cursor()
    try:
        c.execute('ALTER TABLE symbols ADD COLUMN current INTEGER NOT NULL DEFAULT 1')
        stocks.commit()
    except Exception:
        pass  # column already exists
    one_year_ago = time.time() - 365 * 24 * 3600
    c.execute('UPDATE symbols SET current = 1')
    c.execute('''UPDATE symbols SET current = 0
                 WHERE symbol NOT IN (
                     SELECT DISTINCT symbol FROM endofday WHERE date > ?
                 )''', (one_year_ago,))
    stocks.commit()


_migrate_and_refresh_currency()


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

    # Fallback for symbols outside the 30-day window (e.g. low-liquidity options)
    no_price = [s for s in stale if 'price' not in result[s]]
    if no_price:
        ph2 = ','.join('?' * len(no_price))
        c.execute(f'''
            SELECT e.symbol, e.date, e.close, e.volume FROM endofday e
            INNER JOIN (SELECT symbol, MAX(date) AS max_date FROM endofday
                        WHERE symbol IN ({ph2}) GROUP BY symbol) m
            ON e.symbol = m.symbol AND e.date = m.max_date
        ''', no_price)
        for row in c.fetchall():
            result[row[0]]['price']  = row[2]
            result[row[0]]['volume'] = row[3]

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
            mcap_val = row[3] * price
            result[row[0]]['mcap']     = millify(mcap_val)
            result[row[0]]['mcap_raw'] = mcap_val

    # Query 6: key fundamentals (weekly snapshot)
    try:
        c.execute(f'''
            SELECT symbol, trailing_pe, forward_pe, dividend_yield,
                   recommendation_key, target_low_price, target_mean_price, target_high_price, beta
            FROM fundamentals
            WHERE symbol IN ({placeholders})
              AND (symbol, date) IN (SELECT symbol, MAX(date) FROM fundamentals GROUP BY symbol)
        ''', stale)
        for row in c.fetchall():
            result[row[0]]['trailing_pe']      = row[1]
            result[row[0]]['forward_pe']       = row[2]
            result[row[0]]['div_yield']        = row[3]
            result[row[0]]['recommendation']   = row[4]
            result[row[0]]['target_low_price'] = row[5]
            result[row[0]]['target_mean_price'] = row[6]
            result[row[0]]['target_high_price'] = row[7]
            result[row[0]]['beta']             = row[8]
    except Exception:
        pass  # fundamentals table may not exist yet

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


@app.route('/api/symbols/all')
def api_symbols_all():
    """Return ASX symbols. Defaults to current only; pass ?all=1 to include delisted."""
    include_all = request.args.get('all', '0') == '1'
    c = stocks.cursor()
    if include_all:
        c.execute('SELECT symbol, name, current FROM symbols ORDER BY symbol')
    else:
        c.execute('SELECT symbol, name, current FROM symbols WHERE current = 1 ORDER BY symbol')
    return jsonify([{'symbol': r[0], 'name': r[1], 'current': bool(r[2])} for r in c.fetchall()])


@app.route('/api/symbols')
def api_symbols():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify([])
    include_all = request.args.get('all', '0') == '1'
    c = stocks.cursor()
    pattern = q.upper() + '%'
    like    = '%' + q.upper() + '%'
    currency_filter = '' if include_all else 'AND current = 1'
    c.execute(f'''
        SELECT symbol, name, current FROM (
            SELECT symbol, name, current,
                   CASE WHEN symbol LIKE ? THEN 0 ELSE 1 END AS prio
            FROM symbols
            WHERE (symbol LIKE ? OR upper(name) LIKE ?) {currency_filter}
            UNION ALL
            SELECT option_symbol,
                   share_name || ' option exp ' || expiry || ' @$' || CAST(exercise AS TEXT),
                   1,
                   CASE WHEN option_symbol LIKE ? THEN 0 ELSE 1 END
            FROM asx_options
            WHERE option_symbol LIKE ?
        ) ORDER BY prio, symbol
        LIMIT 10
    ''', (pattern, pattern, like, pattern, pattern))
    return jsonify([{'symbol': r[0], 'name': r[1], 'current': bool(r[2])} for r in c.fetchall()])


@app.route('/api/shorts')
def api_shorts():
    c = stocks.cursor()
    # Get the two most recent dates so we can show change
    c.execute('SELECT DISTINCT date FROM shorts ORDER BY date DESC LIMIT 2')
    dates = [r[0] for r in c.fetchall()]
    latest_date = dates[0] if dates else None
    prev_date   = dates[1] if len(dates) > 1 else None

    c.execute('''SELECT s.symbol, s.short, sym.name
                 FROM shorts s LEFT JOIN symbols sym ON s.symbol = sym.symbol
                 WHERE s.date = ? AND length(s.symbol) = 3
                 ORDER BY s.short DESC''', (latest_date,))
    current = {r[0]: {'short': r[1], 'name': r[2] or ''} for r in c.fetchall()}

    prev = {}
    if prev_date:
        c.execute('SELECT symbol, short FROM shorts WHERE date = ? AND length(symbol) = 3',
                  (prev_date,))
        prev = {r[0]: r[1] for r in c.fetchall()}

    rows = [{'symbol': sym, 'short': v['short'], 'name': v['name'],
             'prev_short': prev.get(sym), 'date': date2human(latest_date) if latest_date else None}
            for sym, v in current.items()]

    return jsonify({
        'data': rows,
        'latest_date': date2human(latest_date) if latest_date else None,
        'prev_date':   date2human(prev_date)   if prev_date   else None,
    })


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
    """Quick lookup: name, industry, mcap, shares, options for a single symbol (used by stock page).
    If the symbol is itself an ASX-listed option (in asx_options), returns is_option=True
    with the option's metadata instead of the normal share fields.
    """
    symbol = symbol.strip().upper()
    c = stocks.cursor()

    # Check if this symbol is itself an ASX-listed option
    opt_row = c.execute(
        'SELECT share_symbol, share_name, expiry, exercise, note FROM asx_options WHERE option_symbol = ?',
        (symbol,)
    ).fetchone()
    if opt_row:
        share_symbol, share_name, expiry, exercise, note = opt_row
        return jsonify({
            'is_option': True,
            'name': f'{share_name} Option' if share_name else f'{share_symbol} Option',
            'underlying': share_symbol,
            'expiry': expiry,
            'exercise': exercise,
            'note': note,
            'industry': None, 'mcap': None, 'shares': None, 'options': [],
        })

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
    options = [
        dict(zip(['option_symbol', 'expiry', 'exercise', 'note'], r))
        for r in c.execute(
            'SELECT option_symbol, expiry, exercise, note FROM asx_options'
            ' WHERE share_symbol = ? ORDER BY expiry, exercise',
            (symbol,)
        ).fetchall()
    ]
    short_row = c.execute(
        '''SELECT short FROM shorts WHERE symbol = ?
           ORDER BY date DESC LIMIT 1''',
        (symbol,)
    ).fetchone()
    return jsonify({
        'is_option': False,
        'name': name,
        'industry': industry,
        'mcap': millify(mcap) if mcap else None,
        'mcap_raw': mcap,
        'shares': millify(shares) if shares else None,
        'short_pct': short_row[0] if short_row else None,
        'options': options,
    })


@app.route('/api/fundamentals/<symbol>')
def api_fundamentals(symbol):
    """Full fundamentals row for one symbol."""
    symbol = symbol.strip().upper()
    c = stocks.cursor()
    try:
        row = c.execute(
            'SELECT * FROM fundamentals WHERE symbol = ? ORDER BY date DESC LIMIT 1', (symbol,)
        ).fetchone()
    except Exception:
        return jsonify({'error': 'fundamentals table not available'}), 503
    if not row:
        return jsonify({'error': 'No fundamentals data'}), 404
    cols = [d[0] for d in c.description]
    result = dict(zip(cols, row))

    # Add average daily volume in dollar terms and volume bucket
    try:
        avg_dollar_vol = c.execute(
            'SELECT AVG(close * volume) FROM endofday WHERE symbol = ?', (symbol,)
        ).fetchone()[0]
        if avg_dollar_vol:
            result['avg_daily_dollar_volume'] = avg_dollar_vol
            # Determine volume bucket based on config file
            if volume_config and 'brackets' in volume_config:
                for bracket in volume_config['brackets']:
                    max_val = bracket.get('max')
                    if max_val is None or avg_dollar_vol < max_val:
                        result['volume_bucket'] = bracket['bucket']
                        break
            else:
                # Fallback if config not loaded
                result['volume_bucket'] = 5
    except Exception:
        pass

    # Check for consolidations in the last 12 months
    try:
        cutoff_ts = time.time() - 365 * 86400
        consolidation = c.execute(
            'SELECT date, description FROM corporate_events WHERE symbol = ? AND event_type = ? AND date >= ? ORDER BY date DESC LIMIT 1',
            (symbol, 'consolidation', cutoff_ts)
        ).fetchone()
        if consolidation:
            event_date, description = consolidation
            result['recent_consolidation'] = {
                'date': event_date,
                'description': description or 'Stock consolidation'
            }
    except Exception:
        pass

    # Check for trading suspension status from announcements database
    try:
        announcements_db = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                                       'asx-announcements', 'announcements.db')
        if os.path.exists(announcements_db):
            ann_conn = sqlite3.connect(announcements_db, timeout=5)
            ann_c = ann_conn.cursor()
            # Get the most recent suspension/reinstatement announcement
            suspension = ann_c.execute('''
                SELECT suspension_type, announced_at FROM extracted_suspensions
                WHERE ticker = ? ORDER BY announced_at DESC LIMIT 1
            ''', (symbol,)).fetchone()
            ann_conn.close()

            if suspension:
                suspension_type, announced_at = suspension
                # Check if it's a reinstatement or an active suspension
                is_suspended = suspension_type != 'reinstatement'
                result['trading_suspension'] = {
                    'is_suspended': is_suspended,
                    'type': suspension_type,
                    'announced_at': announced_at
                }
    except Exception:
        pass

    return jsonify(result)


@app.route('/api/financials/<symbol>')
def api_financials(symbol):
    """Annual financial statements for one symbol, sorted oldest first."""
    symbol = symbol.strip().upper()
    c = stocks.cursor()
    try:
        rows = c.execute(
            '''SELECT fiscal_year_end, total_revenue, gross_profit, operating_income,
                      net_income, ebitda, basic_eps, operating_cashflow, free_cashflow,
                      capital_expenditure, total_debt, stockholders_equity, cash
               FROM financials_annual WHERE symbol = ?
               ORDER BY fiscal_year_end''',
            (symbol,)
        ).fetchall()
    except Exception:
        return jsonify({'error': 'financials_annual table not available'}), 503
    if not rows:
        return jsonify([])
    cols = ['fiscal_year_end','total_revenue','gross_profit','operating_income',
            'net_income','ebitda','basic_eps','operating_cashflow','free_cashflow',
            'capital_expenditure','total_debt','stockholders_equity','cash']
    return jsonify([dict(zip(cols, r)) for r in rows])


@app.route('/api/shares/<symbol>')
def api_shares(symbol):
    """Annual shares-on-issue history for one symbol, oldest first."""
    symbol = symbol.strip().upper()
    c = stocks.cursor()
    try:
        rows = c.execute(
            'SELECT year, shares FROM shares_history WHERE symbol = ? ORDER BY year',
            (symbol,)
        ).fetchall()
    except Exception:
        return jsonify([])
    return jsonify([{'year': r[0], 'shares': r[1]} for r in rows])


@app.route('/api/dividends/<symbol>')
def api_dividends(symbol):
    """Historical dividend payments for one symbol, newest first."""
    symbol = symbol.strip().upper()
    c = stocks.cursor()
    try:
        rows = c.execute(
            'SELECT ex_date, amount, currency FROM dividends WHERE symbol = ? ORDER BY ex_date DESC',
            (symbol,)
        ).fetchall()
    except Exception:
        return jsonify({'error': 'dividends table not available'}), 503
    return jsonify([{
        'ex_date':  r[0] * 1000,  # ms for JS/Plotly
        'amount':   r[1],
        'currency': r[2],
    } for r in rows])


@app.route('/api/dividends/batch', methods=['POST'])
def api_dividends_batch():
    """Recent dividend history for multiple symbols. Body: {"symbols": [...], "limit": 6}"""
    data    = request.get_json(force=True) or {}
    symbols = [s.strip().upper() for s in (data.get('symbols') or []) if s]
    limit   = int(data.get('limit', 6))
    if not symbols:
        return jsonify({})
    placeholders = ','.join('?' * len(symbols))
    c = stocks.cursor()
    try:
        rows = c.execute(
            f'''SELECT symbol, ex_date, amount, currency
                FROM dividends WHERE symbol IN ({placeholders})
                ORDER BY symbol, ex_date DESC''',
            symbols,
        ).fetchall()
    except Exception:
        return jsonify({'error': 'dividends table not available'}), 503
    result = {}
    for sym, ex_date, amount, currency in rows:
        if sym not in result:
            result[sym] = []
        if len(result[sym]) < limit:
            result[sym].append({'ex_date': ex_date, 'amount': amount, 'currency': currency})
    return jsonify(result)


@app.route('/api/events/range')
def api_events_range():
    """Return events between two absolute dates.
    Params: from=YYYY-MM-DD, to=YYYY-MM-DD, symbols=A,B,C (optional)
    """
    import datetime as _dt
    from_str = request.args.get('from', '')
    to_str   = request.args.get('to',   '')
    symbols_param = request.args.get('symbols', '')
    try:
        from_ts = int(_dt.datetime.strptime(from_str, '%Y-%m-%d').timestamp())
        to_ts   = int(_dt.datetime.strptime(to_str,   '%Y-%m-%d').timestamp()) + 86399
    except (ValueError, TypeError):
        return jsonify({'error': 'from and to required (YYYY-MM-DD)'}), 400
    c = stocks.cursor()
    try:
        if symbols_param:
            syms = [s.strip().upper() for s in symbols_param.split(',') if s.strip()]
            placeholders = ','.join('?' * len(syms))
            rows = c.execute(f'''
                SELECT e.id, e.symbol, s.name, e.event_date, e.end_date,
                       e.event_type, e.title, e.description, e.is_estimate
                FROM events e JOIN symbols s ON e.symbol = s.symbol
                WHERE e.symbol IN ({placeholders})
                  AND e.event_date BETWEEN ? AND ?
                ORDER BY e.event_date
            ''', (*syms, from_ts, to_ts)).fetchall()
        else:
            rows = c.execute('''
                SELECT e.id, e.symbol, s.name, e.event_date, e.end_date,
                       e.event_type, e.title, e.description, e.is_estimate
                FROM events e JOIN symbols s ON e.symbol = s.symbol
                WHERE e.event_date BETWEEN ? AND ?
                ORDER BY e.event_date
            ''', (from_ts, to_ts)).fetchall()
    except Exception as e:
        app.logger.warning('api_events_range: %s', e)
        return jsonify([])
    cols = ['id','symbol','name','event_date','end_date','event_type','title','description','is_estimate']
    return jsonify([dict(zip(cols, r)) for r in rows])


@app.route('/api/events/upcoming')
def api_events_upcoming():
    days = min(int(request.args.get('days', 90)), 365)
    past = min(int(request.args.get('past', 1)), 30)   # days to look back (default 1)
    symbols_param = request.args.get('symbols', '')
    c = stocks.cursor()
    try:
        if symbols_param:
            syms = [s.strip().upper() for s in symbols_param.split(',') if s.strip()]
            placeholders = ','.join('?' * len(syms))
            rows = c.execute(f'''
                SELECT e.id, e.symbol, s.name, e.event_date, e.end_date,
                       e.event_type, e.title, e.description, e.is_estimate
                FROM events e JOIN symbols s ON e.symbol = s.symbol
                WHERE e.symbol IN ({placeholders})
                  AND e.event_date BETWEEN strftime('%s','now','-'||?||' days')
                  AND strftime('%s','now','+'||?||' days')
                ORDER BY e.event_date
            ''', (*syms, past, days)).fetchall()
        else:
            rows = c.execute('''
                SELECT e.id, e.symbol, s.name, e.event_date, e.end_date,
                       e.event_type, e.title, e.description, e.is_estimate
                FROM events e JOIN symbols s ON e.symbol = s.symbol
                WHERE e.event_date BETWEEN strftime('%s','now','-'||?||' days')
                  AND strftime('%s','now','+'||?||' days')
                ORDER BY e.event_date
            ''', (past, days)).fetchall()
    except Exception as e:
        app.logger.warning('api_events_upcoming: %s', e)
        return jsonify([])
    cols = ['id','symbol','name','event_date','end_date','event_type','title','description','is_estimate']
    return jsonify([dict(zip(cols, r)) for r in rows])


@app.route('/api/events/<int:event_id>/ics')
def api_event_ics(event_id):
    c = stocks.cursor()
    try:
        row = c.execute(
            'SELECT symbol, event_date, end_date, event_type, title, description, is_estimate FROM events WHERE id=?',
            (event_id,)
        ).fetchone()
    except Exception:
        return jsonify({'error': 'not found'}), 404
    if not row:
        return jsonify({'error': 'not found'}), 404
    symbol, event_date, end_date, event_type, title, description, is_estimate = row
    dt_start = datetime.datetime.fromtimestamp(event_date, tz=datetime.timezone.utc)
    dt_end   = datetime.datetime.fromtimestamp(end_date or event_date, tz=datetime.timezone.utc)
    dtstamp  = datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    dtstart  = dt_start.strftime('%Y%m%d')
    # DTEND for all-day events = day AFTER last day (RFC 5545 exclusive end)
    dtend = (dt_end + datetime.timedelta(days=1)).strftime('%Y%m%d')
    uid      = f'{symbol}-{event_type}-{dt_start.strftime("%Y%m%d")}@asx-toolkit'
    status   = 'TENTATIVE' if is_estimate else 'CONFIRMED'
    desc_str = (description or '').replace('\\', '\\\\').replace('\n', '\\n').replace(',', '\\,').replace(';', '\\;')
    title_str = (title or '').replace('\\', '\\\\').replace('\n', '\\n').replace(',', '\\,').replace(';', '\\;')
    ics = (
        'BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//ASX Toolkit//Event Calendar//EN\r\n'
        'CALSCALE:GREGORIAN\r\nMETHOD:PUBLISH\r\n'
        'BEGIN:VEVENT\r\n'
        f'UID:{uid}\r\nDTSTAMP:{dtstamp}\r\n'
        f'DTSTART;VALUE=DATE:{dtstart}\r\nDTEND;VALUE=DATE:{dtend}\r\n'
        f'SUMMARY:{title_str}\r\nDESCRIPTION:{desc_str}\r\nSTATUS:{status}\r\n'
        'END:VEVENT\r\nEND:VCALENDAR\r\n'
    )
    return Response(ics, mimetype='text/calendar',
                    headers={'Content-Disposition': f'attachment; filename="{symbol}-{event_type}.ics"'})


@app.route('/api/events/<symbol>')
def api_events_symbol(symbol):
    symbol = symbol.strip().upper()
    c = stocks.cursor()
    try:
        rows = c.execute('''
            SELECT id, event_date, end_date, event_type, title, description, is_estimate
            FROM events
            WHERE symbol = ?
              AND event_date >= strftime('%s','now','-7 days')
            ORDER BY event_date
        ''', (symbol,)).fetchall()
    except Exception:
        return jsonify([])
    cols = ['id','event_date','end_date','event_type','title','description','is_estimate']
    return jsonify([dict(zip(cols, r)) for r in rows])


_indices_cache = {'ts': 0, 'data': None}
_INDICES_TTL   = 5 * 60  # 5 minutes

_YF_INDICES = {
    'XAO': '^AORD',
    'XJO': '^AXJO',
}

@app.route('/api/live-indices')
def api_live_indices():
    """Live index prices from Yahoo Finance, cached for 5 minutes.
    Returns dict keyed by Australian symbol (XAO, XJO)."""
    global _indices_cache
    now = time.time()
    if now - _indices_cache['ts'] < _INDICES_TTL and _indices_cache['data'] is not None:
        return jsonify(_indices_cache['data'])
    result = {}
    for asx_sym, yf_sym in _YF_INDICES.items():
        try:
            fi = yf.Ticker(yf_sym).fast_info
            price      = fi.last_price
            prev_close = fi.previous_close
            if price is not None and prev_close:
                change    = round(price - prev_close, 2)
                change_pct = round((price - prev_close) / prev_close * 100, 2)
            else:
                change = change_pct = None
            result[asx_sym] = {'price': price, 'change_1d': change, 'change_1d_pct': change_pct}
        except Exception:
            result[asx_sym] = {}
    _indices_cache = {'ts': now, 'data': result}
    return jsonify(result)


@app.route('/api/commodities')
def api_commodities():
    """All commodities with latest price, 24h change, 52-week range, and 30-day sparkline."""
    c = stocks.cursor()
    try:
        yr_ago = int(time.time() - 365 * 86400)
        rows = c.execute('''
            SELECT
                m.id, m.name, m.unit,
                cur.price  AS price,
                cur.date   AS date,
                prev.price AS prev_price,
                stats.high_52w,
                stats.low_52w,
                stats.high_52w_date,
                stats.low_52w_date
            FROM commodity_meta m
            LEFT JOIN commodity_prices cur ON cur.id = m.id
                AND cur.date = (SELECT MAX(date) FROM commodity_prices WHERE id = m.id)
            LEFT JOIN commodity_prices prev ON prev.id = m.id
                AND prev.date = (SELECT MAX(date) FROM commodity_prices
                                 WHERE id = m.id AND date < cur.date)
            LEFT JOIN (
                SELECT
                    id,
                    MAX(price) AS high_52w,
                    MIN(price) AS low_52w,
                    (SELECT date FROM commodity_prices cp2 WHERE cp2.id = cp1.id AND cp2.date >= :yr_ago ORDER BY cp2.price DESC LIMIT 1) AS high_52w_date,
                    (SELECT date FROM commodity_prices cp3 WHERE cp3.id = cp1.id AND cp3.date >= :yr_ago ORDER BY cp3.price ASC LIMIT 1) AS low_52w_date
                FROM commodity_prices cp1 WHERE date >= :yr_ago GROUP BY id
            ) stats ON stats.id = m.id
            ORDER BY m.id
        ''', {'yr_ago': yr_ago}).fetchall()

        # Fetch last 30 days of prices per commodity for sparklines
        spark_cutoff = int(time.time() - 30 * 86400)
        spark_rows = c.execute(
            'SELECT id, date, price FROM commodity_prices WHERE date >= ? ORDER BY id, date',
            (spark_cutoff,)
        ).fetchall()
        sparklines = {}
        for r in spark_rows:
            sparklines.setdefault(r[0], []).append([r[1] * 1000, r[2]])

    except Exception:
        return jsonify({'error': 'commodity tables not available'}), 503

    result = []
    for r in rows:
        cid, name, unit, price, date, prev_price, high_52w, low_52w, high_52w_date, low_52w_date = r
        change_pct = None
        if price is not None and prev_price is not None and prev_price != 0:
            change_pct = (price - prev_price) / prev_price * 100
        result.append({
            'id':         cid,
            'name':       name,
            'unit':       unit,
            'price':      price,
            'date':       date * 1000 if date else None,
            'change_pct': round(change_pct, 2) if change_pct is not None else None,
            'high_52w':   high_52w,
            'low_52w':    low_52w,
            'high_52w_date': high_52w_date * 1000 if high_52w_date else None,
            'low_52w_date':  low_52w_date * 1000 if low_52w_date else None,
            'sparkline':  sparklines.get(cid, []),
        })
    return jsonify(result)


@app.route('/api/commodity/<commodity_id>')
def api_commodity(commodity_id):
    """Time-series prices for one commodity. Optional ?start=YYYYMMDD&end=YYYYMMDD."""
    commodity_id = commodity_id.strip().upper()
    c = stocks.cursor()
    try:
        meta = c.execute(
            'SELECT id, name, unit FROM commodity_meta WHERE id = ?', (commodity_id,)
        ).fetchone()
        if not meta:
            return jsonify({'error': f'Unknown commodity {commodity_id!r}'}), 404
        start_ts = _parse_date_arg(request.args.get('start'), default_days_ago=365 * 100)
        end_ts   = _parse_date_arg(request.args.get('end'),   default_days_ago=0)
        rows = c.execute(
            'SELECT date, price FROM commodity_prices WHERE id = ? AND date >= ? AND date <= ? ORDER BY date',
            (commodity_id, start_ts, end_ts)
        ).fetchall()
    except Exception as e:
        return jsonify({'error': 'commodity tables not available'}), 503
    return jsonify({
        'id':     meta[0],
        'name':   meta[1],
        'unit':   meta[2],
        'prices': [[r[0] * 1000, r[1]] for r in rows],  # ms timestamps for JS/Plotly
    })


def _parse_date_arg(val, default_days_ago: int) -> int:
    """Parse YYYYMMDD query param to Unix timestamp. Falls back to now - default_days_ago."""
    if val:
        try:
            return int(datetime.datetime.strptime(val, '%Y%m%d').replace(tzinfo=datetime.timezone.utc).timestamp())
        except ValueError:
            pass
    return int(time.time() - default_days_ago * 86400)


def _compute_multi_year_metrics(c):
    """Compute per-symbol multi-year derived metrics from financials_annual.
    Returns dict: symbol -> {rev_cagr_3yr, rev_cagr_5yr, fcf_margin_3yr, roe_avg_3yr}
    """
    try:
        fin_rows = c.execute('''
            SELECT symbol, fiscal_year_end, total_revenue, net_income,
                   free_cashflow, stockholders_equity
            FROM financials_annual
            WHERE total_revenue IS NOT NULL
            ORDER BY symbol, fiscal_year_end
        ''').fetchall()
    except Exception:
        return {}

    from collections import defaultdict
    by_sym = defaultdict(list)
    for sym, fy, rev, ni, fcf, eq in fin_rows:
        by_sym[sym].append((fy, rev, ni, fcf, eq))

    def _cagr(v_new, v_old, n_years):
        if v_new and v_old and v_old > 0 and v_new > 0 and n_years > 0:
            return (v_new / v_old) ** (1.0 / n_years) - 1
        return None

    def _avg(vals):
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    derived = {}
    for sym, rows in by_sym.items():
        # rows sorted ascending by fiscal_year_end
        latest = rows[-1]
        rev_latest = latest[1]

        # 3yr CAGR: latest vs row 3 positions back
        rev_cagr_3yr = None
        if len(rows) >= 3 and rows[-3][1]:
            n3_row = rows[-3]
            n_yrs = (datetime.date.fromisoformat(latest[0]) -
                     datetime.date.fromisoformat(n3_row[0])).days / 365.25
            rev_cagr_3yr = _cagr(rev_latest, n3_row[1], n_yrs)

        # 5yr CAGR: latest vs oldest available with non-null revenue (minimum ~2.5 year span)
        rev_cagr_5yr = None
        if len(rows) >= 4:
            # Iterate oldest→newest to find the earliest row with revenue
            for old_row in rows[:-1]:
                if old_row[1]:
                    n_yrs = (datetime.date.fromisoformat(latest[0]) -
                             datetime.date.fromisoformat(old_row[0])).days / 365.25
                    if n_yrs >= 2.5:
                        rev_cagr_5yr = _cagr(rev_latest, old_row[1], n_yrs)
                    break

        # FCF margin avg over last 3 years with revenue
        last3 = rows[-3:]
        fcf_margins = [fcf / rev for _, rev, _, fcf, _ in last3
                       if rev and fcf is not None and rev != 0]
        fcf_margin_3yr = _avg(fcf_margins)

        # ROE avg over last 3 years (net_income / stockholders_equity)
        roe_vals = [ni / eq for _, _, ni, _, eq in last3
                    if ni is not None and eq and eq != 0]
        roe_avg_3yr = _avg(roe_vals)

        derived[sym] = {
            'rev_cagr_3yr':   rev_cagr_3yr,
            'rev_cagr_5yr':   rev_cagr_5yr,
            'fcf_margin_3yr': fcf_margin_3yr,
            'roe_avg_3yr':    roe_avg_3yr,
        }
    return derived


@app.route('/api/fundamentals/all')
def api_fundamentals_all():
    """All current symbols with key fundamentals columns, sorted by market cap desc. Used by screener."""
    c = stocks.cursor()
    try:
        rows = c.execute('''
            SELECT f.symbol, s.name, s.industry,
                   f.market_cap, f.trailing_pe, f.forward_pe, f.price_to_book,
                   f.enterprise_to_ebitda, f.profit_margins, f.return_on_equity,
                   f.return_on_assets, f.revenue_growth, f.earnings_growth,
                   f.dividend_yield, f.five_year_avg_div_yield, f.payout_ratio, f.debt_to_equity,
                   f.current_ratio, f.beta, f.week52_change,
                   f.recommendation_key, f.analyst_count,
                   f.target_mean_price, f.target_low_price, f.target_high_price,
                   f.eps_trailing, f.eps_forward, f.total_revenue, f.ebitda,
                   f.net_income, f.total_cash, f.total_debt, f.free_cashflow,
                   f.shares_outstanding, f.held_pct_insiders, f.held_pct_institutions,
                   f.fetched_at
            FROM fundamentals f
            JOIN symbols s ON f.symbol = s.symbol
            WHERE s.current = 1
              AND f.date = (SELECT MAX(f2.date) FROM fundamentals f2 WHERE f2.symbol = f.symbol)
            ORDER BY f.market_cap DESC
        ''').fetchall()
    except Exception as e:
        app.logger.warning('api_fundamentals_all: %s', e)
        return jsonify([])

    derived = _compute_multi_year_metrics(c)

    cols = [
        'symbol','name','industry','market_cap','trailing_pe','forward_pe','price_to_book',
        'enterprise_to_ebitda','profit_margins','return_on_equity','return_on_assets',
        'revenue_growth','earnings_growth','dividend_yield','five_year_avg_div_yield','payout_ratio','debt_to_equity',
        'current_ratio','beta','week52_change','recommendation_key','analyst_count',
        'target_mean_price','target_low_price','target_high_price',
        'eps_trailing','eps_forward','total_revenue','ebitda','net_income',
        'total_cash','total_debt','free_cashflow',
        'shares_outstanding','held_pct_insiders','held_pct_institutions','fetched_at',
    ]
    result = []
    for r in rows:
        d = dict(zip(cols, r))
        sym_derived = derived.get(d['symbol'], {})
        d['rev_cagr_3yr']   = sym_derived.get('rev_cagr_3yr')
        d['rev_cagr_5yr']   = sym_derived.get('rev_cagr_5yr')
        d['fcf_margin_3yr'] = sym_derived.get('fcf_margin_3yr')
        d['roe_avg_3yr']    = sym_derived.get('roe_avg_3yr')
        result.append(d)
    return jsonify(result)


def _serve_html(filename):
    """Serve an HTML file, injecting FRONTEND_URL as a JS global."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    with open(path) as f:
        content = f.read()
    inject = f'<script>window.FRONTEND_URL = {json.dumps(FRONTEND_URL)};</script>\n'
    content = content.replace('</head>', inject + '</head>', 1)
    return Response(content, mimetype='text/html')


@app.route('/signals')
def signals_page():
    return _serve_html('signals.html')

@app.route('/portfolio')
def portfolio_page():
    return _serve_html('portfolio.html')

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
    return _serve_html('discovery.html')


# Correlations cache (loaded once, refreshed when file changes)
_correlations_cache = {'mtime': 0, 'rows': [], 'meta': {}}


def _load_correlations():
    """Load correlations.csv + meta.json, returning (rows, meta). Cached by mtime."""
    import csv
    csv_path  = os.path.join(ANALYSIS_RESULTS_DIR, 'correlations.csv')
    meta_path = os.path.join(ANALYSIS_RESULTS_DIR, 'correlations_meta.json')

    if not os.path.exists(csv_path):
        return [], {}

    mtime = os.path.getmtime(csv_path)
    if _correlations_cache['mtime'] == mtime:
        return _correlations_cache['rows'], _correlations_cache['meta']

    rows = []
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            rows.append({
                'leader':          row['leader'],
                'follower':        row['follower'],
                'lag_days':        int(row['lag_days']),
                'direction':       row['direction'],
                'train_r':         float(row['train_r']),
                'backtest_r':      float(row['backtest_r']) if row.get('backtest_r') else None,
                'fdr_p':           float(row['fdr_p']),
                'stable':          row['stable'].lower() == 'true',
                'n_stable':        int(row['n_stable']),
                'market_adjusted': row['market_adjusted'].lower() == 'true',
            })

    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)

    _correlations_cache.update({'mtime': mtime, 'rows': rows, 'meta': meta})
    return rows, meta


@app.route('/api/analysis/correlations')
def api_analysis_correlations():
    """Lead-lag correlation pairs.

    Query params:
      leader=BHP   — filter by leading symbol
      follower=RIO — filter by following symbol
      min_r=0.20   — minimum |train_r|
      stable=1     — stable pairs only (significant in all 3 sub-periods)
      lag=5        — specific lag day
    """
    rows, meta = _load_correlations()
    if not rows and not meta:
        return jsonify({'error': 'No correlation results available'}), 404

    leader   = request.args.get('leader', '').strip().upper()
    follower = request.args.get('follower', '').strip().upper()
    min_r    = float(request.args.get('min_r', 0))
    stable   = request.args.get('stable', '0') == '1'
    lag      = request.args.get('lag')

    filtered = rows
    if leader:
        filtered = [r for r in filtered if r['leader'] == leader]
    if follower:
        filtered = [r for r in filtered if r['follower'] == follower]
    if min_r > 0:
        filtered = [r for r in filtered if abs(r['train_r']) >= min_r]
    if stable:
        filtered = [r for r in filtered if r['stable']]
    if lag:
        try:
            lag_int = int(lag)
            filtered = [r for r in filtered if r['lag_days'] == lag_int]
        except ValueError:
            pass

    return jsonify({'meta': meta, 'results': filtered})


# ---------------------------------------------------------------------------
# Industry correlations — SQLite-backed (per-industry pipeline results)
# ---------------------------------------------------------------------------

CORR_DB_PATH = os.path.join(ANALYSIS_RESULTS_DIR, 'correlations.db')


def _corr_db_conn():
    """Open correlations.db read-only. Caller must close."""
    conn = sqlite3.connect(CORR_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA query_only = ON')
    return conn


@app.route('/api/analysis/correlations/industries')
def api_analysis_correlations_industries():
    """List all industries in correlation_runs with run metadata."""
    if not os.path.exists(CORR_DB_PATH):
        return jsonify({'error': 'No correlations database available'}), 404
    try:
        conn = _corr_db_conn()
        rows = conn.execute(
            """SELECT industry, run_at, n_symbols, n_pairs_tested,
                      n_significant, n_stable, train_start, train_end,
                      backtest_start, backtest_end, max_lag, min_r, elapsed_seconds
               FROM correlation_runs
               ORDER BY industry ASC"""
        ).fetchall()
        conn.close()
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500
    return jsonify([dict(r) for r in rows])


@app.route('/api/analysis/correlations/db')
def api_analysis_correlations_db():
    """Filtered query on the correlations table.

    Query params:
      industry   — exact match
      leader     — exact match (uppercased)
      follower   — exact match (uppercased)
      min_r      — minimum |train_r| (default 0)
      min_stable — minimum n_stable (0–5, default 0)
      lag_min    — minimum lag_days (default 1)
      lag_max    — maximum lag_days (default 20)
      direction  — 'positive' | 'negative' | '' (both)
      sort       — column name (default 'train_r')
      order      — 'asc' | 'desc' (default 'desc')
      limit      — max rows, capped at 5000 (default 500)
    """
    if not os.path.exists(CORR_DB_PATH):
        return jsonify({'error': 'No correlations database available'}), 404

    industry  = request.args.get('industry', '').strip()
    leader    = request.args.get('leader', '').strip().upper()
    follower  = request.args.get('follower', '').strip().upper()
    min_r     = abs(float(request.args.get('min_r', 0) or 0))
    min_stable = max(0, min(5, int(request.args.get('min_stable', 0) or 0)))
    lag_min   = max(1,   int(request.args.get('lag_min', 1)  or 1))
    lag_max   = min(100, int(request.args.get('lag_max', 20) or 20))
    direction = request.args.get('direction', '').strip().lower()
    sort_col  = request.args.get('sort', 'train_r').strip()
    order_dir = request.args.get('order', 'desc').strip().lower()
    limit     = min(5000, max(1, int(request.args.get('limit', 500) or 500)))

    _ALLOWED_SORT = {'leader', 'follower', 'lag_days', 'train_r', 'backtest_r',
                     'fdr_p', 'n_stable', 'recency_score', 'stability', 'industry'}
    if sort_col not in _ALLOWED_SORT:
        sort_col = 'train_r'
    if order_dir not in ('asc', 'desc'):
        order_dir = 'desc'

    clauses = ['lag_days >= ?', 'lag_days <= ?']
    params  = [lag_min, lag_max]

    if industry:
        clauses.append('industry = ?');  params.append(industry)
    if leader:
        clauses.append('leader = ?');    params.append(leader)
    if follower:
        clauses.append('follower = ?');  params.append(follower)
    if min_r > 0:
        # Avoid ABS() so idx_corr_ind_r composite index can be used
        clauses.append('(train_r >= ? OR train_r <= ?)'); params.extend([min_r, -min_r])
    if min_stable > 0:
        clauses.append('n_stable >= ?'); params.append(min_stable)
    if direction == 'positive':
        clauses.append('train_r > 0 AND (backtest_r > 0 OR backtest_r IS NULL)')
    elif direction == 'negative':
        clauses.append('train_r < 0 AND (backtest_r < 0 OR backtest_r IS NULL)')

    where_sql = ' AND '.join(clauses)
    if sort_col == 'train_r':
        order_expr = f'ABS(train_r) {order_dir}'
    elif sort_col == 'stability':
        order_expr = f'n_stable {order_dir}, recency_score {order_dir}'
    else:
        order_expr = f'{sort_col} {order_dir}'
    sql = f"""
        SELECT leader, follower, industry, lag_days, direction,
               train_r, backtest_r, fdr_p, stability, n_stable, recency_score,
               market_adjusted, run_at
        FROM correlations
        WHERE {where_sql}
        ORDER BY {order_expr}
        LIMIT ?
    """
    params.append(limit)

    try:
        conn = _corr_db_conn()
        rows = conn.execute(sql, params).fetchall()
        conn.close()
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500

    return jsonify({
        'n': len(rows),
        'results': [{
            'leader':          r['leader'],
            'follower':        r['follower'],
            'industry':        r['industry'],
            'lag_days':        r['lag_days'],
            'direction':       r['direction'],
            'train_r':         r['train_r'],
            'backtest_r':      r['backtest_r'],
            'fdr_p':           r['fdr_p'],
            'stability':       r['stability'],
            'n_stable':        r['n_stable'],
            'recency_score':   r['recency_score'],
            'market_adjusted': bool(r['market_adjusted']),
            'run_at':          r['run_at'],
        } for r in rows],
    })


# ---------------------------------------------------------------------------
# Correlation backtests — parameterized, multi-config
# ---------------------------------------------------------------------------

BACKTEST_START_TS = 1740787200  # 2025-03-01 UTC
_BT_POSITION_SIZE = 1000
_BT_FEE_FLAT      = 6.0
_BT_FEE_PCT       = 0.0008   # 0.08%
_BT_START_BALANCE = 50000


def _bt_fee(trade_value):
    """Fee per leg: $6 flat or 0.08% of trade value, whichever is higher."""
    return max(_BT_FEE_FLAT, _BT_FEE_PCT * trade_value)

BACKTEST_CONFIGS = [
    {
        'id':          'v1',
        'name':        'Baseline',
        'description': 'All positive-direction pairs, no overlap constraint.',
        'params': {
            'min_train_r':    0.90,
            'min_backtest_r': 0.10,
            'min_lag_days':   1,
            'no_overlap':     False,
        },
    },
    {
        'id':          'v2',
        'name':        'Conservative',
        'description': 'Lag ≥ 3d, no overlapping follower positions, tighter r thresholds.',
        'params': {
            'min_train_r':      0.91,
            'min_backtest_r':   0.15,
            'min_lag_days':     3,
            'no_overlap':       True,
            'deduplicate_pairs': False,
        },
    },
    {
        'id':          'v3',
        'name':        'Conservative + Dedup',
        'description': 'As v2, but for each symbol pair {A,B} only the stronger train_r direction is kept.',
        'params': {
            'min_train_r':      0.91,
            'min_backtest_r':   0.15,
            'min_lag_days':     3,
            'no_overlap':       True,
            'deduplicate_pairs': True,
        },
    },
    {
        'id':          'v5',
        'name':        'Dedup, lag ≥ 3d, high backtest_r',
        'description': 'As v4 but lag ≥ 3d, train_r ≥ 0.85, backtest_r ≥ 0.40.',
        'params': {
            'min_train_r':       0.85,
            'min_backtest_r':    0.40,
            'min_lag_days':      3,
            'no_overlap':        True,
            'deduplicate_pairs': True,
        },
    },
    {
        'id':          'v4',
        'name':        'Dedup, lag ≥ 2d',
        'description': 'As v3 but lag ≥ 2d, train_r ≥ 0.94, backtest_r ≥ 0.20.',
        'params': {
            'min_train_r':       0.94,
            'min_backtest_r':    0.20,
            'min_lag_days':      2,
            'no_overlap':        True,
            'deduplicate_pairs': True,
        },
    },
    {
        'id':          'v6',
        'name':        'Sweep hot zone',
        'description': 'Best sweep combo: train_r ≥ 0.80, backtest_r ≥ 0.10, lag ≥ 15d, dedup + no overlap.',
        'params': {
            'min_train_r':       0.80,
            'min_backtest_r':    0.10,
            'min_lag_days':      15,
            'no_overlap':        True,
            'deduplicate_pairs': True,
        },
    },
    {
        'id':          'v7',
        'name':        'Hot zone — Materials + C&PS only',
        'description': 'v6 filtered to Materials and Commercial & Professional Services industries only.',
        'params': {
            'min_train_r':       0.80,
            'min_backtest_r':    0.10,
            'min_lag_days':      15,
            'no_overlap':        True,
            'deduplicate_pairs': True,
            'industries':        ['Materials', 'Commercial & Professional Services'],
        },
    },
    {
        'id':          'v8',
        'name':        'Two-pair core',
        'description': 'Most durable pairs from prior-year analysis: RIO→SBM (lag 15) and BXB→DOW (lag 17).',
        'params': {
            'min_train_r':       0.80,
            'min_backtest_r':    0.10,
            'min_lag_days':      15,
            'no_overlap':        True,
            'deduplicate_pairs': True,
            'symbol_pairs':      [['RIO', 'SBM'], ['BXB', 'DOW']],
        },
    },
]

_backtest_caches = {cfg['id']: {'result': None, 'date': None} for cfg in BACKTEST_CONFIGS}


def _run_backtest(cfg):
    """Compute virtual portfolio backtest for a given config."""
    params            = cfg['params']
    min_train_r       = params['min_train_r']
    min_backtest_r    = params['min_backtest_r']
    min_lag_days      = params.get('min_lag_days', 1)
    no_overlap        = params.get('no_overlap', False)
    deduplicate_pairs = params.get('deduplicate_pairs', False)
    industries   = params.get('industries')    # optional list of industry names
    symbol_pairs = params.get('symbol_pairs')  # optional [[leader, follower], ...] whitelist

    if not os.path.exists(CORR_DB_PATH):
        return None
    cconn = sqlite3.connect(CORR_DB_PATH, check_same_thread=False)
    sql = """SELECT leader, follower, lag_days, train_r, backtest_r
             FROM correlations
             WHERE direction = 'positive' AND train_r >= ? AND backtest_r > ? AND lag_days >= ?"""
    qparams = [min_train_r, min_backtest_r, min_lag_days]
    if industries:
        sql += f" AND industry IN ({','.join('?'*len(industries))})"
        qparams.extend(industries)
    pairs = cconn.execute(sql, qparams).fetchall()
    cconn.close()
    if symbol_pairs:
        allowed = {(a, b) for a, b in symbol_pairs}
        pairs = [p for p in pairs if (p[0], p[1]) in allowed]
    if not pairs:
        return None

    if deduplicate_pairs:
        # For each unordered symbol pair {A, B}, keep only the direction with higher |train_r|
        best = {}
        for p in pairs:
            key = tuple(sorted([p[0], p[1]]))
            if key not in best or abs(p[3]) > abs(best[key][3]):
                best[key] = p
        pairs = list(best.values())

    all_symbols = set()
    for leader, follower, *_ in pairs:
        all_symbols.add(leader)
        all_symbols.add(follower)

    sconn = sqlite3.connect(DATABASE, check_same_thread=False)
    placeholders = ','.join('?' * len(all_symbols))
    rows = sconn.execute(
        f"""SELECT symbol, date, open, close FROM endofday
            WHERE symbol IN ({placeholders}) AND date >= ?
            ORDER BY symbol, date ASC""",
        list(all_symbols) + [BACKTEST_START_TS]
    ).fetchall()
    sconn.close()

    eod = {}
    date_set = set()
    for symbol, ts, open_p, close_p in rows:
        ds = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
        eod.setdefault(symbol, {})[ds] = (open_p, close_p)
        date_set.add(ds)

    calendar = sorted(date_set)
    if len(calendar) < 2:
        return None

    transactions = []
    # follower -> sell_date of open position (only used when no_overlap=True)
    follower_busy_until = {}

    for i in range(1, len(calendar)):
        d      = calendar[i]
        d_prev = calendar[i - 1]

        for leader, follower, lag_days, train_r, backtest_r in pairs:
            if i + lag_days >= len(calendar):
                continue

            lc_today = eod.get(leader, {}).get(d)
            lc_prev  = eod.get(leader, {}).get(d_prev)
            if not lc_today or not lc_prev or lc_prev[1] == 0:
                continue

            leader_ret = (lc_today[1] - lc_prev[1]) / lc_prev[1]
            if leader_ret <= 0:
                continue

            buy_date  = calendar[i + 1]
            sell_date = calendar[i + lag_days]

            if no_overlap:
                if buy_date < follower_busy_until.get(follower, ''):
                    continue

            buy_data  = eod.get(follower, {}).get(buy_date)
            sell_data = eod.get(follower, {}).get(sell_date)
            if not buy_data or not sell_data:
                continue

            buy_price  = buy_data[0]   # open
            sell_price = sell_data[1]  # close
            if not buy_price or buy_price <= 0:
                continue

            # Shares: start assuming min (flat) fee, then verify actual fee fits
            shares = int((_BT_POSITION_SIZE - _BT_FEE_FLAT) / buy_price)
            buy_fee = _bt_fee(shares * buy_price)
            while shares > 0 and shares * buy_price + buy_fee > _BT_POSITION_SIZE:
                shares -= 1
                buy_fee = _bt_fee(shares * buy_price)
            if shares < 1:
                continue

            sell_fee = _bt_fee(shares * sell_price)
            cost     = shares * buy_price + buy_fee
            proceeds = shares * sell_price - sell_fee
            pnl      = proceeds - cost

            if no_overlap and sell_date > follower_busy_until.get(follower, ''):
                follower_busy_until[follower] = sell_date

            transactions.append({
                'signal_date': d,
                'buy_date':    buy_date,
                'sell_date':   sell_date,
                'leader':      leader,
                'follower':    follower,
                'lag_days':    lag_days,
                'train_r':     round(train_r, 4),
                'backtest_r':  round(backtest_r, 4),
                'buy_price':   round(buy_price, 4),
                'sell_price':  round(sell_price, 4),
                'shares':      shares,
                'cost':        round(cost, 2),
                'proceeds':    round(proceeds, 2),
                'pnl':         round(pnl, 2),
            })

    transactions.sort(key=lambda t: (t['sell_date'], t['signal_date']))

    balance = _BT_START_BALANCE
    balance_by_date = {}
    for t in transactions:
        balance += t['pnl']
        t['balance_after'] = round(balance, 2)
        balance_by_date[t['sell_date']] = round(balance, 2)

    balance_series = [{'date': d, 'balance': b} for d, b in sorted(balance_by_date.items())]
    balance_series.insert(0, {'date': calendar[0], 'balance': _BT_START_BALANCE})

    n_trades   = len(transactions)
    n_wins     = sum(1 for t in transactions if t['pnl'] > 0)
    n_losses   = n_trades - n_wins
    total_fees = round(sum(t['cost'] - t['shares'] * t['buy_price'] +
                          t['shares'] * t['sell_price'] - t['proceeds']
                          for t in transactions), 2)
    avg_pnl    = round(sum(t['pnl'] for t in transactions) / n_trades, 2) if n_trades else 0
    end_bal    = round(balance, 2)
    total_ret  = round((end_bal - _BT_START_BALANCE) / _BT_START_BALANCE * 100, 2)

    today     = calendar[-1]
    today_idx = len(calendar) - 1
    recommendations = []
    for leader, follower, lag_days, train_r, backtest_r in pairs:
        if today_idx < 1:
            continue
        lc_today = eod.get(leader, {}).get(today)
        lc_prev  = eod.get(leader, {}).get(calendar[today_idx - 1])
        if not lc_today or not lc_prev or lc_prev[1] == 0:
            continue
        leader_ret = (lc_today[1] - lc_prev[1]) / lc_prev[1]
        if leader_ret <= 0:
            continue
        follower_today = eod.get(follower, {}).get(today)
        recommendations.append({
            'leader':              leader,
            'leader_return_pct':   round(leader_ret * 100, 2),
            'follower':            follower,
            'lag_days':            lag_days,
            'train_r':             round(train_r, 4),
            'backtest_r':          round(backtest_r, 4),
            'follower_last_close': round(follower_today[1], 4) if follower_today else None,
            'signal_date':         today,
        })
    recommendations.sort(key=lambda r: r['leader_return_pct'], reverse=True)

    return {
        'id':   cfg['id'],
        'name': cfg['name'],
        'strategy': {
            'min_train_r':        min_train_r,
            'min_backtest_r':     min_backtest_r,
            'min_lag_days':       min_lag_days,
            'no_overlap':         no_overlap,
            'deduplicate_pairs':  deduplicate_pairs,
            'position_size':      _BT_POSITION_SIZE,
            'fee_flat':           _BT_FEE_FLAT,
            'fee_pct':            _BT_FEE_PCT,
            'start_balance':      _BT_START_BALANCE,
            'backtest_start':     '2025-03-01',
            'n_qualifying_pairs': len(pairs),
        },
        'summary': {
            'end_balance':      end_bal,
            'total_return_pct': total_ret,
            'n_trades':         n_trades,
            'n_wins':           n_wins,
            'n_losses':         n_losses,
            'win_rate':         round(n_wins / n_trades * 100, 1) if n_trades else 0,
            'avg_trade_pnl':    avg_pnl,
            'total_fees':       total_fees,
        },
        'balance_series':  balance_series,
        'transactions':    list(reversed(transactions)),
        'recommendations': recommendations,
    }


def _get_backtest(cfg_id):
    """Return cached backtest result, recomputing if stale (once per calendar day)."""
    cfg = next((c for c in BACKTEST_CONFIGS if c['id'] == cfg_id), None)
    if not cfg:
        return None
    cache = _backtest_caches[cfg_id]
    today = datetime.date.today().isoformat()
    if cache['date'] != today or cache['result'] is None:
        result = _run_backtest(cfg)
        if result is None:
            return None
        cache['result'] = result
        cache['date']   = today
    return cache['result']


@app.route('/api/analysis/correlations/backtests')
def api_analysis_correlations_backtests_list():
    """List all backtest configs with summaries (computes each if not cached)."""
    today = datetime.date.today().isoformat()
    out = []
    for cfg in BACKTEST_CONFIGS:
        result = _get_backtest(cfg['id'])
        if result is None:
            out.append({'id': cfg['id'], 'name': cfg['name'],
                        'description': cfg['description'], 'params': cfg['params'],
                        'run_date': None, 'available': False})
        else:
            out.append({'id': cfg['id'], 'name': cfg['name'],
                        'description': cfg['description'], 'params': cfg['params'],
                        'run_date': today, 'available': True,
                        'strategy': result['strategy'], 'summary': result['summary']})
    return jsonify(out)


@app.route('/api/analysis/correlations/backtests/<cfg_id>')
def api_analysis_correlations_backtest_detail(cfg_id):
    """Full backtest detail for a single config id."""
    if not any(c['id'] == cfg_id for c in BACKTEST_CONFIGS):
        return jsonify({'error': 'Unknown backtest id'}), 404
    result = _get_backtest(cfg_id)
    if result is None:
        return jsonify({'error': 'No correlation data available'}), 404
    return jsonify(result)


# Keep old singular endpoint as alias for v1
@app.route('/api/analysis/correlations/backtest')
def api_analysis_correlations_backtest():
    result = _get_backtest('v1')
    if result is None:
        return jsonify({'error': 'No correlation data available'}), 404
    return jsonify(result)


@app.route('/api/analysis/correlations/backtest-sweep')
def api_analysis_correlations_backtest_sweep():
    """Serve pre-computed sweep results from backtest_sweep.json."""
    data = _load_analysis_file('backtest_sweep.json')
    if data is None:
        return jsonify({'error': 'No sweep results — run analysis/backtest_sweep.py first'}), 404
    return jsonify(data)


# ---------------------------------------------------------------------------
# Signal backtests — factor-based cross-sectional strategies
# ---------------------------------------------------------------------------

SIGNAL_BT_CONFIGS = [
    {
        'id': 's1',
        'name': 'Short Interest',
        'description': 'Stocks with elevated short positions tend to outperform (short-squeeze). Buy top-10 by short_pct, hold 3 days.',
        'params': {
            'factor': 'short_pct',
            'lag': 3,
            'direction': 'long_top',
            'top_n': 10,
            'no_overlap': True,
        },
    },
    {
        'id': 's2',
        'name': 'Low Volatility',
        'description': 'High intraday range stocks tend to underperform. Buy the 10 lowest hl_spread stocks, hold 9 days.',
        'params': {
            'factor': 'hl_spread',
            'lag': 9,
            'direction': 'long_bottom',
            'top_n': 10,
            'no_overlap': True,
        },
    },
    {
        'id': 's3',
        'name': 'Mean Reversion',
        'description': 'Buy the most oversold stocks (lowest returns_z20). 20-day mean reversion with 1-day hold.',
        'params': {
            'factor': 'returns_z20',
            'lag': 1,
            'direction': 'long_bottom',
            'top_n': 10,
            'no_overlap': True,
        },
    },
    {
        'id': 's4',
        'name': 'Gap Momentum',
        'description': 'Buy stocks with the largest overnight gap up. Gap continuation, hold 2 days.',
        'params': {
            'factor': 'gap',
            'lag': 2,
            'direction': 'long_top',
            'top_n': 10,
            'no_overlap': True,
        },
    },
    {
        'id': 's5',
        'name': 'Volume Anomaly',
        'description': 'Unusual volume spike (20-day z-score). Buy top-10 volume anomaly stocks, hold 3 days.',
        'params': {
            'factor': 'volume_z20',
            'lag': 3,
            'direction': 'long_top',
            'top_n': 10,
            'no_overlap': True,
        },
    },
    {
        'id': 's7',
        'name': 'Short Interest Z-Score',
        'description': 'Stocks with elevated short interest relative to their own 20-day history tend to underperform. Buy lowest short_z20, lag 1. IC_IR −0.116, 100% same-sign across all lags.',
        'params': {
            'factor': 'short_z20',
            'lag': 1,
            'direction': 'long_bottom',
            'top_n': 10,
            'no_overlap': True,
        },
    },
    {
        'id': 's8',
        'name': 'Volume Spike (5-day)',
        'description': 'Unusual volume relative to the 5-day baseline predicts outperformance. Buy top volume_z5 stocks, lag 4. IC_IR +0.094, 100% same-sign across all lags.',
        'params': {
            'factor': 'volume_z5',
            'lag': 4,
            'direction': 'long_top',
            'top_n': 10,
            'no_overlap': True,
        },
    },
    {
        'id': 's9',
        'name': 'Short Trend (20-day slope)',
        'description': 'Rising short interest trend predicts underperformance. Buy lowest short_slope20 stocks, lag 1. IC_IR −0.047, 95% same-sign — weakest of the directional set.',
        'params': {
            'factor': 'short_slope20',
            'lag': 1,
            'direction': 'long_bottom',
            'top_n': 10,
            'no_overlap': True,
        },
    },
    {
        'id': 's6',
        'name': 'Top-6 Positive Combined',
        'description': 'Union of signals from the 6 highest positive-IC factors: short_pct, returns_1d, gap, volume_z20, volume_z5. Top-8 per factor.',
        'params': {
            'factors': [
                ('short_pct',  3, 'long_top'),
                ('returns_1d', 1, 'long_top'),
                ('gap',        2, 'long_top'),
                ('volume_z20', 3, 'long_top'),
                ('volume_z5',  2, 'long_top'),
            ],
            'top_n_per_factor': 8,
            'no_overlap': True,
        },
    },
    # Fundamentals-based configs (use weekly Yahoo Finance snapshot; factor prefix 'f_')
    {
        'id': 'f1',
        'name': 'Low P/E',
        'description': 'Buy the 10 cheapest stocks by trailing P/E. Value factor — low PE tends to outperform over medium horizons.',
        'params': {
            'factor': 'f_trailing_pe',
            'lag': 5,
            'direction': 'long_bottom',
            'top_n': 10,
            'no_overlap': True,
        },
    },
    {
        'id': 'f2',
        'name': 'High ROE',
        'description': 'Buy the 10 stocks with the highest return on equity. Quality factor — high ROE firms tend to sustain outperformance.',
        'params': {
            'factor': 'f_return_on_equity',
            'lag': 5,
            'direction': 'long_top',
            'top_n': 10,
            'no_overlap': True,
        },
    },
    {
        'id': 'f3',
        'name': 'High Dividend Yield',
        'description': 'Buy the 10 highest dividend yield stocks. Income factor — high yield may signal undervaluation or income support.',
        'params': {
            'factor': 'f_dividend_yield',
            'lag': 5,
            'direction': 'long_top',
            'top_n': 10,
            'no_overlap': True,
        },
    },
    {
        'id': 'f4',
        'name': 'Low Leverage',
        'description': 'Buy the 10 stocks with the lowest debt/equity ratio. Balance sheet quality — low leverage reduces downside risk.',
        'params': {
            'factor': 'f_debt_to_equity',
            'lag': 5,
            'direction': 'long_bottom',
            'top_n': 10,
            'no_overlap': True,
        },
    },
]

_signal_bt_caches = {cfg['id']: {'result': None, 'date': None} for cfg in SIGNAL_BT_CONFIGS}
_signal_bt_lock   = threading.Lock()
_signal_bt_thread = None  # background computation thread

SIGNAL_BT_LOOKBACK = 60  # extra days before backtest start for rolling calculations


def _linreg_slope(ys):
    """Least-squares slope of ys (x = 0..n-1). Returns None if fewer than 2 points."""
    n = len(ys)
    if n < 2:
        return None
    sx = n * (n - 1) / 2
    sx2 = n * (n - 1) * (2 * n - 1) / 6
    sy = sum(ys)
    sxy = sum(i * y for i, y in enumerate(ys))
    denom = n * sx2 - sx * sx
    if denom == 0:
        return None
    return (n * sxy - sx * sy) / denom


def _zscore(vals, window):
    """Z-score of the last value in vals using up to the last `window` values."""
    if len(vals) < 2:
        return None
    w = vals[-window:] if len(vals) >= window else vals
    n = len(w)
    if n < 2:
        return None
    mean = sum(w) / n
    var = sum((x - mean) ** 2 for x in w) / n
    if var <= 0:
        return None
    return (w[-1] - mean) / var ** 0.5


def _compute_factor(factor, history_eod, history_short, date_str, sym_fund=None):
    """
    Compute a scalar factor value for (symbol, date_str).
    history_eod:   list of (open, high, low, close, volume) sorted by date ascending,
                   including date_str at index [-1].
    history_short: list of short_pct values aligned to the same dates, may have None gaps.
    sym_fund:      dict of fundamentals column -> value (weekly snapshot) or None.
    Returns float or None.
    """
    # Fundamentals factors — static weekly snapshot, column name follows 'f_' prefix
    if factor.startswith('f_'):
        if not sym_fund:
            return None
        col = factor[2:]  # strip 'f_' prefix → column name in fundamentals table
        v = sym_fund.get(col)
        return float(v) if v is not None else None

    if not history_eod:
        return None
    cur = history_eod[-1]
    o, h, l, c, vol = cur

    if factor == 'short_pct':
        return history_short[-1] if history_short else None

    if factor == 'hl_spread':
        if not c or c == 0:
            return None
        return (h - l) / c

    if factor == 'gap':
        if len(history_eod) < 2:
            return None
        prev_c = history_eod[-2][3]
        if not prev_c or prev_c == 0:
            return None
        return (o - prev_c) / prev_c

    if factor == 'returns_1d':
        if len(history_eod) < 2:
            return None
        prev_c = history_eod[-2][3]
        if not prev_c or prev_c == 0:
            return None
        return (c - prev_c) / prev_c

    if factor == 'returns_z20':
        if len(history_eod) < 3:
            return None
        rets = []
        for i in range(1, len(history_eod)):
            pc = history_eod[i - 1][3]
            cc = history_eod[i][3]
            if pc and cc and pc != 0:
                rets.append((cc - pc) / pc)
        return _zscore(rets, 20)

    if factor == 'volume_z20':
        vols = [r[4] for r in history_eod if r[4] and r[4] > 0]
        return _zscore(vols, 20)

    if factor == 'volume_z5':
        vols = [r[4] for r in history_eod if r[4] and r[4] > 0]
        return _zscore(vols, 5)

    if factor == 'short_z20':
        vals = [v for v in history_short if v is not None]
        return _zscore(vals, 20)

    if factor == 'returns_slope20':
        if len(history_eod) < 3:
            return None
        rets = []
        for i in range(1, len(history_eod)):
            pc = history_eod[i - 1][3]
            cc = history_eod[i][3]
            if pc and cc and pc != 0:
                rets.append((cc - pc) / pc)
        w = rets[-20:] if len(rets) >= 20 else rets
        return _linreg_slope(w)

    if factor == 'short_slope20':
        vals = [v for v in history_short if v is not None]
        w = vals[-20:] if len(vals) >= 20 else vals
        return _linreg_slope(w)

    return None


def _run_signal_backtest(cfg):
    """Compute a factor-based cross-sectional backtest."""
    params     = cfg['params']
    no_overlap = params.get('no_overlap', True)
    multi      = 'factors' in params

    # Load all current symbols
    sconn = sqlite3.connect(DATABASE, check_same_thread=False)
    # Restrict to equities and ETFs: exclude market indices (XAO etc.) and ASX-listed options.
    # 'Delisted' industry symbols that are still current=1 are mostly legitimate ETFs/LICs
    # (data quality issue in the symbols table) so they are retained.
    symbols_rows = sconn.execute(
        "SELECT symbol FROM symbols WHERE current = 1 AND industry != 'Index' "
        "AND symbol NOT IN (SELECT option_symbol FROM asx_options)"
    ).fetchall()
    all_symbols = [r[0] for r in symbols_rows]
    if not all_symbols:
        sconn.close()
        return None

    load_start_ts = BACKTEST_START_TS - SIGNAL_BT_LOOKBACK * 86400

    # Load EOD — GROUP BY deduplicates split/consolidation double-rows.
    # MAX(open/close) selects the split-adjusted (higher) price for consolidations,
    # giving a continuous price series across corporate events.
    ph = ','.join('?' * len(all_symbols))
    eod_rows = sconn.execute(
        f"SELECT symbol, date, MAX(open), MAX(high), MAX(low), MAX(close), MAX(volume) FROM endofday "
        f"WHERE symbol IN ({ph}) AND date >= ? GROUP BY symbol, date ORDER BY symbol, date ASC",
        all_symbols + [load_start_ts]
    ).fetchall()

    # Load shorts — GROUP BY to match EOD dedup approach
    short_rows = sconn.execute(
        f"SELECT symbol, date, MAX(short) FROM shorts "
        f"WHERE symbol IN ({ph}) AND date >= ? GROUP BY symbol, date ORDER BY symbol, date ASC",
        all_symbols + [load_start_ts]
    ).fetchall()

    # Load fundamentals snapshot (for f_ prefixed factors)
    _FUND_COLS = [
        'trailing_pe','forward_pe','price_to_book','enterprise_to_ebitda',
        'profit_margins','return_on_equity','return_on_assets','revenue_growth',
        'earnings_growth','dividend_yield','payout_ratio','debt_to_equity',
        'current_ratio','beta',
    ]
    sym_fund = {}
    try:
        fund_rows = sconn.execute(
            f"SELECT symbol, {','.join(_FUND_COLS)} FROM fundamentals"
            f" WHERE symbol IN ({ph})"
            f" AND (symbol, date) IN (SELECT symbol, MAX(date) FROM fundamentals GROUP BY symbol)",
            all_symbols
        ).fetchall()
        for row in fund_rows:
            sym_fund[row[0]] = dict(zip(_FUND_COLS, row[1:]))
    except Exception:
        pass  # fundamentals table may not be populated yet

    sconn.close()

    # Build per-symbol dicts: date_str -> (o,h,l,c,v) and date_str -> short_pct
    sym_eod   = {}  # symbol -> [(date_str, o,h,l,c,v)]
    sym_short = {}  # symbol -> {date_str: short_pct}

    date_set_all = set()
    for symbol, ts, o, h, l, c, vol in eod_rows:
        ds = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
        sym_eod.setdefault(symbol, []).append((ds, o, h, l, c, vol))
        date_set_all.add(ds)

    for symbol, ts, short_pct in short_rows:
        ds = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
        sym_short.setdefault(symbol, {})[ds] = short_pct

    full_calendar = sorted(date_set_all)
    backtest_start_str = datetime.datetime.utcfromtimestamp(BACKTEST_START_TS).strftime('%Y-%m-%d')
    backtest_calendar = [d for d in full_calendar if d >= backtest_start_str]
    if len(backtest_calendar) < 3:
        return None

    # Pre-build sorted date lists per symbol for fast history lookup
    sym_dates = {sym: [row[0] for row in rows] for sym, rows in sym_eod.items()}
    sym_eod_vals = {
        sym: {row[0]: (row[1], row[2], row[3], row[4], row[5]) for row in rows}
        for sym, rows in sym_eod.items()
    }

    # Detect price discontinuities caused by partially-applied split/consolidation adjustments.
    # When fetch_splits re-downloads adjusted history, some rows may be missed, leaving the
    # unadjusted price alongside an adjusted price on adjacent dates (e.g., $0.002 followed by
    # $0.125 the next day). Flag any date where close/prev_close > 4× or < 0.25× as a
    # "bad date". Trades spanning a bad date are excluded.
    _DISCONTINUITY_THRESHOLD = 4.0
    sym_bad_dates = {}  # symbol -> set of date strings with price discontinuities
    for sym, rows in sym_eod.items():
        bad = set()
        prev_close = None
        for ds, o, h, l, c, vol in rows:
            if prev_close and c and prev_close > 0:
                ratio = c / prev_close
                if ratio > _DISCONTINUITY_THRESHOLD or ratio < 1.0 / _DISCONTINUITY_THRESHOLD:
                    bad.add(ds)
            prev_close = c if c else prev_close
        if bad:
            sym_bad_dates[sym] = bad

    def get_history(symbol, up_to_date, n=25):
        """Return last n EOD rows up to and including up_to_date."""
        dates = sym_dates.get(symbol, [])
        idx = bisect.bisect_right(dates, up_to_date)
        slice_dates = dates[max(0, idx - n):idx]
        vals_map = sym_eod_vals.get(symbol, {})
        short_map = sym_short.get(symbol, {})
        eod_h   = [vals_map[d] for d in slice_dates if d in vals_map]
        short_h = [short_map.get(d) for d in slice_dates]
        return eod_h, short_h

    transactions = []
    symbol_busy_until = {}  # symbol -> sell_date str (no_overlap guard)

    if multi:
        factors_list   = params['factors']   # [(factor, lag, direction), ...]
        top_n_per      = params['top_n_per_factor']
    else:
        factors_list   = [(params['factor'], params['lag'], params['direction'])]
        top_n_per      = params['top_n']

    for i, signal_date in enumerate(backtest_calendar):
        if i + 1 >= len(backtest_calendar):
            break

        buy_date = backtest_calendar[i + 1]

        for factor, lag, direction in factors_list:
            if i + lag >= len(backtest_calendar):
                continue
            sell_date = backtest_calendar[i + lag]

            # Compute factor for all symbols on signal_date
            factor_vals = {}
            for symbol in all_symbols:
                # Skip if the symbol has a price discontinuity in its recent history window:
                # a corporate event within the last 25 trading days would corrupt rolling calcs.
                bad = sym_bad_dates.get(symbol)
                if bad:
                    sym_d = sym_dates.get(symbol, [])
                    idx_end = bisect.bisect_right(sym_d, signal_date)
                    window_start = idx_end - 25
                    if any(sym_d[k] in bad for k in range(max(0, window_start), idx_end)):
                        continue

                eod_h, short_h = get_history(symbol, signal_date, n=25)
                if not eod_h:
                    continue
                # Only use symbol if it has EOD data ON signal_date
                if eod_h[-1][3] is None:  # close must exist
                    continue
                # Check latest date in history is signal_date
                last_date_in_hist = sym_dates.get(symbol, [])
                if last_date_in_hist:
                    ld = last_date_in_hist[bisect.bisect_right(last_date_in_hist, signal_date) - 1] if bisect.bisect_right(last_date_in_hist, signal_date) > 0 else None
                    if ld != signal_date:
                        continue  # no data on this exact date
                val = _compute_factor(factor, eod_h, short_h, signal_date,
                                     sym_fund=sym_fund.get(symbol))
                if val is None:
                    continue
                factor_vals[symbol] = val

            if len(factor_vals) < 2:
                continue

            # Cross-sectional rank [0..1]
            sorted_syms = sorted(factor_vals, key=lambda s: factor_vals[s])
            rank = {s: i / (len(sorted_syms) - 1) for i, s in enumerate(sorted_syms)}

            if direction == 'long_top':
                selected = sorted_syms[-top_n_per:]
            else:  # long_bottom
                selected = sorted_syms[:top_n_per]

            for symbol in selected:
                if no_overlap and buy_date < symbol_busy_until.get(symbol, ''):
                    continue

                # Skip any trade that spans a price discontinuity (corporate event artifact).
                # Check all dates from buy_date through sell_date inclusive.
                bad = sym_bad_dates.get(symbol)
                if bad:
                    sym_d = sym_dates.get(symbol, [])
                    lo = bisect.bisect_left(sym_d, buy_date)
                    hi = bisect.bisect_right(sym_d, sell_date)
                    if any(sym_d[k] in bad for k in range(lo, hi)):
                        continue

                buy_vals  = sym_eod_vals.get(symbol, {}).get(buy_date)
                sell_vals = sym_eod_vals.get(symbol, {}).get(sell_date)
                if not buy_vals or not sell_vals:
                    continue

                buy_price  = buy_vals[0]   # open
                sell_price = sell_vals[3]  # close
                if not buy_price or buy_price <= 0:
                    continue

                shares = int((_BT_POSITION_SIZE - _BT_FEE_FLAT) / buy_price)
                buy_fee = _bt_fee(shares * buy_price)
                while shares > 0 and shares * buy_price + buy_fee > _BT_POSITION_SIZE:
                    shares -= 1
                    buy_fee = _bt_fee(shares * buy_price)
                if shares < 1:
                    continue

                sell_fee = _bt_fee(shares * sell_price)
                cost     = shares * buy_price + buy_fee
                proceeds = shares * sell_price - sell_fee
                pnl      = proceeds - cost

                if no_overlap and sell_date > symbol_busy_until.get(symbol, ''):
                    symbol_busy_until[symbol] = sell_date

                transactions.append({
                    'signal_date':   signal_date,
                    'buy_date':      buy_date,
                    'sell_date':     sell_date,
                    'symbol':        symbol,
                    'factor':        factor,
                    'factor_value':  round(factor_vals[symbol], 5),
                    'rank':          round(rank[symbol], 3),
                    'lag_days':      lag,
                    'buy_price':     round(buy_price, 4),
                    'sell_price':    round(sell_price, 4),
                    'shares':        shares,
                    'cost':          round(cost, 2),
                    'proceeds':      round(proceeds, 2),
                    'pnl':           round(pnl, 2),
                })

    transactions.sort(key=lambda t: (t['sell_date'], t['signal_date']))

    balance = _BT_START_BALANCE
    balance_by_date = {}
    for t in transactions:
        balance += t['pnl']
        t['balance_after'] = round(balance, 2)
        balance_by_date[t['sell_date']] = round(balance, 2)

    balance_series = [{'date': d, 'balance': b} for d, b in sorted(balance_by_date.items())]
    balance_series.insert(0, {'date': backtest_calendar[0], 'balance': _BT_START_BALANCE})

    n_trades   = len(transactions)
    n_wins     = sum(1 for t in transactions if t['pnl'] > 0)
    n_losses   = n_trades - n_wins
    total_fees = round(sum(
        t['cost'] - t['shares'] * t['buy_price'] +
        t['shares'] * t['sell_price'] - t['proceeds']
        for t in transactions
    ), 2)
    avg_pnl    = round(sum(t['pnl'] for t in transactions) / n_trades, 2) if n_trades else 0
    end_bal    = round(balance, 2)
    total_ret  = round((end_bal - _BT_START_BALANCE) / _BT_START_BALANCE * 100, 2)

    # Sharpe: annualised using daily P&L / start balance as daily return
    daily_pnl = {}
    for t in transactions:
        daily_pnl[t['sell_date']] = daily_pnl.get(t['sell_date'], 0.0) + t['pnl']
    daily_rets = [v / _BT_START_BALANCE for v in daily_pnl.values()]
    sharpe = None
    if len(daily_rets) >= 5:
        mean_r = sum(daily_rets) / len(daily_rets)
        var_r  = sum((r - mean_r) ** 2 for r in daily_rets) / len(daily_rets)
        std_r  = var_r ** 0.5
        if std_r > 0:
            sharpe = round(mean_r / std_r * (252 ** 0.5), 3)

    # Max drawdown
    peak = _BT_START_BALANCE
    max_dd = 0.0
    for pt in balance_series:
        b = pt['balance']
        if b > peak:
            peak = b
        dd = (peak - b) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
    max_dd = round(max_dd, 2)

    strategy_info = {
        'position_size':   _BT_POSITION_SIZE,
        'fee_flat':        _BT_FEE_FLAT,
        'fee_pct':         _BT_FEE_PCT,
        'start_balance':   _BT_START_BALANCE,
        'backtest_start':  '2025-03-01',
        'no_overlap':      no_overlap,
    }
    if multi:
        strategy_info['factors'] = [
            {'factor': f, 'lag': lg, 'direction': d}
            for f, lg, d in factors_list
        ]
        strategy_info['top_n_per_factor'] = params['top_n_per_factor']
    else:
        strategy_info['factor']    = params['factor']
        strategy_info['lag']       = params['lag']
        strategy_info['direction'] = params['direction']
        strategy_info['top_n']     = params['top_n']

    return {
        'id':      cfg['id'],
        'name':    cfg['name'],
        'strategy': strategy_info,
        'summary': {
            'end_balance':       end_bal,
            'total_return_pct':  total_ret,
            'n_trades':          n_trades,
            'n_wins':            n_wins,
            'n_losses':          n_losses,
            'win_rate':          round(n_wins / n_trades * 100, 1) if n_trades else 0,
            'avg_trade_pnl':     avg_pnl,
            'total_fees':        total_fees,
            'sharpe_ratio':      sharpe,
            'max_drawdown_pct':  max_dd,
        },
        'balance_series': balance_series,
        'transactions':   list(reversed(transactions)),
    }


def _bg_compute_signal_backtests():
    """Compute all signal backtests sequentially in a background thread."""
    today = datetime.date.today().isoformat()
    for cfg in SIGNAL_BT_CONFIGS:
        try:
            result = _run_signal_backtest(cfg)
            if result:
                with _signal_bt_lock:
                    _signal_bt_caches[cfg['id']]['result'] = result
                    _signal_bt_caches[cfg['id']]['date']   = today
        except Exception as e:
            print(f'Signal backtest {cfg["id"]} error: {e}')


def _ensure_signal_bt_computing():
    """Start background computation if cache is stale and no thread is running."""
    global _signal_bt_thread
    today = datetime.date.today().isoformat()
    with _signal_bt_lock:
        stale = any(c['date'] != today or c['result'] is None
                    for c in _signal_bt_caches.values())
        running = _signal_bt_thread is not None and _signal_bt_thread.is_alive()
    if stale and not running:
        t = threading.Thread(target=_bg_compute_signal_backtests, daemon=True)
        t.start()
        with _signal_bt_lock:
            _signal_bt_thread = t


def _get_signal_bt_cached(cfg_id):
    """Return cached result for cfg_id, or None if not yet computed."""
    today = datetime.date.today().isoformat()
    with _signal_bt_lock:
        cache = _signal_bt_caches.get(cfg_id, {})
        return cache.get('result') if cache.get('date') == today else None


# Kick off background computation at startup so first page load is instant.
_ensure_signal_bt_computing()


@app.route('/api/analysis/signal-backtests')
def api_analysis_signal_backtests_list():
    _ensure_signal_bt_computing()
    today = datetime.date.today().isoformat()
    computing = _signal_bt_thread is not None and _signal_bt_thread.is_alive()
    out = []
    for cfg in SIGNAL_BT_CONFIGS:
        result = _get_signal_bt_cached(cfg['id'])
        if result is None:
            out.append({'id': cfg['id'], 'name': cfg['name'],
                        'description': cfg['description'], 'params': cfg['params'],
                        'run_date': None, 'available': False, 'computing': computing})
        else:
            out.append({'id': cfg['id'], 'name': cfg['name'],
                        'description': cfg['description'], 'params': cfg['params'],
                        'run_date': today, 'available': True, 'computing': False,
                        'strategy': result['strategy'], 'summary': result['summary']})
    return jsonify(out)


@app.route('/api/analysis/signal-backtests/<cfg_id>')
def api_analysis_signal_backtest_detail(cfg_id):
    if not any(c['id'] == cfg_id for c in SIGNAL_BT_CONFIGS):
        return jsonify({'error': 'Unknown signal backtest id'}), 404
    _ensure_signal_bt_computing()
    result = _get_signal_bt_cached(cfg_id)
    if result is None:
        return jsonify({'computing': True}), 202
    return jsonify(result)


@app.route('/symbol-changes')
def api_symbol_changes():
    """Look up rename for a symbol. ?symbol=EMS → {found, new_symbol?, effective_date?}"""
    symbol = request.args.get('symbol', '').strip().upper()
    if not symbol:
        abort(400)
    c = stocks.cursor()
    try:
        row = c.execute(
            'SELECT new_symbol, effective_date FROM symbol_changes'
            ' WHERE old_symbol = ? ORDER BY effective_date DESC LIMIT 1',
            (symbol,)
        ).fetchone()
    except Exception:
        return jsonify({'found': False})
    if row:
        return jsonify({'found': True, 'symbol': symbol,
                        'new_symbol': row[0], 'effective_date': row[1]})
    return jsonify({'found': False})


@app.route('/options')
def api_options():
    """Options for a symbol. ?symbol=BHP → [{option_symbol, expiry, exercise, eod_price, eod_date, ...}, ...]"""
    symbol = request.args.get('symbol', '').strip().upper()
    c = stocks.cursor()
    eod_join = (
        " LEFT JOIN endofday e ON e.symbol = o.option_symbol"
        " AND e.date = (SELECT MAX(date) FROM endofday WHERE symbol = o.option_symbol)"
    )
    cols = ("o.option_symbol, o.expiry, o.exercise, o.share_symbol, o.share_name, o.note, o.fetched_at,"
            " e.close AS eod_price, date(e.date, 'unixepoch', '+10 hours') AS eod_date")
    try:
        if symbol:
            rows = c.execute(
                f'SELECT {cols} FROM asx_options o{eod_join}'
                ' WHERE o.share_symbol = ? ORDER BY o.expiry, o.exercise',
                (symbol,)
            ).fetchall()
        else:
            rows = c.execute(
                f'SELECT {cols} FROM asx_options o{eod_join}'
                ' ORDER BY o.share_symbol, o.expiry, o.exercise'
            ).fetchall()
    except Exception:
        return jsonify([])
    return jsonify([{
        'option_symbol': r[0], 'expiry': r[1], 'exercise': r[2],
        'share_symbol': r[3], 'share_name': r[4], 'note': r[5], 'fetched_at': r[6],
        'eod_price': r[7], 'eod_date': r[8],
    } for r in rows])


@app.route('/api/quotes', methods=['POST'])
def api_quotes():
    """Batch live prices from Yahoo Finance, each cached for 5 minutes."""
    symbols = [s.strip().upper() for s in (request.get_json(force=True) or {}).get('symbols', [])[:60]]
    now_ts = time.time()
    result = {}
    stale = []
    for sym in symbols:
        entry = _quote_cache.get(sym)
        if entry and now_ts - entry[0] < _QUOTE_TTL:
            result[sym] = entry[1]
        else:
            stale.append(sym)

    def _is_option(sym):
        """ASX options have 'O' as their fourth character (e.g. GNMO, EXROB)."""
        return len(sym) >= 4 and sym[3] == 'O'

    def _fetch_asx(sym):
        """Fetch latest price from ASX chart API."""
        url = f'https://www.asx.com.au/asx/1/chart/highcharts?asx_code={sym}&complete=true'
        resp = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        rows = resp.json()
        if not (rows and isinstance(rows, list) and len(rows) >= 1):
            return None
        price = float(rows[-1][4])
        prev  = float(rows[-2][4]) if len(rows) >= 2 else None
        return {
            'price':      round(price, 4),
            'prev_close': round(prev, 4) if prev else None,
            'change':     round(price - prev, 4) if prev else None,
            'change_pct': round((price - prev) / prev * 100, 2) if prev else None,
            'source':     'asx',
        }

    def fetch_one(sym):
        if _is_option(sym):
            # Try Markit API first for options (live data via authenticated endpoint)
            markit_token = os.environ.get('MARKIT_TOKEN')
            if markit_token:
                try:
                    resp = requests.get(
                        f'https://asx.api.markitdigital.com/asx-research/1.0/companies/{sym}/header',
                        headers={'Authorization': f'Bearer {markit_token}'},
                        timeout=10,
                    )
                    if resp.ok:
                        d = resp.json().get('data', {})
                        if d.get('priceLast'):
                            return sym, {
                                'price':      round(float(d['priceLast']), 4),
                                'prev_close': round(float(d['priceLast']) - float(d.get('priceChange', 0)), 4),
                                'change':     round(float(d.get('priceChange', 0)), 4),
                                'change_pct': round(float(d.get('priceChangePercent', 0)), 2),
                                'source':     'markit',
                            }
                except Exception:
                    pass
            # Fallback to Yahoo Finance
            yf_ticker = f'{sym}.AX'
            try:
                fi = yf.Ticker(yf_ticker).fast_info
                price = fi.last_price
                prev  = fi.previous_close
                if price:
                    return sym, {
                        'price':      round(float(price), 4),
                        'prev_close': round(float(prev), 4) if prev else None,
                        'change':     round(float(price - prev), 4) if prev else None,
                        'change_pct': round(float((price - prev) / prev * 100), 2) if prev else None,
                        'source':     'yf',
                    }
            except Exception:
                pass
            # Final fallback to ASX chart API
            try:
                return sym, _fetch_asx(sym)
            except Exception:
                return sym, None
        # Non-options: use Yahoo Finance
        yf_ticker = '^AORD' if sym == 'XAO' else f'{sym}.AX'
        try:
            fi = yf.Ticker(yf_ticker).fast_info
            price = fi.last_price
            prev  = fi.previous_close
            if price:
                return sym, {
                    'price':      round(float(price), 3),
                    'prev_close': round(float(prev), 3) if prev else None,
                    'change':     round(float(price - prev), 3) if prev else None,
                    'change_pct': round(float((price - prev) / prev * 100), 2) if prev else None,
                }
        except Exception:
            pass
        return sym, None

    if stale:
        with ThreadPoolExecutor(max_workers=10) as ex:
            for sym, data in ex.map(fetch_one, stale):
                if data:
                    _quote_cache[sym] = (now_ts, data)
                    result[sym] = data

    return jsonify(result)


@app.route('/api/quote/<symbol>')
def api_quote(symbol):
    """Live price from Markit (for options), Yahoo Finance, or ASX chart API, cached for 5 minutes."""
    symbol = symbol.strip().upper()
    now_ts = time.time()
    entry = _quote_cache.get(symbol)
    if entry and now_ts - entry[0] < _QUOTE_TTL:
        return jsonify(entry[1])

    # Check if this is an option (4th character is 'O')
    if len(symbol) >= 4 and symbol[3] == 'O':
        data = None
        # Try Markit API first for options (live data via authenticated endpoint)
        markit_token = os.environ.get('MARKIT_TOKEN')
        if markit_token:
            try:
                resp = requests.get(
                    f'https://asx.api.markitdigital.com/asx-research/1.0/companies/{symbol}/header',
                    headers={'Authorization': f'Bearer {markit_token}'},
                    timeout=10,
                )
                if resp.ok:
                    d = resp.json().get('data', {})
                    if d.get('priceLast'):
                        data = {
                            'symbol':     symbol,
                            'price':      round(float(d['priceLast']), 4),
                            'prev_close': round(float(d['priceLast']) - float(d.get('priceChange', 0)), 4),
                            'change':     round(float(d.get('priceChange', 0)), 4),
                            'change_pct': round(float(d.get('priceChangePercent', 0)), 2),
                            'source':     'markit',
                        }
            except Exception:
                pass

        # Fallback to Yahoo Finance if Markit doesn't have data
        if not data:
            try:
                yf_ticker = f'{symbol}.AX'
                fi = yf.Ticker(yf_ticker).fast_info
                price = fi.last_price
                prev  = fi.previous_close
                if price:
                    data = {
                        'symbol':     symbol,
                        'price':      round(float(price), 4),
                        'prev_close': round(float(prev), 4) if prev else None,
                        'change':     round(float(price - prev), 4) if prev else None,
                        'change_pct': round(float((price - prev) / prev * 100), 2) if prev else None,
                        'source':     'yf',
                    }
            except Exception:
                pass

        # Fallback to ASX chart API if neither Markit nor Yahoo Finance have data
        if not data:
            try:
                url = f'https://www.asx.com.au/asx/1/chart/highcharts?asx_code={symbol}&complete=true'
                resp = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
                rows = resp.json()
                if rows and isinstance(rows, list) and len(rows) >= 1:
                    price = float(rows[-1][4])
                    prev  = float(rows[-2][4]) if len(rows) >= 2 else None
                    data = {
                        'symbol':     symbol,
                        'price':      round(price, 4),
                        'prev_close': round(prev, 4) if prev else None,
                        'change':     round(price - prev, 4) if prev else None,
                        'change_pct': round((price - prev) / prev * 100, 2) if prev else None,
                        'source':     'asx',
                    }
            except Exception:
                pass

        if data:
            _quote_cache[symbol] = (now_ts, data)
            return jsonify(data)
        abort(503)

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


@app.route('/api/ib/price/<symbol>')
def api_ib_price(symbol):
    """Last price from IB Gateway. Strips .AX suffix; returns last or close price."""
    symbol = symbol.strip().upper().removesuffix('.AX')
    try:
        import asyncio, random
        asyncio.set_event_loop(asyncio.new_event_loop())
        from ib_insync import IB, Stock, Contract
        ib = IB()
        ib.connect('127.0.0.1', 4001, clientId=random.randint(100, 199), readonly=True, timeout=5)
        ib.reqMarketDataType(1)
        is_warrant = len(symbol) >= 4 and symbol[3] == 'O'
        if is_warrant:
            contract = Contract(secType='WAR', localSymbol=symbol, exchange='ASX', currency='AUD')
        else:
            contract = Stock(symbol, 'ASX', 'AUD')
        if not ib.qualifyContracts(contract):
            ib.disconnect()
            return jsonify({'error': f'not found: {symbol}'}), 404
        ticker = ib.reqMktData(contract, genericTickList='', snapshot=False)
        ib.sleep(2)

        def _price(val):
            import math
            if val is None or (isinstance(val, float) and (math.isnan(val) or val == -1.0)):
                return None
            return val

        last  = _price(ticker.last)
        close = _price(ticker.close)
        price = last if last is not None else close

        ib.cancelMktData(contract)
        ib.disconnect()

        if price is None:
            return jsonify({'error': 'no price available'}), 503
        return jsonify({'symbol': symbol, 'price': round(float(price), 3), 'source': 'ib'})
    except Exception as e:
        try:
            ib.disconnect()
        except Exception:
            pass
        app.logger.warning('api_ib_price %s failed: %s', symbol, e)
        return jsonify({'error': str(e)}), 503
