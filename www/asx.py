#! /usr/bin/env python3
# Copyright (c) 2019-2021, Bill Segall
# All rights reserved. See LICENSE for details.

# Local
import stockdb

# System
import datetime, math, os, time
from flask import Flask, abort, jsonify, request, render_template, send_from_directory

app = Flask(__name__)

# Application config
app.config.update(
    DATABASE = os.environ.get('DATABASE', '../stockdb/stockdb.db')
)

## Utility functions

def date2human(date):
    t = datetime.datetime.fromtimestamp(date)
    return t.strftime('%Y%m%d')


def millify(n):
    millnames = ['',' Thousand',' Million',' Billion',' Trillion']
    n = float(n)
    millidx = max(0,min(len(millnames)-1, int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))
    return '{:.0f}{}'.format(n / 10**(3 * millidx), millnames[millidx])


# Open our database and grab some useful info from it
stocks = stockdb.StockDB(app.config['DATABASE'], False)
c = stocks.cursor()
c.execute('SELECT min(date), max(date) FROM endofday where symbol = "XAO"')
xao_date_min, xao_date_max = c.fetchone()
print("Data available from %s to %s" % (date2human(xao_date_min), date2human(xao_date_max)))


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/stock')
@app.route('/stock/<symbol>')
def stock(symbol=None):
    if symbol is None:
        symbol = request.args.get('symbol', '').strip().upper()
    else:
        symbol = symbol.strip().upper()

    if not symbol:
        return render_template('index.html')

    c = stocks.cursor()
    if not c.execute('SELECT 1 FROM endofday WHERE symbol = ? LIMIT 1', (symbol,)).fetchone():
        abort(404)

    name, industry, shares = stocks.LookupSymbol(symbol)
    mcap = None
    if shares:
        lc = stocks.cursor()
        row = lc.execute('SELECT close FROM endofday WHERE symbol = ? ORDER BY date DESC LIMIT 1', (symbol,)).fetchone()
        if row:
            mcap = shares * row[0]
    return render_template('stock.html',
                           symbol=symbol,
                           name=name or symbol,
                           industry=industry or '',
                           mcap=millify(mcap) if mcap else '')


@app.context_processor
def utility_processor():
    def date2human(date):
        t = datetime.datetime.fromtimestamp(date)
        return t.strftime('%d/%m/%Y')
    return dict(date2human=date2human)


@app.route('/api/stock/<symbol>')
def api_stock(symbol):
    symbol = symbol.strip().upper()
    start_str = request.args.get('start')
    end_str = request.args.get('end')

    try:
        start_ts = time.mktime(time.strptime(start_str, '%Y%m%d')) if start_str else 0
    except Exception:
        start_ts = 0
    try:
        end_ts = time.mktime(time.strptime(end_str, '%Y%m%d')) if end_str else time.time()
    except Exception:
        end_ts = time.time()

    c = stocks.cursor()

    name, industry, shares = stocks.LookupSymbol(symbol)
    mcap = None
    if shares:
        lc = stocks.cursor()
        row = lc.execute('SELECT close FROM endofday WHERE symbol = ? ORDER BY date DESC LIMIT 1', (symbol,)).fetchone()
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
    shorts = [[int(r[0]) * 1000, r[1]] for r in c.fetchall()]

    return jsonify({
        'symbol': symbol,
        'info': {
            'name': name,
            'industry': industry,
            'mcap': millify(mcap) if mcap else None,
        },
        'ohlcv': ohlcv,
        'xao': xao,
        'shorts': shorts,
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


@app.route('/shorts-now')
def shorts_now():
    return render_template('shorts-now.html')


@app.route('/api/shorts-now')
def api_shorts_now():
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


@app.route('/privacy')
def privacy():
    return render_template('privacy.html')


@app.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404

