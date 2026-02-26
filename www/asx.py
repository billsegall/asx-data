#! /usr/bin/env python3
# Copyright (c) 2019-2021, Bill Segall
# All rights reserved. See LICENSE for details.

# Local
import stockdb

# System
import datetime, math, os, sqlite3, time
from flask import Flask, jsonify, request, render_template, send_from_directory

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
default_date_min = date2human(xao_date_max - 365 * 24 * 60 * 60)  # One year
default_date_max = date2human(xao_date_max)
print("Data available from %s to %s" % (date2human(xao_date_min), date2human(xao_date_max)))


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')


@app.route('/')
def index():
    return render_template('index.html',
                           default_start=default_date_min,
                           default_end=default_date_max)


@app.route('/stock')
@app.route('/stock/<symbol>')
def stock(symbol=None):
    if symbol is None:
        symbol = request.args.get('symbol', '').strip().upper()
    else:
        symbol = symbol.strip().upper()

    if not symbol:
        return render_template('index.html',
                               default_start=default_date_min,
                               default_end=default_date_max)

    start = request.args.get('start')
    end = request.args.get('end')

    # Default to last year of this symbol's own data
    if not start or not end:
        c = stocks.cursor()
        c.execute('SELECT max(date) FROM endofday WHERE symbol = ?', (symbol,))
        row = c.fetchone()
        sym_max = row[0] if row and row[0] else xao_date_max
        if not end:
            end = date2human(sym_max)
        if not start:
            start = date2human(sym_max - 365 * 24 * 60 * 60)

    try:
        time.mktime(time.strptime(start, '%Y%m%d'))
    except Exception:
        start = default_date_min
    try:
        time.mktime(time.strptime(end, '%Y%m%d'))
    except Exception:
        end = default_date_max

    name, industry, mcap = stocks.LookupSymbol(symbol)
    return render_template('stock.html',
                           symbol=symbol,
                           name=name or symbol,
                           industry=industry or '',
                           mcap=millify(mcap) if mcap else '',
                           start=start,
                           end=end)


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

    name, industry, mcap = stocks.LookupSymbol(symbol)

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
            'mcap': mcap,
        },
        'ohlcv': ohlcv,
        'xao': xao,
        'shorts': shorts,
    })


@app.route('/shorts-historical')
def shorts_historical():
    return render_template('shorts-historical.html')


@app.route('/api/shorts-historical')
def api_shorts_historical():
    c = stocks.cursor()
    c.execute('SELECT symbol, date, max(short) FROM shorts WHERE length(symbol) = 3 GROUP BY symbol ORDER BY short DESC')
    rows = [{'symbol': r[0], 'date': date2human(r[1]), 'short': r[2]} for r in c.fetchall()]
    return jsonify(rows)


@app.route('/shorts-now')
def shorts_now():
    return render_template('shorts-now.html')


@app.route('/api/shorts-now')
def api_shorts_now():
    c = stocks.cursor()
    c.execute('SELECT symbol, max(date), short FROM shorts WHERE length(symbol) = 3 GROUP BY symbol ORDER BY date DESC, short DESC')
    rows = [{'symbol': r[0], 'date': date2human(r[1]), 'short': r[2]} for r in c.fetchall()]
    return jsonify(rows)


@app.route('/privacy')
def privacy():
    return render_template('privacy.html')

