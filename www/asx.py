#! /usr/bin/env python3
# Copyright (c) 2019-2021, Bill Segall
# All rights reserved. See LICENSE for details.

# Local
import stockdb

# System
import atexit, datetime, io, math, os, random, sqlite3, time
from flask import Flask, Response, g, request, render_template, send_from_directory
from flask_wtf import FlaskForm
from wtforms import StringField, validators

import matplotlib
matplotlib.use('Agg') # Don't require an X Server

from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt

# To change default form value after instantiation.
# There really should be a better way.
from werkzeug.datastructures import MultiDict

app = Flask(__name__)

# Application config
app.config.update(
    SECRET_KEY =          '926b93f2a3301883826827209a1623d4c326f21b',
    WTF_CSRF_SECRET_KEY = '926b93f2a3301883826827209a1623d4c326f21b',
    DATABASE = '../stockdb/stockdb.db'
)

## Utility functions

# Not very human, will ultimately need internationalization
def date2human(date):
    t = datetime.datetime.fromtimestamp(date)
    return t.strftime('%Y%m%d')


def millify(n):
    millnames = ['',' Thousand',' Million',' Billion',' Trillion']
    n = float(n)
    millidx = max(0,min(len(millnames)-1, int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))
    return '{:.0f}{}'.format(n / 10**(3 * millidx), millnames[millidx])

def symbolinfo(symbol):
    if symbol == None or len(symbol) == 0:
        return "NULL Symbol"

    name, industry, mcap = stocks.LookupSymbol(symbol)
    #print(name, industry, mcap)
    if name == None:
        return "Symbol lookup failed"
    else:
        return name + ', Industry: ' + industry + ', Market Cap: $' + millify(mcap)

# Open our database and grab some useful info from it
stocks = stockdb.StockDB(app.config['DATABASE'], False)
c = stocks.cursor()
c.execute('SELECT min(date), max(date) FROM endofday where symbol = "XAO"')
xao_date_min, xao_date_max, = c.fetchone()
print(xao_date_min, xao_date_max)
default_date_min = date2human(xao_date_max - 365 * 24 * 60 * 60) # One year
default_date_max = date2human(xao_date_max)
print("Data available from %s to %s" %(date2human(xao_date_min), date2human(xao_date_max)))

# Default form data fields done as multidict so it can be updated
formdata = MultiDict({'symbol' : "", 'min' : default_date_min, 'max' : default_date_max, 'desc' : 'Choose Symbol'})

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

class StockForm(FlaskForm):
        symbol = StringField('Symbol', default=formdata['symbol'], validators=[validators.DataRequired(), validators.Length(min=3, max=5)])
        start = StringField('From', default=formdata['min'], validators=[validators.DataRequired()])
        end = StringField('To', default=formdata['max'], validators=[validators.DataRequired()])

@app.route('/', methods=('GET', 'POST'))
@app.route('/<symbol>', methods=('GET', 'POST'))
def index(symbol=None):
    form = StockForm()

    if not form.validate_on_submit():
        description = 'validate failed'
        symbol = None

    if request.method == 'POST':
        symbol = request.form.get('symbol').strip()

    formdata['symbol'] = symbol
    formdata['desc'] = symbolinfo(symbol)

    return render_template('index.html', formdata=formdata, form=form)

@app.route('/stock', methods=('GET', 'POST'))
@app.route('/stock/<symbol>', methods=('GET', 'POST'))
def stock(symbol=None, start=None, end=None):


    #if symbol != None and form.validate_on_submit():
        #description = 'invalid symbol'
        #symbol = None

    #if request.method == 'POST':
    #    print("POST")

    if symbol == None:
        symbol = request.form.get('symbol').strip()

    if symbol == None:
        symbol=""
    else:
        symbol = symbol.strip().upper()
        description = symbolinfo(symbol)

    formdata['symbol'] = symbol
    formdata['description'] = description

    # Get sensible start and end dates
    if start == None:
        start = request.form.get('start')
        if start == None:
            start = formdata['min']
        else:
            try:
                dt = time.mktime(time.strptime(start, '%Y%m%d'))
            except:
                print("Bad start date:", start)
                start = formdata['min']

        if end == None:
            end = request.form.get('end')
            if end == None:
                end = formdata['max']
            else:
                try:
                    dt = time.mktime(time.strptime(end, '%Y%m%d'))
                except:
                    print("Bad end date:", end)
                    end = formdata['max']

    # Url args
    form = StockForm()
    return render_template('stock.html', formdata=formdata, symbol=symbol, start=start, end=end, description=description, form=form)

@app.context_processor
def utility_processor():
    def date2human(date):
        t = datetime.datetime.fromtimestamp(date)
        return t.strftime('%d/%m/%Y')
    return dict(date2human=date2human)

@app.route('/shorts-historical', methods=('GET', 'POST'))
def shorts_historical():
    c = stocks.cursor()
    c.row_factory = sqlite3.Row
    c.execute('select symbol, date, max(short) from shorts where length(symbol) = 3 group by symbol order by short desc')
    #print(c.description)
    rows = c.fetchall()
    return render_template('shorts-historical.html', rows=rows)

@app.route('/shorts-now', methods=('GET', 'POST'))
def shorts_now():
    c = stocks.cursor()
    c.row_factory = sqlite3.Row
    c.execute('select symbol, max(date), short from shorts where length(symbol) = 3 group by symbol order by date desc, short desc')
    #print(c.description)
    rows = c.fetchall()
    return render_template('shorts-now.html', rows=rows)

@app.route('/privacy')
def privacy():
    return render_template('privacy.html')

@app.route('/images/<symbol>.png', methods=('GET',))
def graph1_png(symbol=None):
    if symbol == None:
        return # FIXME
    start = request.args.get('start') 
    end = request.args.get('end') 
    fig = graph1(symbol, start, end)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

@app.route('/images/<symbol>-adjusted.png', methods=('GET',))
def graph2_png(symbol=None):
    if symbol == None:
        return # FIXME
    fig = graph2(symbol)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

def graph1(symbol, start=None, end=None):
    '''
    For each symbol we plot:
    * The symbol's closing price
    * The XAO scaled to that
    * The short percentage
    '''

    if start != None:
        try:
            user_date_min = time.mktime(time.strptime(start, '%Y%m%d'))
        except:
            user_date_min = 0

    if end != None:
        try:
            user_date_max = time.mktime(time.strptime(end, '%Y%m%d'))
        except:
            user_date_max = round(time.time())

    # Find the date range available for this symbol
    c = stocks.cursor()
    c.execute('SELECT min(date), max(date) FROM endofday WHERE symbol = ?', (symbol,))
    price_date_min, price_date_max = c.fetchone()

    # limit date range by data availabity (index, symbol) and user selection
    date_min = max(price_date_min, xao_date_min, user_date_min)
    date_max = min(price_date_max, xao_date_max, user_date_max)

    # So now we want our maxima and minima for our two axes: price and index
    c.execute('SELECT min(close), max(close) FROM endofday WHERE symbol = ? AND date >= ? AND date <= ?', (symbol, date_min, date_max))
    price_min, price_max = c.fetchone()

    c.execute('SELECT min(close), max(close) FROM endofday WHERE symbol = "XAO" AND date >= ? AND date <= ?', (date_min, date_max))
    xao_min, xao_max = c.fetchone()

    # Grab a figure
    fig, ax = plt.subplots()
    ax.set_xlabel("Date")

    # endofday (allowed to scale naturally and is our left axis label)
    dates = []
    values = []
    c.execute('SELECT date, close FROM endofday WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date ASC', (symbol, date_min, date_max))
    data = c.fetchall()
    for row in data:
        dates.append(datetime.datetime.fromtimestamp(row[0]))
        values.append(row[1])
    ax.plot_date(dates, values, '-', label="price", lw=1)
    ax.set_ylabel("Price")

    # XAO (scaled to price, no label)
    dates = []
    values = []
    c.execute('SELECT date, close FROM endofday where symbol = "XAO" AND date >= ? AND date <= ?  ORDER BY date ASC', (date_min, date_max))
    data = c.fetchall()
    for row in data:
        dates.append(datetime.datetime.fromtimestamp(row[0]))
        values.append(scale(row[1], xao_min, xao_max, price_min, price_max))
    ax.plot_date(dates, values, '-', label="XAO", lw=1)

    # Shorts (scaled to percentage, label on right)
    c.execute('SELECT max(short) FROM shorts WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date ASC', (symbol, date_min, date_max))
    tmp = c.fetchone()[0]
    if tmp == None: # No short data
        short_max = 100
    else:
        short_max = round(tmp + 0.5)

    dates = []
    values = []
    c.execute('SELECT date, short FROM shorts WHERE symbol = ? AND date >= ? AND date <= ? ORDER BY date ASC', (symbol, date_min, date_max))
    data = c.fetchall()
    for row in data:
        dates.append(datetime.datetime.fromtimestamp(row[0]))
        values.append(scale(row[1], 0, short_max, price_min, price_max))
    ax.plot_date(dates, values, '-', label="short", lw=1)
    # Add a parasitic scale to the right
    par = ax.twinx()
    par.set_ylabel("% Short")
    if short_max < 1:
        short_max = 1
    par.set_ylim(0, short_max)

    # Legend
    fig.legend(loc=2, fontsize='small')

    return fig

def scale(this, min_from, max_from, min_to, max_to):
    # e.g, 4500 is half in       3000-6000 scaled between $3 and $5 should be 4
    # e.g, 4000 is one third in  3000-6000 scaled between $7 and $9 should be 7.66
    # e.g, 4000 is one third in  3000-6000 scaled between $1 and $1 should be 1.00
    if max_from == min_from:
        proportion = 1
    else:
        proportion = (this - min_from) / (max_from - min_from)   # 0.5, .33, 1
    return min_to + (max_to - min_to) * proportion               # 4, 7.66, 1

def graph2(symbol):
    '''
    For each symbol we plot:
    * The symbol's closing price / the xao scaling for that period
    * The short percentage (inverted for correlation)
    '''

    c = stocks.cursor()
    c.execute('SELECT min(close), max(close) FROM endofday where symbol = ?', (symbol,))
    price_min, price_max = c.fetchone()

    # Grab a figure
    fig, ax = plt.subplots()
    ax.set_xlabel("Date")

    # Stop the dates overlapping
    #ax.xticks(rotation=45)
    #fig.autofmt_xdate()
    #plt.tick_params(labelsize=12)

    # XAO (used for scaling endofday)
    xao_values = []
    c.execute('SELECT close FROM endofday where symbol = "XAO" order by date asc')
    data = c.fetchall()
    for row in data:
        xao_values.append(row[0])

    # endofday (scaled to XAO and is our left axis label)
    dates = []
    values = []
    c.execute('SELECT date, close FROM endofday where symbol = ? order by date asc', (symbol,))
    data = c.fetchall()
    for i, row in enumerate(data):
        if len(xao_values) > i:
            dates.append(datetime.datetime.fromtimestamp(row[0]))
            values.append(row[1] / (xao_values[i] / xao_values[0]))
        
    ax.plot_date(dates, values, '-', label="price (XAO adj)", lw=1)
    ax.set_ylabel("Price (XAO adj)")

    # Shorts (scaled to percentage, label on right)
    dates = []
    values = []
    c.execute('SELECT date, short FROM shorts where symbol = ? order by date asc', (symbol,))
    data = c.fetchall()
    for row in data:
        dates.append(datetime.datetime.fromtimestamp(row[0]))
        values.append(scale(row[1], 0, 100, price_min, price_max))
    ax.plot_date(dates, values, '-', label="short", lw=1)
    # Add a parasitic scale to the right
    par = ax.twinx()
    par.set_ylabel("% Short")
    par.set_ylim(0, 100)

    # Legend
    fig.legend(loc=2, fontsize='small')


    return fig
