#! /usr/bin/env python3
# Copyright (c) 2019, Bill Segall
# All rights reserved. See LICENSE for details.

# Local
import stockdb

# System
import atexit, datetime, io, math, os, random, sqlite3, time
from flask import Flask, Response, g, request, render_template, send_from_directory
from flask_wtf import FlaskForm
from wtforms import StringField, validators
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt

app = Flask(__name__)

# Application config
app.config.update(
    SECRET_KEY =          '926b93f2a3301883826827209a1623d4c326f21b',
    WTF_CSRF_SECRET_KEY = '926b93f2a3301883826827209a1623d4c326f21b',
    DATABASE = '../stockdb/stockdb.db'
)

stocks = stockdb.StockDB(app.config['DATABASE'])

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

class StockForm(FlaskForm):
    ticker = StringField('Enter ticker', validators=[validators.DataRequired(), validators.Length(min=3, max=5)])

@app.route('/', methods=('GET', 'POST'))
@app.route('/<ticker>', methods=('GET', 'POST'))
def index(ticker=None, name='Choose ticker'):
    form = StockForm()

    if not form.validate_on_submit():
        name = 'Invalid ticker'
        ticker = None

    if request.method == 'POST':
        ticker = request.form.get('ticker')

    if ticker != None:
        name = stocks.ticker2name(ticker)

    return render_template('index.html', ticker=ticker, name=name, form=form)

@app.route('/stock', methods=('GET',))
@app.route('/stock/<ticker>', methods=('GET',))
def stock(ticker=None, name=None):
    if ticker == None:
        return render_template('index.html', ticker=ticker, name=name)

    ticker = ticker.upper()
    name = stocks.ticker2name(ticker)
    return render_template('stock.html', ticker=ticker, name=name)


@app.context_processor
def utility_processor():
    def date2human(date):
        t = datetime.datetime.fromtimestamp(date)
        return t.strftime('%d/%m/%Y')
    return dict(date2human=date2human)

@app.route('/shorts', methods=('GET', 'POST'))
def shorts():
    c = stocks.cursor()
    c.row_factory = sqlite3.Row
    c.execute('select ticker, date, max(short) from shorts where length(ticker) = 3 group by ticker order by short desc')
    #print(c.description)
    rows = c.fetchall()
    return render_template('shorts.html', rows=rows)

@app.route('/privacy')
def privacy():
    return render_template('privacy.html')

@app.route('/images/<ticker>.png', methods=('GET',))
def short_png(ticker=None):
    if ticker == None:
        return # FIXME
    fig = graph_ticker(ticker)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

def graph_ticker(ticker):
    '''
    For each ticker we plot:
    * The ticker's closing price
    * The XAO scaled to that
    * The short percentage (inverted for correlation)
    '''

    c = stocks.cursor()
    c.execute('SELECT min(close), max(close) FROM prices where ticker = ?', (ticker,))
    price_min, price_max = c.fetchone()

    c.execute('SELECT min(close), max(close) FROM prices where ticker = "XAO"')
    xao_min, xao_max = c.fetchone()

    # Grab a figure
    fig, ax = plt.subplots()
    ax.set_xlabel("Date")

    # Prices (allowed to scale naturally and is our left axis label)
    dates = []
    values = []
    c.execute('SELECT date, close FROM prices where ticker = ? order by date asc', (ticker,))
    data = c.fetchall()
    for row in data:
        dates.append(datetime.datetime.fromtimestamp(row[0]))
        values.append(row[1])
    ax.plot_date(dates, values, '-', label="price", lw=1)
    ax.set_ylabel("Price")

    # XAO (scaled to price, no label)
    dates = []
    values = []
    c.execute('SELECT date, close FROM prices where ticker = "XAO" order by date asc')
    data = c.fetchall()
    for row in data:
        dates.append(datetime.datetime.fromtimestamp(row[0]))
        values.append(scale(row[1], xao_min, xao_max, price_min, price_max))
    ax.plot_date(dates, values, '-', label="XAO", lw=1)

    # Shorts (scaled to percentage, label on right)
    dates = []
    values = []
    c.execute('SELECT date, short FROM shorts where ticker = ? order by date asc', (ticker,))
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

def scale(this, min_from, max_from, min_to, max_to):
    # e.g, 4500 is half in       3000-6000 scaled between $3 and $5 should be 4
    # e.g, 4000 is one third in  3000-6000 scaled between $7 and $9 should be 6.66
    proportion = (this - min_from) / (max_from - min_from)       # 0.5, .33
    return min_to + (max_to - min_to) * proportion               # 4, 7.66
