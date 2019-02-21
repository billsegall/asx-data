#! /usr/bin/env python3
# Copyright (c) 2019, Bill Segall
# All rights reserved. See LICENSE for details.

import atexit, datetime, os, sqlite3, time
import stockdb
from dateutil import parser
from flask import Flask, g, request, render_template, send_from_directory
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired

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
    ticker = StringField('', validators=[DataRequired()])

@app.route('/', methods=('GET', 'POST'))
@app.route('/<ticker>', methods=('GET', 'POST'))
def index(ticker=None, name=None):
    form = StockForm()

    if form.validate_on_submit():
        pass

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

import io
import random
from flask import Response
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
from mpl_toolkits.axisartist.parasite_axes import HostAxes, ParasiteAxes

@app.route('/images/<ticker>-price.png', methods=('GET',))
def price_png(ticker=None):
    if ticker == None:
        return # FIXME
    fig = graph_both(ticker)
    #fig = graph_price(ticker)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

@app.route('/images/<ticker>.png', methods=('GET',))
def short_png(ticker=None):
    if ticker == None:
        return # FIXME
    fig = graph_ticker(ticker)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

def graph_ticker(ticker):
    # Grab a figure
    #fig = plt.figure()
    fig, ax = plt.subplots()

    # Axes
    #host = HostAxes(fig, [0.15, 0.1, 0.65, 0.8])
    #parasite1 = ParasiteAxes(host, sharex=host)
    #host.parasites.append(parasite1)
    #host.set_ylabel("Price")
    #host.set_xlabel("Date")
    #host.axis["right"].set_visible(False)

    #parasite1.set_ylabel("Short")
    #parasite1.axis["right"].set_visible(True)
    #parasite1.axis["right"].major_ticklabels.set_visible(True)
    #parasite1.axis["right"].label.set_visible(True)

    #fig.add_axes(host)

    #host.set_xlim(0, 2)
    #host.set_ylim(0, 2)

    c = stocks.cursor()
    c.execute('SELECT date, short FROM shorts where ticker = ? order by date asc', (ticker,))
    data = c.fetchall()

    dates = []
    values = []
    
    for row in data:
        t = datetime.datetime.fromtimestamp(row[0])
        dates.append(parser.parse(t.strftime('%m/%d/%Y')))
        values.append(row[1])
        #print(row[0], t.strftime('%m/%d/%Y'))

    ax.plot_date(dates, values, '-', label="short", lw=1)

    c.execute('SELECT date, close FROM prices where ticker = ? order by date asc', (ticker,))
    data = c.fetchall()

    dates = []
    values = []
    
    for row in data:
        t = datetime.datetime.fromtimestamp(float(row[0]))
        dates.append(parser.parse(t.strftime('%m/%d/%Y')))
        values.append(row[1])
        #print(row[0], t.strftime('%m/%d/%Y'))

    ax.plot_date(dates, values, '-', label="price", lw=1)
    ax.legend()

    return fig
