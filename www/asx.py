#! /usr/bin/env python3
# Copyright (c) 2019, Bill Segall
# All rights reserved. See LICENSE for details.

import atexit, datetime, os, sqlite3, time
import stockdb
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

# Our shorts table stores times as ints and we want to be able to display them nicely
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
