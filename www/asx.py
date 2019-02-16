#! /usr/bin/env python3
# Copyright (c) 2019, Bill Segall
# All rights reserved. See LICENSE for details.

import atexit, os, sqlite3
from flask import Flask, g, request, render_template, send_from_directory
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired

app = Flask(__name__)

# Application config
app.config.update(
    SECRET_KEY =          '926b93f2a3301883826827209a1623d4c326f21b',
    WTF_CSRF_SECRET_KEY = '926b93f2a3301883826827209a1623d4c326f21b',
    DATABASE = '../data/stocks.db'
)

# Database initialization/finalization
def db_close():
    db.close()

db = sqlite3.connect(app.config['DATABASE'], detect_types=sqlite3.PARSE_DECLTYPES)

#atexit.register(db_close)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

class StockForm(FlaskForm):
    ticker = StringField('', validators=[DataRequired()])

@app.route('/', methods=('GET', 'POST'))
@app.route('/<ticker>', methods=('GET', 'POST'))
def index(ticker=None):
    form = StockForm()

    if form.validate_on_submit():
        pass

    if request.method == 'GET':
        return render_template('index.html', ticker=ticker, form=form)

    if request.method == 'POST':
        ticker = request.form.get('ticker')
        name = None
        try:
            c = db.cursor()
            name = c.execute('select name from symbols where ticker = ?', (ticker,)).fetchone()[0]
        except Exception as e:
            name = "Unknown"
            
        return render_template('index.html', ticker=ticker, name=name, form=form)

@app.route('/stock/')
@app.route('/stock/<ticker>')
def stock(ticker=None):
    return render_template('stock.html', ticker=ticker)
