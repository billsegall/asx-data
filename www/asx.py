# Copyright (c) 2019, Bill Segall
# All rights reserved. See LICENSE for details.

import os
from flask import Flask
from flask import render_template
from flask import send_from_directory

app = Flask(__name__)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/stock/')
@app.route('/stock/<ticker>')
def stock(ticker=None):
    return render_template('stock.html', ticker=ticker)
