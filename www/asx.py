#! /usr/bin/env python3
# Copyright (c) 2019-2021, Bill Segall
# All rights reserved. See LICENSE for details.

# Local
import stockdb

# System
import datetime, json, math, os, re, secrets, sqlite3, time, urllib.request
from flask import Flask, abort, jsonify, redirect, request, render_template, send_from_directory, url_for
from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user, current_user
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)

# Application config
app.config.update(
    DATABASE          = os.environ.get('DATABASE', '../stockdb/stockdb.db'),
    ANNOUNCEMENTS_URL = os.environ.get('ANNOUNCEMENTS_URL', 'https://harri.tailb1cff.ts.net:8081'),
    USERS_DB          = os.environ.get('USERS_DB', '../stockdb/users.db'),
    SECRET_KEY        = os.environ.get('SECRET_KEY', secrets.token_hex(32)),
)

## Auth setup

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'


class User(UserMixin):
    def __init__(self, id, email, pw_hash, enabled=1):
        self.id = id
        self.email = email
        self.pw_hash = pw_hash
        self.enabled = enabled

    @property
    def is_active(self):
        return bool(self.enabled)

    @property
    def is_admin(self):
        return self.email.lower() == 'admin@segall.net'

    @property
    def needs_password(self):
        return self.pw_hash is None


def users_db():
    conn = sqlite3.connect(app.config['USERS_DB'])
    conn.row_factory = sqlite3.Row
    return conn


def init_users_db():
    with users_db() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS users (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            email   TEXT NOT NULL UNIQUE COLLATE NOCASE,
            pw_hash TEXT,
            enabled INTEGER NOT NULL DEFAULT 1
        )''')
        try:
            conn.execute('ALTER TABLE users ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1')
        except sqlite3.OperationalError:
            pass  # column already exists
        conn.execute("INSERT OR IGNORE INTO users (email, pw_hash, enabled) VALUES ('admin@segall.net', NULL, 1)")
        conn.commit()


init_users_db()


@login_manager.user_loader
def load_user(user_id):
    with users_db() as conn:
        row = conn.execute('SELECT id, email, pw_hash, enabled FROM users WHERE id = ?', (user_id,)).fetchone()
    if row:
        return User(row['id'], row['email'], row['pw_hash'], row['enabled'])
    return None


_AUTH_EXEMPT = {'login', 'set_password', 'logout', 'static', 'favicon', 'privacy'}

@app.before_request
def auth_checks():
    if current_user.is_authenticated:
        if not current_user.is_active:
            logout_user()
            return redirect(url_for('login'))
        if current_user.needs_password and request.endpoint not in _AUTH_EXEMPT:
            return redirect(url_for('set_password'))


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


## Auth routes

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    error = None
    if request.method == 'POST':
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        with users_db() as conn:
            row = conn.execute('SELECT id, email, pw_hash, enabled FROM users WHERE email = ?', (email,)).fetchone()
        if row:
            user = User(row['id'], row['email'], row['pw_hash'], row['enabled'])
            if not user.is_active:
                error = 'Account disabled.'
            elif user.pw_hash is None or check_password_hash(user.pw_hash, password):
                login_user(user)
                if user.needs_password:
                    return redirect(url_for('set_password'))
                return redirect(request.args.get('next') or url_for('index'))
            else:
                error = 'Invalid email or password.'
        else:
            error = 'Invalid email or password.'
    return render_template('login.html', error=error)


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


@app.route('/set-password', methods=['GET', 'POST'])
@login_required
def set_password():
    error = None
    if request.method == 'POST':
        pw = request.form.get('password', '')
        confirm = request.form.get('confirm', '')
        if len(pw) < 8:
            error = 'Password must be at least 8 characters.'
        elif pw != confirm:
            error = 'Passwords do not match.'
        else:
            pw_hash = generate_password_hash(pw)
            with users_db() as conn:
                conn.execute('UPDATE users SET pw_hash = ? WHERE id = ?', (pw_hash, current_user.id))
                conn.commit()
            current_user.pw_hash = pw_hash
            return redirect(url_for('index'))
    return render_template('set_password.html', error=error)


@app.route('/admin', methods=['GET', 'POST'])
@login_required
def admin():
    if not current_user.is_admin:
        abort(403)
    error = None
    message = None
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'add':
            email = request.form.get('email', '').strip()
            if email:
                try:
                    with users_db() as conn:
                        conn.execute('INSERT INTO users (email, pw_hash) VALUES (?, NULL)', (email,))
                        conn.commit()
                    message = f'User {email} added.'
                except sqlite3.IntegrityError:
                    error = f'{email} already exists.'
        elif action == 'set_user_password':
            user_id = request.form.get('user_id')
            pw = request.form.get('password', '')
            if len(pw) < 8:
                error = 'Password must be at least 8 characters.'
            else:
                with users_db() as conn:
                    conn.execute('UPDATE users SET pw_hash = ? WHERE id = ?',
                                 (generate_password_hash(pw), user_id))
                    conn.commit()
                message = 'Password updated.'
        elif action == 'toggle_enabled':
            user_id = request.form.get('user_id')
            enabled = 1 if request.form.get('enabled') == '1' else 0
            with users_db() as conn:
                row = conn.execute('SELECT email FROM users WHERE id = ?', (user_id,)).fetchone()
                if row and row['email'].lower() != 'admin@segall.net':
                    conn.execute('UPDATE users SET enabled = ? WHERE id = ?', (enabled, user_id))
                    conn.commit()
        elif action == 'delete':
            user_id = request.form.get('user_id')
            with users_db() as conn:
                row = conn.execute('SELECT email FROM users WHERE id = ?', (user_id,)).fetchone()
                if row and row['email'].lower() != 'admin@segall.net':
                    conn.execute('DELETE FROM users WHERE id = ?', (user_id,))
                    conn.commit()
                    message = 'User deleted.'
    with users_db() as conn:
        users = [dict(r) for r in conn.execute('SELECT id, email, pw_hash, enabled FROM users ORDER BY id').fetchall()]
    return render_template('admin.html', users=users, error=error, message=message)


## Existing routes

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')


@app.route('/')
@login_required
def index():
    return render_template('index.html')


@app.route('/stock')
@app.route('/stock/<symbol>')
@login_required
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
@login_required
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
@login_required
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


@app.route('/shorts')
@login_required
def shorts():
    return render_template('shorts.html')


@app.route('/api/shorts')
@login_required
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


@app.route('/privacy')
def privacy():
    return render_template('privacy.html')


@app.route('/announcements')
@login_required
def announcements():
    return render_template('announcements.html')


def valid_ticker(ticker):
    return bool(re.fullmatch(r'[A-Z]{2,5}', ticker))

@app.route('/api/announcements')
@login_required
def api_announcements_all():
    qs = 'limit=' + request.args.get('limit', '200')
    for key in ('date', 'price_sensitive'):
        val = request.args.get(key)
        if val:
            qs += '&' + key + '=' + val
    ticker = request.args.get('ticker', '')
    if ticker:
        ticker = ticker.strip().upper()
        if not valid_ticker(ticker):
            abort(400)
        qs += '&ticker=' + ticker
    url = app.config['ANNOUNCEMENTS_URL'] + '/announcements?' + qs
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return jsonify(json.loads(resp.read()))
    except Exception:
        return jsonify([])


@app.route('/api/announcements/<symbol>')
@login_required
def api_announcements(symbol):
    symbol = symbol.strip().upper()
    if not valid_ticker(symbol):
        abort(400)
    url = app.config['ANNOUNCEMENTS_URL'] + '/announcements?ticker=' + symbol + '&limit=25'
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return jsonify(json.loads(resp.read()))
    except Exception:
        return jsonify([])


@app.route('/api/announcements/<ids_id>/pdf')
@login_required
def api_announcement_pdf(ids_id):
    if not re.fullmatch(r'[0-9]+', ids_id):
        abort(400)
    return redirect(app.config['ANNOUNCEMENTS_URL'] + '/announcements/' + ids_id + '/pdf')


@app.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404
