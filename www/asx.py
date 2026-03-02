#! /usr/bin/env python3
# Copyright (c) 2019-2021, Bill Segall
# All rights reserved. See LICENSE for details.

# System
import datetime, json, os, re, requests, secrets, sqlite3, time, urllib.request
from flask import Flask, abort, jsonify, redirect, request, render_template, send_from_directory, url_for
from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user, current_user
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)

# Application config
app.config.update(
    BACKEND_URL       = os.environ.get('BACKEND_URL', 'http://localhost:8082'),
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


WATCHLIST_COLUMNS = [
    {'key': 'symbol',        'label': 'Symbol',   'default': True},
    {'key': 'name',          'label': 'Name',     'default': True},
    {'key': 'industry',      'label': 'Industry', 'default': False},
    {'key': 'price',         'label': 'Price',    'default': True},
    {'key': 'change_1d',     'label': 'Chg $',    'default': True},
    {'key': 'change_1d_pct', 'label': 'Day %',    'default': True},
    {'key': 'change_1w_pct', 'label': '1W %',     'default': False},
    {'key': 'change_1m_pct', 'label': '1M %',     'default': False},
    {'key': 'change_3m_pct', 'label': '3M %',     'default': False},
    {'key': 'change_6m_pct', 'label': '6M %',     'default': False},
    {'key': 'change_1y_pct', 'label': '1Y %',     'default': False},
    {'key': 'change_3y_pct', 'label': '3Y %',     'default': False},
    {'key': 'change_5y_pct', 'label': '5Y %',     'default': False},
    {'key': 'high_52w',      'label': '1Y High',  'default': True,  'portfolio_default': False},
    {'key': 'low_52w',       'label': '1Y Low',   'default': True,  'portfolio_default': False},
    {'key': 'mcap',          'label': 'Mkt Cap',  'default': False},
    {'key': 'short_pct',     'label': 'Short %',  'default': True,  'portfolio_default': False},
    {'key': 'volume',        'label': 'Volume',   'default': True,  'portfolio_default': False},
    {'key': 'notes',         'label': 'Notes',    'default': False},
]

PORTFOLIO_EXTRA_COLUMNS = [
    {'key': 'quantity',       'label': 'Units',     'default': True},
    {'key': 'purchase_price', 'label': 'Buy Price', 'default': True},
    {'key': 'purchase_date',  'label': 'Buy Date',  'default': False},
    {'key': 'days_held',      'label': 'Days Held', 'default': False},
    {'key': 'cost_basis',     'label': 'Cost',      'default': False},
    {'key': 'current_value',  'label': 'Value',     'default': True},
    {'key': 'pnl',            'label': 'P&L $',     'default': True},
    {'key': 'pnl_pct',        'label': 'P&L %',     'default': True},
    {'key': 'alloc_pct',      'label': 'Alloc %',   'default': True},
]

PORTFOLIO_COLUMNS = WATCHLIST_COLUMNS + PORTFOLIO_EXTRA_COLUMNS


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

        conn.execute('''CREATE TABLE IF NOT EXISTS list_groups (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            type     TEXT NOT NULL CHECK(type IN ('watchlist','portfolio')),
            name     TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0
        )''')

        conn.execute('''CREATE TABLE IF NOT EXISTS lists (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            type     TEXT NOT NULL CHECK(type IN ('watchlist','portfolio')),
            group_id INTEGER REFERENCES list_groups(id) ON DELETE SET NULL,
            name     TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0
        )''')

        conn.execute('''CREATE TABLE IF NOT EXISTS watchlist_items (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            list_id  INTEGER NOT NULL REFERENCES lists(id) ON DELETE CASCADE,
            symbol   TEXT NOT NULL,
            notes    TEXT,
            position INTEGER NOT NULL DEFAULT 0,
            UNIQUE(list_id, symbol)
        )''')
        try:
            conn.execute('ALTER TABLE watchlist_items ADD COLUMN notes TEXT')
        except sqlite3.OperationalError:
            pass

        conn.execute('''CREATE TABLE IF NOT EXISTS portfolio_items (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            list_id        INTEGER NOT NULL REFERENCES lists(id) ON DELETE CASCADE,
            symbol         TEXT NOT NULL,
            quantity       REAL NOT NULL,
            purchase_price REAL NOT NULL,
            purchase_date  TEXT,
            notes          TEXT,
            position       INTEGER NOT NULL DEFAULT 0
        )''')

        conn.execute('''CREATE TABLE IF NOT EXISTS list_column_prefs (
            user_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            type     TEXT NOT NULL CHECK(type IN ('watchlist','portfolio')),
            columns  TEXT NOT NULL,
            PRIMARY KEY (user_id, type)
        )''')

        conn.commit()


init_users_db()


## Watchlist / Portfolio helpers

def _get_column_prefs(user_id, list_type, conn):
    """Return list of {key, label, visible} dicts for the given list type."""
    base = WATCHLIST_COLUMNS if list_type == 'watchlist' else PORTFOLIO_COLUMNS
    row = conn.execute(
        'SELECT columns FROM list_column_prefs WHERE user_id = ? AND type = ?',
        (user_id, list_type)
    ).fetchone()
    def col_default(col_def):
        if list_type == 'portfolio':
            return col_def.get('portfolio_default', col_def['default'])
        return col_def['default']

    if row:
        try:
            result = []
            seen = set()
            for c in json.loads(row['columns']):
                col_def = next((x for x in base if x['key'] == c['key']), None)
                if col_def and c['key'] not in seen:
                    result.append({'key': c['key'], 'label': col_def['label'], 'visible': c.get('visible', col_default(col_def))})
                    seen.add(c['key'])
            # Add any new columns not in saved prefs
            for col_def in base:
                if col_def['key'] not in seen:
                    result.append({'key': col_def['key'], 'label': col_def['label'], 'visible': col_default(col_def)})
            return result
        except Exception:
            pass
    return [{'key': c['key'], 'label': c['label'], 'visible': col_default(c)} for c in base]


def _ensure_default_lists(user_id, list_type, conn):
    """Create a default list for the user if they have none of this type."""
    count = conn.execute(
        'SELECT COUNT(*) FROM lists WHERE user_id = ? AND type = ?',
        (user_id, list_type)
    ).fetchone()[0]
    if count == 0:
        name = 'Watchlist' if list_type == 'watchlist' else 'Portfolio'
        conn.execute(
            'INSERT INTO lists (user_id, type, name, position) VALUES (?, ?, ?, 0)',
            (user_id, list_type, name)
        )
        conn.commit()


def _check_list_owner(list_id, user_id, conn):
    """Return the list row, or abort 403/404."""
    row = conn.execute('SELECT * FROM lists WHERE id = ?', (list_id,)).fetchone()
    if not row:
        abort(404)
    if row['user_id'] != user_id:
        abort(403)
    return row


def enrich_symbols(symbols):
    """Return dict of symbol -> metrics from backend API."""
    if not symbols:
        return {}
    try:
        resp = requests.post(
            app.config['BACKEND_URL'] + '/api/enrich',
            json={'symbols': list(symbols)},
            timeout=10,
        )
        if resp.ok:
            return resp.json()
    except Exception:
        pass
    return {s: {} for s in symbols}


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

def _backend_get(path, **kwargs):
    """GET from backend API. Returns parsed JSON or None on error."""
    try:
        resp = requests.get(app.config['BACKEND_URL'] + path, timeout=10, **kwargs)
        if resp.ok:
            return resp.json()
    except Exception:
        pass
    return None


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

    info = _backend_get(f'/api/symbol/{symbol}')
    if info is None:
        abort(404)
    return render_template('stock.html',
                           symbol=symbol,
                           name=info.get('name') or symbol,
                           industry=info.get('industry') or '',
                           mcap=info.get('mcap') or '')


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
    params = {k: v for k, v in [('start', request.args.get('start')), ('end', request.args.get('end'))] if v}
    data = _backend_get(f'/api/stock/{symbol}', params=params)
    if data is None:
        abort(404)
    return jsonify(data)


@app.route('/api/symbols')
@login_required
def api_symbols():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify([])
    data = _backend_get('/api/symbols', params={'q': q})
    return jsonify(data or [])


@app.route('/shorts')
@login_required
def shorts():
    return render_template('shorts.html')


@app.route('/api/shorts')
@login_required
def api_shorts():
    data = _backend_get('/api/shorts')
    if data is None:
        return jsonify({'data': [], 'latest_date': None})
    return jsonify(data)


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


## Watchlist / Portfolio routes

@app.route('/watchlists')
@login_required
def watchlists():
    with users_db() as conn:
        _ensure_default_lists(current_user.id, 'watchlist', conn)
    return render_template('lists.html', list_type='watchlist', page_title='Watchlists', nav_active='watchlists')


@app.route('/portfolios')
@login_required
def portfolios():
    with users_db() as conn:
        _ensure_default_lists(current_user.id, 'portfolio', conn)
    return render_template('lists.html', list_type='portfolio', page_title='Portfolios', nav_active='portfolios')


@app.route('/api/lists')
@login_required
def api_lists_get():
    list_type = request.args.get('type', 'watchlist')
    if list_type not in ('watchlist', 'portfolio'):
        abort(400)
    symbol = request.args.get('symbol', '').strip().upper() or None
    with users_db() as conn:
        groups = [dict(r) for r in conn.execute(
            'SELECT id, name, position FROM list_groups WHERE user_id = ? AND type = ? ORDER BY position, id',
            (current_user.id, list_type)
        ).fetchall()]
        lists = [dict(r) for r in conn.execute(
            'SELECT id, name, group_id, position FROM lists WHERE user_id = ? AND type = ? ORDER BY position, id',
            (current_user.id, list_type)
        ).fetchall()]
        if symbol and list_type == 'watchlist':
            members = {row[0] for row in conn.execute(
                'SELECT list_id FROM watchlist_items WHERE list_id IN ({}) AND symbol = ?'.format(
                    ','.join(str(l['id']) for l in lists) if lists else '0'
                ), (symbol,)
            ).fetchall()}
            for lst in lists:
                lst['has_symbol'] = lst['id'] in members
        col_prefs = _get_column_prefs(current_user.id, list_type, conn)
    return jsonify({'lists': lists, 'groups': groups, 'column_prefs': col_prefs})


@app.route('/api/lists', methods=['POST'])
@login_required
def api_lists_create():
    data = request.get_json(force=True)
    list_type = data.get('type', 'watchlist')
    if list_type not in ('watchlist', 'portfolio'):
        abort(400)
    name = (data.get('name') or '').strip()
    if not name:
        abort(400)
    group_id = data.get('group_id')
    with users_db() as conn:
        if group_id:
            g = conn.execute('SELECT id FROM list_groups WHERE id = ? AND user_id = ?', (group_id, current_user.id)).fetchone()
            if not g:
                abort(403)
        cur = conn.execute(
            'INSERT INTO lists (user_id, type, group_id, name, position) VALUES (?, ?, ?, ?, 0)',
            (current_user.id, list_type, group_id, name)
        )
        conn.commit()
        row = dict(conn.execute('SELECT id, name, group_id, position FROM lists WHERE id = ?', (cur.lastrowid,)).fetchone())
    return jsonify(row), 201


@app.route('/api/lists/<int:list_id>', methods=['PATCH'])
@login_required
def api_lists_patch(list_id):
    data = request.get_json(force=True)
    with users_db() as conn:
        _check_list_owner(list_id, current_user.id, conn)
        if 'name' in data:
            name = data['name'].strip()
            if name:
                conn.execute('UPDATE lists SET name = ? WHERE id = ?', (name, list_id))
        if 'group_id' in data:
            gid = data['group_id']
            if gid is not None:
                g = conn.execute('SELECT id FROM list_groups WHERE id = ? AND user_id = ?', (gid, current_user.id)).fetchone()
                if not g:
                    abort(403)
            conn.execute('UPDATE lists SET group_id = ? WHERE id = ?', (gid, list_id))
        if 'position' in data:
            conn.execute('UPDATE lists SET position = ? WHERE id = ?', (int(data['position']), list_id))
        conn.commit()
        row = dict(conn.execute('SELECT id, name, group_id, position FROM lists WHERE id = ?', (list_id,)).fetchone())
    return jsonify(row)


@app.route('/api/lists/<int:list_id>', methods=['DELETE'])
@login_required
def api_lists_delete(list_id):
    with users_db() as conn:
        _check_list_owner(list_id, current_user.id, conn)
        conn.execute('DELETE FROM lists WHERE id = ?', (list_id,))
        conn.commit()
    return '', 204


@app.route('/api/list-groups', methods=['POST'])
@login_required
def api_list_groups_create():
    data = request.get_json(force=True)
    list_type = data.get('type', 'watchlist')
    if list_type not in ('watchlist', 'portfolio'):
        abort(400)
    name = (data.get('name') or '').strip()
    if not name:
        abort(400)
    with users_db() as conn:
        cur = conn.execute(
            'INSERT INTO list_groups (user_id, type, name, position) VALUES (?, ?, ?, 0)',
            (current_user.id, list_type, name)
        )
        conn.commit()
        row = dict(conn.execute('SELECT id, name, position FROM list_groups WHERE id = ?', (cur.lastrowid,)).fetchone())
    return jsonify(row), 201


@app.route('/api/list-groups/<int:group_id>', methods=['PATCH'])
@login_required
def api_list_groups_patch(group_id):
    data = request.get_json(force=True)
    with users_db() as conn:
        g = conn.execute('SELECT id FROM list_groups WHERE id = ? AND user_id = ?', (group_id, current_user.id)).fetchone()
        if not g:
            abort(404)
        if 'name' in data:
            name = data['name'].strip()
            if name:
                conn.execute('UPDATE list_groups SET name = ? WHERE id = ?', (name, group_id))
        if 'position' in data:
            conn.execute('UPDATE list_groups SET position = ? WHERE id = ?', (int(data['position']), group_id))
        conn.commit()
        row = dict(conn.execute('SELECT id, name, position FROM list_groups WHERE id = ?', (group_id,)).fetchone())
    return jsonify(row)


@app.route('/api/list-groups/<int:group_id>', methods=['DELETE'])
@login_required
def api_list_groups_delete(group_id):
    with users_db() as conn:
        g = conn.execute('SELECT id FROM list_groups WHERE id = ? AND user_id = ?', (group_id, current_user.id)).fetchone()
        if not g:
            abort(404)
        conn.execute('DELETE FROM list_groups WHERE id = ?', (group_id,))
        conn.commit()
    return '', 204


@app.route('/api/lists/<int:list_id>/items')
@login_required
def api_list_items_get(list_id):
    with users_db() as conn:
        lst = _check_list_owner(list_id, current_user.id, conn)
        list_type = lst['type']
        if list_type == 'watchlist':
            rows = conn.execute(
                'SELECT id, symbol, notes, position FROM watchlist_items WHERE list_id = ? ORDER BY position, id',
                (list_id,)
            ).fetchall()
            items = [dict(r) for r in rows]
        else:
            rows = conn.execute(
                'SELECT id, symbol, quantity, purchase_price, purchase_date, notes, position '
                'FROM portfolio_items WHERE list_id = ? ORDER BY position, id',
                (list_id,)
            ).fetchall()
            items = [dict(r) for r in rows]
        col_prefs = _get_column_prefs(current_user.id, list_type, conn)

    symbols = list({item['symbol'] for item in items})
    metrics = enrich_symbols(symbols)
    for item in items:
        m = metrics.get(item['symbol'], {})
        item.update(m)
        # Portfolio-specific computed fields
        if list_type == 'portfolio':
            price = m.get('price')
            qty = item.get('quantity', 0)
            buy = item.get('purchase_price', 0)
            item['cost_basis'] = round(qty * buy, 2) if qty and buy else None
            item['current_value'] = round(qty * price, 2) if qty and price else None
            if price and buy:
                item['pnl'] = round(qty * (price - buy), 2)
                item['pnl_pct'] = round((price - buy) / buy * 100, 2)
            if item.get('purchase_date'):
                try:
                    pd_date = datetime.datetime.strptime(item['purchase_date'], '%Y-%m-%d').date()
                    item['days_held'] = (datetime.date.today() - pd_date).days
                except Exception:
                    item['days_held'] = None

    return jsonify({'list': dict(lst), 'items': items, 'column_prefs': col_prefs})


@app.route('/api/lists/<int:list_id>/items', methods=['POST'])
@login_required
def api_list_items_create(list_id):
    data = request.get_json(force=True)
    with users_db() as conn:
        lst = _check_list_owner(list_id, current_user.id, conn)
        list_type = lst['type']
        symbol = (data.get('symbol') or '').strip().upper()
        if not symbol:
            abort(400)
        if list_type == 'watchlist':
            notes = data.get('notes') or None
            try:
                cur = conn.execute(
                    'INSERT INTO watchlist_items (list_id, symbol, notes, position) VALUES (?, ?, ?, 0)',
                    (list_id, symbol, notes)
                )
                conn.commit()
                item = dict(conn.execute('SELECT id, symbol, notes, position FROM watchlist_items WHERE id = ?', (cur.lastrowid,)).fetchone())
            except sqlite3.IntegrityError:
                abort(409)  # duplicate
        else:
            try:
                qty = float(data.get('quantity', 0))
                buy_price = float(data.get('purchase_price', 0))
            except (TypeError, ValueError):
                abort(400)
            if qty <= 0 or buy_price <= 0:
                abort(400)
            purchase_date = data.get('purchase_date') or None
            notes = data.get('notes') or None
            cur = conn.execute(
                'INSERT INTO portfolio_items (list_id, symbol, quantity, purchase_price, purchase_date, notes, position) VALUES (?, ?, ?, ?, ?, ?, 0)',
                (list_id, symbol, qty, buy_price, purchase_date, notes)
            )
            conn.commit()
            item = dict(conn.execute(
                'SELECT id, symbol, quantity, purchase_price, purchase_date, notes, position FROM portfolio_items WHERE id = ?',
                (cur.lastrowid,)
            ).fetchone())

    metrics = enrich_symbols([symbol])
    item.update(metrics.get(symbol, {}))
    if list_type == 'portfolio':
        price = item.get('price')
        qty = item.get('quantity', 0)
        buy = item.get('purchase_price', 0)
        item['cost_basis'] = round(qty * buy, 2) if qty and buy else None
        item['current_value'] = round(qty * price, 2) if qty and price else None
        if price and buy:
            item['pnl'] = round(qty * (price - buy), 2)
            item['pnl_pct'] = round((price - buy) / buy * 100, 2)
    return jsonify(item), 201


@app.route('/api/lists/<int:list_id>/items/<int:item_id>', methods=['DELETE'])
@login_required
def api_list_items_delete(list_id, item_id):
    with users_db() as conn:
        lst = _check_list_owner(list_id, current_user.id, conn)
        if lst['type'] == 'watchlist':
            conn.execute('DELETE FROM watchlist_items WHERE id = ? AND list_id = ?', (item_id, list_id))
        else:
            conn.execute('DELETE FROM portfolio_items WHERE id = ? AND list_id = ?', (item_id, list_id))
        conn.commit()
    return '', 204


@app.route('/api/lists/<int:list_id>/items/<int:item_id>', methods=['PATCH'])
@login_required
def api_list_items_patch(list_id, item_id):
    data = request.get_json(force=True)
    with users_db() as conn:
        lst = _check_list_owner(list_id, current_user.id, conn)
        if lst['type'] == 'watchlist':
            row = conn.execute('SELECT id FROM watchlist_items WHERE id = ? AND list_id = ?', (item_id, list_id)).fetchone()
            if not row:
                abort(404)
            conn.execute('UPDATE watchlist_items SET notes = ? WHERE id = ?', (data.get('notes') or None, item_id))
            conn.commit()
            item = dict(conn.execute('SELECT id, symbol, notes, position FROM watchlist_items WHERE id = ?', (item_id,)).fetchone())
            metrics = enrich_symbols([item['symbol']])
            item.update(metrics.get(item['symbol'], {}))
            return jsonify(item)

        # Portfolio
        row = conn.execute('SELECT id FROM portfolio_items WHERE id = ? AND list_id = ?', (item_id, list_id)).fetchone()
        if not row:
            abort(404)
        updates = []
        params = []
        for field in ('quantity', 'purchase_price'):
            if field in data:
                try:
                    val = float(data[field])
                except (TypeError, ValueError):
                    abort(400)
                updates.append(f'{field} = ?')
                params.append(val)
        for field in ('purchase_date', 'notes'):
            if field in data:
                updates.append(f'{field} = ?')
                params.append(data[field] or None)
        if updates:
            params.append(item_id)
            conn.execute(f'UPDATE portfolio_items SET {", ".join(updates)} WHERE id = ?', params)
            conn.commit()
        item = dict(conn.execute(
            'SELECT id, symbol, quantity, purchase_price, purchase_date, notes, position FROM portfolio_items WHERE id = ?',
            (item_id,)
        ).fetchone())

    metrics = enrich_symbols([item['symbol']])
    item.update(metrics.get(item['symbol'], {}))
    price = item.get('price')
    qty = item.get('quantity', 0)
    buy = item.get('purchase_price', 0)
    item['cost_basis'] = round(qty * buy, 2) if qty and buy else None
    item['current_value'] = round(qty * price, 2) if qty and price else None
    if price and buy:
        item['pnl'] = round(qty * (price - buy), 2)
        item['pnl_pct'] = round((price - buy) / buy * 100, 2)
    return jsonify(item)


@app.route('/api/lists/<int:list_id>/items/<int:item_id>/move', methods=['POST'])
@login_required
def api_list_items_move(list_id, item_id):
    data = request.get_json(force=True)
    target_list_id = data.get('target_list_id')
    if not target_list_id:
        abort(400)
    with users_db() as conn:
        src = _check_list_owner(list_id, current_user.id, conn)
        dst = _check_list_owner(target_list_id, current_user.id, conn)
        if src['type'] != dst['type']:
            abort(400)
        list_type = src['type']
        if list_type == 'watchlist':
            row = conn.execute('SELECT symbol FROM watchlist_items WHERE id = ? AND list_id = ?', (item_id, list_id)).fetchone()
            if not row:
                abort(404)
            symbol = row['symbol']
            conn.execute('DELETE FROM watchlist_items WHERE id = ?', (item_id,))
            try:
                conn.execute('INSERT INTO watchlist_items (list_id, symbol, position) VALUES (?, ?, 0)', (target_list_id, symbol))
            except sqlite3.IntegrityError:
                pass  # already in target list
        else:
            row = conn.execute('SELECT id FROM portfolio_items WHERE id = ? AND list_id = ?', (item_id, list_id)).fetchone()
            if not row:
                abort(404)
            conn.execute('UPDATE portfolio_items SET list_id = ? WHERE id = ?', (target_list_id, item_id))
        conn.commit()
    return '', 204


@app.route('/api/lists/<int:list_id>/items/reorder', methods=['POST'])
@login_required
def api_list_items_reorder(list_id):
    data = request.get_json(force=True)
    order = data.get('order', [])
    with users_db() as conn:
        lst = _check_list_owner(list_id, current_user.id, conn)
        table = 'watchlist_items' if lst['type'] == 'watchlist' else 'portfolio_items'
        for pos, item_id in enumerate(order):
            conn.execute(f'UPDATE {table} SET position = ? WHERE id = ? AND list_id = ?', (pos, item_id, list_id))
        conn.commit()
    return '', 204


@app.route('/api/column-prefs', methods=['PUT'])
@login_required
def api_column_prefs_put():
    data = request.get_json(force=True)
    list_type = data.get('type')
    if list_type not in ('watchlist', 'portfolio'):
        abort(400)
    columns = data.get('columns')
    if not isinstance(columns, list):
        abort(400)
    with users_db() as conn:
        conn.execute(
            'INSERT OR REPLACE INTO list_column_prefs (user_id, type, columns) VALUES (?, ?, ?)',
            (current_user.id, list_type, json.dumps(columns))
        )
        conn.commit()
    return '', 204


@app.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404
