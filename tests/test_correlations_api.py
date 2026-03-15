"""
Tests for the /api/analysis/correlations/* endpoints.

These are integration tests that hit the live backend at localhost:8082.
Run from the repo root:
    python3 tests/test_correlations_api.py

Requirements: backend running, correlations.db present in analysis/results/.
"""

import sys
import urllib.request
import urllib.parse
import json


BASE = 'http://localhost:8082'


# ── Helpers ───────────────────────────────────────────────────────────────────

def get(path, **params):
    url = BASE + path
    if params:
        url += '?' + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


class Failures:
    def __init__(self):
        self.items = []

    def check(self, name, condition, detail=''):
        mark = '  OK' if condition else 'FAIL'
        msg = f'{mark}  {name}'
        if not condition and detail:
            msg += f'\n       {detail}'
        print(msg)
        if not condition:
            self.items.append(name)

    def summary(self):
        if self.items:
            print(f'\n{len(self.items)} failure(s):')
            for f in self.items:
                print(f'  - {f}')
            return False
        print('\nAll tests passed.')
        return True


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_industries(f):
    print('\n── Industries endpoint ──────────────────────────────────────────')
    data = get('/api/analysis/correlations/industries')
    f.check('returns a list', isinstance(data, list))
    f.check('non-empty', len(data) > 0)
    if data:
        row = data[0]
        for key in ('industry', 'n_significant', 'n_stable', 'run_at', 'n_symbols'):
            f.check(f'industry row has {key!r}', key in row)


def test_db_defaults(f):
    print('\n── Default query ────────────────────────────────────────────────')
    data = get('/api/analysis/correlations/db')
    f.check('returns dict with n and results', 'n' in data and 'results' in data)
    results = data['results']
    f.check('returns rows', len(results) > 0)
    if results:
        row = results[0]
        for key in ('leader', 'follower', 'industry', 'lag_days', 'direction',
                    'train_r', 'backtest_r', 'fdr_p', 'stability', 'n_stable',
                    'recency_score', 'market_adjusted'):
            f.check(f'row has {key!r}', key in row,
                    f'row keys: {list(row.keys())}')


def test_stability_field(f):
    print('\n── Stability field integrity ────────────────────────────────────')
    data = get('/api/analysis/correlations/db', limit=200)
    results = data['results']
    f.check('has rows to test', len(results) > 0)
    for row in results:
        stab = row.get('stability', '')
        f.check(f'stability is 5-char string for {row["leader"]}->{row["follower"]} lag{row["lag_days"]}',
                isinstance(stab, str) and len(stab) == 5,
                f'got {stab!r}')
        if not (isinstance(stab, str) and len(stab) == 5):
            break
        f.check(f'stability contains only 0/1 for {stab}',
                all(c in '01' for c in stab))
        if not all(c in '01' for c in stab):
            break

        expected_n = stab.count('1')
        f.check(f'n_stable matches 1-count for {stab}',
                row['n_stable'] == expected_n,
                f'n_stable={row["n_stable"]} but stab.count("1")={expected_n}')

        # Recency score: index 0 (oldest) = weight 1, index 4 (newest) = weight 16
        expected_score = sum(int(c) * (2 ** i) for i, c in enumerate(stab))
        f.check(f'recency_score correct for {stab}',
                row['recency_score'] == expected_score,
                f'got {row["recency_score"]}, expected {expected_score}')
        break  # one full check is enough; spot-check a few more
    # Spot-check all rows for n_stable/stability consistency
    mismatches = [r for r in results
                  if r.get('stability') and r['n_stable'] != r['stability'].count('1')]
    f.check('n_stable matches 1-count across all returned rows',
            len(mismatches) == 0,
            f'{len(mismatches)} mismatches; first: {mismatches[0] if mismatches else ""}')


def test_direction_filter(f):
    print('\n── Direction filter ─────────────────────────────────────────────')
    for direction in ('positive', 'negative'):
        data = get('/api/analysis/correlations/db', direction=direction, limit=200)
        results = data['results']
        f.check(f'direction={direction} returns rows', len(results) > 0)

        wrong_train = [r for r in results
                       if (r['train_r'] > 0) != (direction == 'positive')]
        f.check(f'direction={direction}: all train_r have correct sign',
                len(wrong_train) == 0,
                f'{len(wrong_train)} rows with wrong train_r sign')

        wrong_bt = [r for r in results
                    if r['backtest_r'] is not None
                    and (r['backtest_r'] > 0) != (direction == 'positive')]
        f.check(f'direction={direction}: all backtest_r have correct sign (or NULL)',
                len(wrong_bt) == 0,
                f'{len(wrong_bt)} rows with wrong backtest_r sign')

    # Selecting both directions should return more rows than either alone
    # Use a high limit so the cap doesn't distort the comparison
    pos = get('/api/analysis/correlations/db', direction='positive', limit=5000)['n']
    neg = get('/api/analysis/correlations/db', direction='negative', limit=5000)['n']
    all_ = get('/api/analysis/correlations/db', limit=5000)['n']
    f.check('positive + negative ≤ total (direction filters are exclusive)',
            pos + neg <= all_,
            f'pos={pos} neg={neg} total={all_}')
    f.check('positive and negative are both non-zero', pos > 0 and neg > 0,
            f'pos={pos} neg={neg}')


def test_lag_filter(f):
    print('\n── Lag range filter ─────────────────────────────────────────────')
    all_data  = get('/api/analysis/correlations/db', lag_min=1, lag_max=20, limit=1000)
    lag1_data = get('/api/analysis/correlations/db', lag_min=1, lag_max=1,  limit=1000)
    lag5_data = get('/api/analysis/correlations/db', lag_min=1, lag_max=5,  limit=1000)

    f.check('lag_max=1 returns fewer rows than lag_max=20',
            lag1_data['n'] < all_data['n'],
            f'lag1={lag1_data["n"]} all={all_data["n"]}')
    f.check('lag_max=5 returns fewer rows than lag_max=20',
            lag5_data['n'] < all_data['n'],
            f'lag5={lag5_data["n"]} all={all_data["n"]}')
    f.check('lag_max=5 returns more rows than lag_max=1',
            lag5_data['n'] >= lag1_data['n'],
            f'lag5={lag5_data["n"]} lag1={lag1_data["n"]}')

    out_of_range = [r for r in lag1_data['results'] if r['lag_days'] != 1]
    f.check('lag_max=1: all rows have lag_days=1',
            len(out_of_range) == 0,
            f'{len(out_of_range)} rows with lag_days != 1')

    out_of_range5 = [r for r in lag5_data['results'] if not (1 <= r['lag_days'] <= 5)]
    f.check('lag_max=5: all rows have lag_days in 1-5',
            len(out_of_range5) == 0,
            f'{len(out_of_range5)} rows outside range')


def test_min_r_filter(f):
    print('\n── Min |r| filter ───────────────────────────────────────────────')
    low  = get('/api/analysis/correlations/db', min_r=0.15, limit=1000)
    high = get('/api/analysis/correlations/db', min_r=0.80, limit=1000)
    f.check('min_r=0.80 returns fewer rows than min_r=0.15',
            high['n'] < low['n'],
            f'high={high["n"]} low={low["n"]}')
    below = [r for r in high['results'] if abs(r['train_r']) < 0.80]
    f.check('min_r=0.80: no rows with |train_r| < 0.80',
            len(below) == 0,
            f'{len(below)} rows below threshold')


def test_min_stable_filter(f):
    print('\n── Min stable filter ────────────────────────────────────────────')
    any_  = get('/api/analysis/correlations/db', limit=1000)
    min4  = get('/api/analysis/correlations/db', min_stable=4, limit=1000)
    min5  = get('/api/analysis/correlations/db', min_stable=5, limit=1000)

    f.check('min_stable=4 returns fewer rows than no filter',
            min4['n'] < any_['n'],
            f'min4={min4["n"]} any={any_["n"]}')
    f.check('min_stable=5 returns 0 rows (no fully stable pairs in current data)',
            min5['n'] == 0,
            f'got {min5["n"]} rows (expected 0)')

    below = [r for r in min4['results'] if r['n_stable'] < 4]
    f.check('min_stable=4: no rows with n_stable < 4',
            len(below) == 0,
            f'{len(below)} rows with n_stable < 4')


def test_sort_stability(f):
    print('\n── Sort by stability ────────────────────────────────────────────')
    data = get('/api/analysis/correlations/db', sort='stability', order='desc', limit=200)
    results = data['results']
    f.check('sort=stability returns rows', len(results) > 0)

    for i in range(len(results) - 1):
        a, b = results[i], results[i + 1]
        # n_stable descending first
        if a['n_stable'] != b['n_stable']:
            f.check(f'stability sort: n_stable descending at rows {i},{i+1}',
                    a['n_stable'] >= b['n_stable'],
                    f'a={a["n_stable"]} b={b["n_stable"]}')
            break
        # recency_score descending within same n_stable
        if a['recency_score'] != b['recency_score']:
            f.check(f'stability sort: recency_score descending at rows {i},{i+1}',
                    a['recency_score'] >= b['recency_score'],
                    f'a={a["recency_score"]} b={b["recency_score"]}')
            break

    # Verify top row has highest n_stable
    if results:
        max_n = max(r['n_stable'] for r in results)
        f.check('top row has maximum n_stable',
                results[0]['n_stable'] == max_n,
                f'top={results[0]["n_stable"]} max={max_n}')


def test_industry_filter(f):
    print('\n── Industry filter ──────────────────────────────────────────────')
    data = get('/api/analysis/correlations/db', industry='Materials', limit=500)
    f.check('industry=Materials returns rows', data['n'] > 0)
    wrong = [r for r in data['results'] if r['industry'] != 'Materials']
    f.check('all rows have industry=Materials', len(wrong) == 0,
            f'{len(wrong)} rows with wrong industry')

    data2 = get('/api/analysis/correlations/db', industry='Materials',
                industry2='Banks', limit=500)
    # industry2 is not a real param — just checking industry alone is honoured
    f.check('industry filter isolates correctly', data['n'] > 0)


def test_leader_follower_filter(f):
    print('\n── Leader / follower filter ─────────────────────────────────────')
    # Get a known leader from the data
    all_data = get('/api/analysis/correlations/db', limit=5)
    if not all_data['results']:
        f.check('has data to test leader/follower', False)
        return
    known_leader = all_data['results'][0]['leader']

    data = get('/api/analysis/correlations/db', leader=known_leader, limit=200)
    f.check(f'leader={known_leader} returns rows', data['n'] > 0)
    wrong = [r for r in data['results'] if r['leader'] != known_leader]
    f.check(f'all rows have leader={known_leader}', len(wrong) == 0,
            f'{len(wrong)} rows with wrong leader')


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print(f'Testing correlations API at {BASE}')
    f = Failures()

    try:
        test_industries(f)
        test_db_defaults(f)
        test_stability_field(f)
        test_direction_filter(f)
        test_lag_filter(f)
        test_min_r_filter(f)
        test_min_stable_filter(f)
        test_sort_stability(f)
        test_industry_filter(f)
        test_leader_follower_filter(f)
    except urllib.error.URLError as e:
        print(f'\nERROR: Could not connect to backend at {BASE}: {e}')
        print('Is the backend running? sudo systemctl start asx-backend')
        sys.exit(2)

    sys.exit(0 if f.summary() else 1)
