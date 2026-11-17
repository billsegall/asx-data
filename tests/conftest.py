import atexit
import os
import shutil
import sqlite3
import tempfile
import urllib.request

import pytest

BASE = 'http://localhost:8082'

_REAL_DB = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'stockdb', 'stockdb.db'))


def _schema_only_db():
    """An empty database with stockdb.db's schema, for tests that import backend/api.py.

    api.py runs its startup migrations (including the symbols.current refresh)
    against DATABASE as soon as it's imported. Pointed at the real stockdb.db,
    the nightly test run used to rewrite production flags. Set here, at conftest
    import, because test modules import api at collection time.
    """
    tmp = tempfile.mkdtemp(prefix='asx-data-tests-')
    atexit.register(shutil.rmtree, tmp, ignore_errors=True)
    path = os.path.join(tmp, 'stockdb.db')
    src = sqlite3.connect(f'file:{_REAL_DB}?mode=ro', uri=True)
    try:
        ddl = [r[0] for r in src.execute(
            "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'"
            " ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'view' THEN 1 WHEN 'index' THEN 2 ELSE 3 END")]
    finally:
        src.close()
    dst = sqlite3.connect(path)
    for statement in ddl:
        dst.execute(statement)
    dst.commit()
    dst.close()
    return path


if 'DATABASE' not in os.environ and os.path.exists(_REAL_DB):
    os.environ['DATABASE'] = _schema_only_db()


def _backend_available():
    try:
        urllib.request.urlopen(BASE + '/api/symbols?q=BHP', timeout=2)
        return True
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    if not _backend_available():
        skip = pytest.mark.skip(reason="backend not running at localhost:8082")
        for item in items:
            if 'test_correlations' in str(item.fspath):
                item.add_marker(skip)


class Failures:
    def __init__(self):
        self.items = []

    def check(self, name, condition, detail=''):
        if not condition:
            msg = name
            if detail:
                msg += f': {detail}'
            self.items.append(msg)


@pytest.fixture
def f():
    failures = Failures()
    yield failures
    if failures.items:
        msg = '\n'.join(f'  FAIL  {i}' for i in failures.items)
        pytest.fail(f'{len(failures.items)} check(s) failed:\n{msg}')
