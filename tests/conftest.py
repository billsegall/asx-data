import pytest
import urllib.request

BASE = 'http://localhost:8082'


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
