# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""SQLite → pandas data loader with split filtering.

Split values: 'train' (before TRAIN_CUTOFF_TS), 'backtest' (>= TRAIN_CUTOFF_TS), 'all'.
"""

import sqlite3
import pandas as pd
from .train_test_split import TRAIN_CUTOFF_TS


class DataLoader:
    def __init__(self, db_path: str, split: str = 'train'):
        assert split in ('train', 'backtest', 'all'), f"Invalid split: {split!r}"
        self.db_path = db_path
        self.split = split

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA cache_size=-131072")  # 128MB
        return conn

    def _date_filter_sql(self, col: str = 'date') -> tuple[str, list]:
        """Return (WHERE clause fragment, params) for the current split."""
        if self.split == 'train':
            return f"{col} < ?", [TRAIN_CUTOFF_TS]
        elif self.split == 'backtest':
            return f"{col} >= ?", [TRAIN_CUTOFF_TS]
        else:
            return "1=1", []

    def load_eod(self, symbols=None, min_history_days: int = 252) -> pd.DataFrame:
        """Load endofday OHLCV filtered by split. Returns long-format DataFrame."""
        date_clause, date_params = self._date_filter_sql('date')

        sym_clause = ""
        sym_params = []
        if symbols:
            placeholders = ','.join('?' * len(symbols))
            sym_clause = f" AND symbol IN ({placeholders})"
            sym_params = list(symbols)

        query = f"""
            SELECT symbol, date, open, high, low, close, volume
            FROM endofday
            WHERE {date_clause}{sym_clause}
            ORDER BY symbol, date
        """

        with self._conn() as conn:
            df = pd.read_sql_query(query, conn, params=date_params + sym_params)

        df['date'] = pd.to_datetime(df['date'], unit='s')

        if min_history_days > 0 and self.split != 'backtest':
            counts = df.groupby('symbol')['date'].count()
            valid = counts[counts >= min_history_days].index
            df = df[df['symbol'].isin(valid)]

        return df

    def load_shorts(self, symbols=None) -> pd.DataFrame:
        """Load shorts data filtered by split. Returns long-format DataFrame."""
        date_clause, date_params = self._date_filter_sql('date')

        sym_clause = ""
        sym_params = []
        if symbols:
            placeholders = ','.join('?' * len(symbols))
            sym_clause = f" AND symbol IN ({placeholders})"
            sym_params = list(symbols)

        query = f"""
            SELECT symbol, date, short
            FROM shorts
            WHERE {date_clause}{sym_clause}
            ORDER BY symbol, date
        """

        with self._conn() as conn:
            df = pd.read_sql_query(query, conn, params=date_params + sym_params)

        df['date'] = pd.to_datetime(df['date'], unit='s')
        return df

    def load_symbols(self) -> pd.DataFrame:
        """Load symbols metadata (no date filter)."""
        query = "SELECT symbol, name, industry, shares FROM symbols"
        with self._conn() as conn:
            return pd.read_sql_query(query, conn)

    def get_active_symbols(self, min_days: int = 252) -> list[str]:
        """Symbols with at least min_days of EOD data in this split."""
        date_clause, date_params = self._date_filter_sql('date')
        query = f"""
            SELECT symbol, COUNT(*) as n
            FROM endofday
            WHERE {date_clause}
            GROUP BY symbol
            HAVING n >= ?
        """
        with self._conn() as conn:
            df = pd.read_sql_query(query, conn, params=date_params + [min_days])
        return df['symbol'].tolist()
