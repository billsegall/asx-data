#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
fetch_financials.py — Fetch annual financial statements from Yahoo Finance.

Stores 4-5 years of annual income statement, balance sheet, and cash flow data
per symbol. Each (symbol, fiscal_year_end) row is INSERT OR REPLACE, so re-runs
are safe. Complements fetch_fundamentals.py which stores weekly snapshot metrics.

Usage:
  python3 fetch_financials.py [--db /path/to/stockdb.db] [--delay SECONDS]
  python3 fetch_financials.py --symbols BHP RIO WDS
"""

import argparse
import datetime
import os
import sys
import time
import sqlite3

import yfinance as yf
import pandas as pd


DELAY = 0.5        # seconds between symbols; ~1500 symbols ≈ 12 min
LOG_EVERY = 100


def _float(val):
    """Return float or None. Rejects inf/nan."""
    try:
        f = float(val)
        if f != f or abs(f) > 1e308:
            return None
        return f
    except (TypeError, ValueError):
        return None


def create_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS financials_annual (
            symbol              TEXT NOT NULL,
            fiscal_year_end     TEXT NOT NULL,  -- YYYY-MM-DD (column date from yfinance)
            fetched_at          TEXT NOT NULL,

            -- Income statement
            total_revenue       REAL,
            gross_profit        REAL,
            operating_income    REAL,
            net_income          REAL,
            ebitda              REAL,
            basic_eps           REAL,
            interest_expense    REAL,
            tax_provision       REAL,

            -- Cash flow
            operating_cashflow  REAL,
            free_cashflow       REAL,
            capital_expenditure REAL,
            dividends_paid      REAL,

            -- Balance sheet
            total_assets        REAL,
            total_debt          REAL,
            stockholders_equity REAL,
            cash                REAL,
            total_liabilities   REAL,

            PRIMARY KEY (symbol, fiscal_year_end)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_fin_sym ON financials_annual(symbol)')
    conn.commit()


def _row_val(df, *keys):
    """Get a value from a DataFrame row, trying multiple key names."""
    if df is None or df.empty:
        return None
    for key in keys:
        if key in df.index:
            return key
    return None


def _get(df, *keys):
    """Get first non-null value from a DataFrame for the given row keys."""
    if df is None or df.empty:
        return {}
    for key in keys:
        if key in df.index:
            row = df.loc[key]
            return {str(col.date()): _float(row[col]) for col in df.columns}
    return {}


def fetch_symbol(ticker_str):
    """Fetch annual financial statements. Returns dict of {fiscal_year_end: row_dict} or None."""
    try:
        t = yf.Ticker(ticker_str)
        inc = t.income_stmt
        bal = t.balance_sheet
        cf  = t.cashflow

        if inc is None or inc.empty:
            return None

        # Get all fiscal year end dates from income statement
        dates = [str(col.date()) for col in inc.columns]
        if not dates:
            return None

        results = {}
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        def val(df, *keys):
            """Return dict of date→value for the first matching row key."""
            if df is None or df.empty:
                return {}
            for k in keys:
                if k in df.index:
                    return {str(c.date()): _float(df.loc[k, c]) for c in df.columns}
            return {}

        rev      = val(inc, 'Total Revenue')
        gp       = val(inc, 'Gross Profit')
        oi       = val(inc, 'Operating Income', 'Total Operating Income As Reported')
        ni       = val(inc, 'Net Income')
        ebitda   = val(inc, 'EBITDA')
        eps      = val(inc, 'Basic EPS')
        int_exp  = val(inc, 'Interest Expense', 'Interest Expense Non Operating')
        tax      = val(inc, 'Tax Provision')

        opcf     = val(cf,  'Operating Cash Flow')
        fcf      = val(cf,  'Free Cash Flow')
        capex    = val(cf,  'Capital Expenditure', 'Purchase Of PPE')
        divpaid  = val(cf,  'Cash Dividends Paid', 'Common Stock Dividend Paid')

        ta       = val(bal, 'Total Assets')
        td       = val(bal, 'Total Debt')
        eq       = val(bal, 'Stockholders Equity', 'Common Stock Equity')
        cash     = val(bal, 'Cash And Cash Equivalents',
                           'Cash Cash Equivalents And Short Term Investments')
        tl       = val(bal, 'Total Liabilities Net Minority Interest')

        for d in dates:
            results[d] = {
                'fetched_at':          now,
                'total_revenue':       rev.get(d),
                'gross_profit':        gp.get(d),
                'operating_income':    oi.get(d),
                'net_income':          ni.get(d),
                'ebitda':              ebitda.get(d),
                'basic_eps':           eps.get(d),
                'interest_expense':    int_exp.get(d),
                'tax_provision':       tax.get(d),
                'operating_cashflow':  opcf.get(d),
                'free_cashflow':       fcf.get(d),
                'capital_expenditure': capex.get(d),
                'dividends_paid':      divpaid.get(d),
                'total_assets':        ta.get(d),
                'total_debt':          td.get(d),
                'stockholders_equity': eq.get(d),
                'cash':                cash.get(d),
                'total_liabilities':   tl.get(d),
            }

        return results

    except Exception:
        return None


INSERT_SQL = '''
    INSERT OR REPLACE INTO financials_annual
    (symbol, fiscal_year_end, fetched_at,
     total_revenue, gross_profit, operating_income, net_income, ebitda,
     basic_eps, interest_expense, tax_provision,
     operating_cashflow, free_cashflow, capital_expenditure, dividends_paid,
     total_assets, total_debt, stockholders_equity, cash, total_liabilities)
    VALUES (?,?,?, ?,?,?,?,?,?,?,?, ?,?,?,?, ?,?,?,?,?)
'''


def main():
    parser = argparse.ArgumentParser(description='Fetch annual financial statements')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_db = os.path.join(script_dir, '..', 'stockdb', 'stockdb.db')
    parser.add_argument('--db', default=os.environ.get('STOCKDB', default_db))
    parser.add_argument('--delay', type=float, default=DELAY)
    parser.add_argument('--symbols', nargs='+',
                        help='Fetch specific symbols only (default: all current)')
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute('PRAGMA journal_mode=WAL')
    create_table(conn)

    if args.symbols:
        symbols = [s.upper() for s in args.symbols]
    else:
        symbols = [r[0] for r in conn.execute(
            """SELECT s.symbol FROM symbols s
               WHERE s.current = 1
               AND EXISTS (
                   SELECT 1 FROM endofday e
                   WHERE e.symbol = s.symbol
                   AND e.date >= strftime('%s', 'now', '-30 days')
               )
               ORDER BY s.symbol"""
        ).fetchall()]

    total = len(symbols)
    print(f'{datetime.datetime.now():%Y-%m-%d %H:%M:%S}  Fetching annual financials for {total} symbols '
          f'(delay={args.delay}s, est. {total * args.delay / 60:.0f} min)')

    ok = skipped = errors = 0

    for i, symbol in enumerate(symbols, 1):
        ticker_str = '^AORD' if symbol == 'XAO' else f'{symbol}.AX'
        data = fetch_symbol(ticker_str)

        if data is None:
            skipped += 1
        else:
            try:
                for fiscal_year_end, row in data.items():
                    conn.execute(INSERT_SQL, (
                        symbol, fiscal_year_end, row['fetched_at'],
                        row['total_revenue'], row['gross_profit'],
                        row['operating_income'], row['net_income'],
                        row['ebitda'], row['basic_eps'],
                        row['interest_expense'], row['tax_provision'],
                        row['operating_cashflow'], row['free_cashflow'],
                        row['capital_expenditure'], row['dividends_paid'],
                        row['total_assets'], row['total_debt'],
                        row['stockholders_equity'], row['cash'],
                        row['total_liabilities'],
                    ))
                conn.commit()
                ok += 1
            except Exception as e:
                print(f'  ERROR inserting {symbol}: {e}', file=sys.stderr)
                errors += 1

        if i % LOG_EVERY == 0 or i == total:
            pct = 100 * i / total
            print(f'  {i}/{total} ({pct:.0f}%)  ok={ok}  skipped={skipped}  errors={errors}')

        if i < total and args.delay > 0:
            time.sleep(args.delay)

    conn.close()
    print(f'{datetime.datetime.now():%Y-%m-%d %H:%M:%S}  Done: {ok} upserted, '
          f'{skipped} skipped, {errors} errors')


if __name__ == '__main__':
    main()
