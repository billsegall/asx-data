#!/usr/bin/env python3
# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""
fetch_fundamentals.py — Fetch fundamental data from Yahoo Finance and store in stockdb.db.

Fetches one symbol at a time (yfinance Ticker.info cannot batch). Runs weekly —
typically Friday evening after EOD prices are updated.

Re-run safe: INSERT OR REPLACE upserts by (symbol, date), so re-running on the same
day replaces that day's snapshot. Each new weekly run appends a new dated row,
preserving full history.

Usage:
  python3 fetch_fundamentals.py [--db /path/to/stockdb.db] [--delay SECONDS]
"""

import argparse
import os
import sqlite3
import subprocess
import time
import datetime
import sys

import yfinance as yf

DELAY = 0.4          # seconds between requests; ~1500 symbols ≈ 10 minutes
LOG_EVERY = 100      # print progress every N symbols


def _float(val):
    """Return float or None. Rejects inf/nan (not valid JSON)."""
    try:
        f = float(val) if val is not None else None
        if f is None or (f == f and abs(f) <= 1e308):
            return f
        return None  # inf or nan
    except (TypeError, ValueError):
        return None


def _int(val):
    """Return int or None."""
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _create_new_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS fundamentals (
            symbol                      TEXT NOT NULL,
            date                        TEXT NOT NULL,  -- YYYY-MM-DD snapshot date
            fetched_at                  TEXT NOT NULL,

            -- Valuation
            market_cap                  REAL,
            enterprise_value            REAL,
            trailing_pe                 REAL,
            forward_pe                  REAL,
            price_to_book               REAL,
            price_to_sales              REAL,
            enterprise_to_revenue       REAL,
            enterprise_to_ebitda        REAL,

            -- Profitability
            profit_margins              REAL,
            operating_margins           REAL,
            gross_margins               REAL,
            ebitda_margins              REAL,
            return_on_assets            REAL,
            return_on_equity            REAL,

            -- Growth
            revenue_growth              REAL,
            earnings_growth             REAL,

            -- Income / balance sheet
            total_revenue               REAL,
            ebitda                      REAL,
            net_income                  REAL,
            free_cashflow               REAL,
            operating_cashflow          REAL,
            total_cash                  REAL,
            total_debt                  REAL,
            debt_to_equity              REAL,
            current_ratio               REAL,
            quick_ratio                 REAL,
            eps_trailing                REAL,
            eps_forward                 REAL,

            -- Dividends
            dividend_yield              REAL,
            dividend_rate               REAL,
            payout_ratio                REAL,
            five_year_avg_div_yield     REAL,
            ex_dividend_date            INTEGER,
            last_dividend_value         REAL,

            -- Analyst consensus
            recommendation_mean         REAL,
            recommendation_key          TEXT,
            analyst_count               INTEGER,
            target_mean_price           REAL,
            target_high_price           REAL,
            target_low_price            REAL,
            target_median_price         REAL,

            -- Risk / volatility
            beta                        REAL,
            week52_change               REAL,

            -- Ownership
            shares_outstanding          REAL,
            float_shares                REAL,
            held_pct_insiders           REAL,
            held_pct_institutions       REAL,

            -- Description
            business_summary            TEXT,

            -- Company info
            full_time_employees         INTEGER,
            website                     TEXT,
            sector                      TEXT,
            country                     TEXT,
            city                        TEXT,
            long_name                   TEXT,

            -- Technical reference
            fifty_day_average           REAL,
            two_hundred_day_average     REAL,
            average_volume              INTEGER,
            all_time_high               REAL,
            all_time_low                REAL,

            -- Governance risk scores (1=low risk, 10=high risk)
            audit_risk                  INTEGER,
            board_risk                  INTEGER,
            compensation_risk           INTEGER,
            shareholder_rights_risk     INTEGER,
            overall_risk                INTEGER,

            -- Per-share metrics
            book_value                  REAL,
            revenue_per_share           REAL,
            total_cash_per_share        REAL,
            eps_current_year            REAL,
            price_eps_current_year      REAL,
            trailing_peg_ratio          REAL,

            -- Dates (unix timestamps)
            earnings_timestamp          INTEGER,
            last_fiscal_year_end        INTEGER,
            next_fiscal_year_end        INTEGER,
            most_recent_quarter         INTEGER,

            -- Additional growth
            earnings_quarterly_growth   REAL,

            -- Dividend date
            last_dividend_date          INTEGER,

            PRIMARY KEY (symbol, date)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_fund_sym_date ON fundamentals(symbol, date)')


def create_table(conn):
    """Create or migrate the fundamentals table. Returns True if table already existed."""
    existing = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='fundamentals'"
    ).fetchone() is not None

    if existing:
        cols = {r[1] for r in conn.execute('PRAGMA table_info(fundamentals)').fetchall()}
        new_cols = [
            ('business_summary',        'TEXT'),
            ('full_time_employees',      'INTEGER'),
            ('website',                  'TEXT'),
            ('sector',                   'TEXT'),
            ('country',                  'TEXT'),
            ('city',                     'TEXT'),
            ('long_name',                'TEXT'),
            ('fifty_day_average',        'REAL'),
            ('two_hundred_day_average',  'REAL'),
            ('average_volume',           'INTEGER'),
            ('all_time_high',            'REAL'),
            ('all_time_low',             'REAL'),
            ('audit_risk',               'INTEGER'),
            ('board_risk',               'INTEGER'),
            ('compensation_risk',        'INTEGER'),
            ('shareholder_rights_risk',  'INTEGER'),
            ('overall_risk',             'INTEGER'),
            ('book_value',               'REAL'),
            ('revenue_per_share',        'REAL'),
            ('total_cash_per_share',     'REAL'),
            ('eps_current_year',         'REAL'),
            ('price_eps_current_year',   'REAL'),
            ('trailing_peg_ratio',       'REAL'),
            ('earnings_timestamp',       'INTEGER'),
            ('last_fiscal_year_end',     'INTEGER'),
            ('next_fiscal_year_end',     'INTEGER'),
            ('most_recent_quarter',      'INTEGER'),
            ('earnings_quarterly_growth','REAL'),
            ('last_dividend_date',       'INTEGER'),
        ]
        added = [(c, t) for c, t in new_cols if c not in cols]
        if added:
            for col, coltype in added:
                conn.execute(f'ALTER TABLE fundamentals ADD COLUMN {col} {coltype}')
            conn.commit()
            print(f'Added {len(added)} new column(s) to fundamentals.')
        if 'date' not in cols:
            print('Migrating fundamentals table to composite (symbol, date) primary key...')
            conn.execute('ALTER TABLE fundamentals RENAME TO fundamentals_v1')
            _create_new_table(conn)
            conn.execute('''
                INSERT INTO fundamentals
                SELECT symbol, substr(fetched_at, 1, 10), fetched_at,
                       market_cap, enterprise_value, trailing_pe, forward_pe,
                       price_to_book, price_to_sales, enterprise_to_revenue, enterprise_to_ebitda,
                       profit_margins, operating_margins, gross_margins, ebitda_margins,
                       return_on_assets, return_on_equity,
                       revenue_growth, earnings_growth,
                       total_revenue, ebitda, net_income, free_cashflow, operating_cashflow,
                       total_cash, total_debt, debt_to_equity, current_ratio, quick_ratio,
                       eps_trailing, eps_forward,
                       dividend_yield, dividend_rate, payout_ratio, five_year_avg_div_yield,
                       ex_dividend_date, last_dividend_value,
                       recommendation_mean, recommendation_key, analyst_count,
                       target_mean_price, target_high_price, target_low_price, target_median_price,
                       beta, week52_change,
                       shares_outstanding, float_shares, held_pct_insiders, held_pct_institutions
                FROM fundamentals_v1
            ''')
            conn.execute('DROP TABLE fundamentals_v1')
            conn.commit()
            print('Migration complete.')
        return True

    _create_new_table(conn)
    conn.commit()
    return False  # newly created


def fetch_symbol(ticker_str):
    """Return info dict for ticker_str (e.g. 'BHP.AX') or None on failure."""
    try:
        t = yf.Ticker(ticker_str)
        info = t.info
        # yfinance returns a minimal dict (just quoteType etc.) for unknown symbols
        if not info or info.get('quoteType') not in ('EQUITY', 'ETF', 'MUTUALFUND'):
            return None
        return info
    except Exception:
        return None


def info_to_row(symbol, today, info):
    """Map yfinance info dict → tuple of values matching the INSERT statement."""
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return (
        symbol, today, now,

        # Valuation
        _float(info.get('marketCap')),
        _float(info.get('enterpriseValue')),
        _float(info.get('trailingPE')),
        _float(info.get('forwardPE')),
        _float(info.get('priceToBook')),
        _float(info.get('priceToSalesTrailing12Months')),
        _float(info.get('enterpriseToRevenue')),
        _float(info.get('enterpriseToEbitda')),

        # Profitability
        _float(info.get('profitMargins')),
        _float(info.get('operatingMargins')),
        _float(info.get('grossMargins')),
        _float(info.get('ebitdaMargins')),
        _float(info.get('returnOnAssets')),
        _float(info.get('returnOnEquity')),

        # Growth
        _float(info.get('revenueGrowth')),
        _float(info.get('earningsGrowth')),

        # Income / balance sheet
        _float(info.get('totalRevenue')),
        _float(info.get('ebitda')),
        _float(info.get('netIncomeToCommon')),
        _float(info.get('freeCashflow')),
        _float(info.get('operatingCashflow')),
        _float(info.get('totalCash')),
        _float(info.get('totalDebt')),
        _float(info.get('debtToEquity')),
        _float(info.get('currentRatio')),
        _float(info.get('quickRatio')),
        _float(info.get('trailingEps')),
        _float(info.get('forwardEps')),

        # Dividends
        _float(info.get('dividendYield')),
        _float(info.get('dividendRate')),
        _float(info.get('payoutRatio')),
        _float(info.get('fiveYearAvgDividendYield')),
        _int(info.get('exDividendDate')),
        _float(info.get('lastDividendValue')),

        # Analyst consensus
        _float(info.get('recommendationMean')),
        info.get('recommendationKey'),
        _int(info.get('numberOfAnalystOpinions')),
        _float(info.get('targetMeanPrice')),
        _float(info.get('targetHighPrice')),
        _float(info.get('targetLowPrice')),
        _float(info.get('targetMedianPrice')),

        # Risk / volatility
        _float(info.get('beta')),
        _float(info.get('52WeekChange')),

        # Ownership
        _float(info.get('sharesOutstanding')),
        _float(info.get('floatShares')),
        _float(info.get('heldPercentInsiders')),
        _float(info.get('heldPercentInstitutions')),

        # Description
        info.get('longBusinessSummary') or None,

        # Company info
        _int(info.get('fullTimeEmployees')),
        info.get('website') or None,
        info.get('sector') or None,
        info.get('country') or None,
        info.get('city') or None,
        info.get('longName') or None,

        # Technical reference
        _float(info.get('fiftyDayAverage')),
        _float(info.get('twoHundredDayAverage')),
        _int(info.get('averageVolume')),
        _float(info.get('allTimeHigh')),
        _float(info.get('allTimeLow')),

        # Governance risk scores
        _int(info.get('auditRisk')),
        _int(info.get('boardRisk')),
        _int(info.get('compensationRisk')),
        _int(info.get('shareHolderRightsRisk')),
        _int(info.get('overallRisk')),

        # Per-share metrics
        _float(info.get('bookValue')),
        _float(info.get('revenuePerShare')),
        _float(info.get('totalCashPerShare')),
        _float(info.get('epsCurrentYear')),
        _float(info.get('priceEpsCurrentYear')),
        _float(info.get('trailingPegRatio')),

        # Dates
        _int(info.get('earningsTimestamp')),
        _int(info.get('lastFiscalYearEnd')),
        _int(info.get('nextFiscalYearEnd')),
        _int(info.get('mostRecentQuarter')),

        # Growth
        _float(info.get('earningsQuarterlyGrowth')),

        # Dividend date
        _int(info.get('lastDividendDate')),
    )


INSERT_SQL = '''
    INSERT OR REPLACE INTO fundamentals VALUES (
        ?,?,?,  -- symbol, date, fetched_at
        ?,?,?,?,?,?,?,?,  -- valuation (8)
        ?,?,?,?,?,?,      -- profitability (6)
        ?,?,              -- growth (2)
        ?,?,?,?,?,?,?,?,?,?,?,?,  -- income/balance (12)
        ?,?,?,?,?,?,      -- dividends (6)
        ?,?,?,?,?,?,?,    -- analyst (7)
        ?,?,              -- risk (2)
        ?,?,?,?,          -- ownership (4)
        ?,                -- description (1)
        ?,?,?,?,?,?,      -- company info (6)
        ?,?,?,?,?,        -- technical reference (5)
        ?,?,?,?,?,        -- governance risk (5)
        ?,?,?,?,?,?,      -- per-share metrics (6)
        ?,?,?,?,          -- dates (4)
        ?,                -- quarterly growth (1)
        ?                 -- dividend date (1)
    )
'''


def main():
    parser = argparse.ArgumentParser(description='Fetch fundamentals from Yahoo Finance')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_db = os.path.join(script_dir, '..', 'stockdb', 'stockdb.db')
    parser.add_argument('--db', default=os.environ.get('STOCKDB', default_db))
    parser.add_argument('--delay', type=float, default=DELAY,
                        help=f'Seconds between requests (default: {DELAY})')
    parser.add_argument('--symbols', nargs='+',
                        help='Fetch specific symbols only (default: all current)')
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute('PRAGMA journal_mode=WAL')
    table_existed = create_table(conn)

    today = datetime.date.today().strftime('%Y-%m-%d')

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
    print(f'{datetime.datetime.now():%Y-%m-%d %H:%M:%S}  Fetching fundamentals for {total} symbols '
          f'(delay={args.delay}s, est. {total * args.delay / 60:.0f} min)')

    ok = skipped = errors = 0

    for i, symbol in enumerate(symbols, 1):
        ticker_str = '^AORD' if symbol == 'XAO' else f'{symbol}.AX'
        info = fetch_symbol(ticker_str)

        if info is None:
            skipped += 1
        else:
            try:
                row = info_to_row(symbol, today, info)
                conn.execute(INSERT_SQL, row)
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
          f'{skipped} skipped (no data), {errors} errors')

    if not table_existed:
        # Table was newly created — restart backend so its persistent DB connection
        # picks up the new schema (Python sqlite3 caches schema per connection).
        print('New fundamentals table created — restarting asx-backend...')
        try:
            subprocess.run(['sudo', 'systemctl', 'restart', 'asx-backend'], check=True)
            print('asx-backend restarted.')
        except Exception as e:
            print(f'WARNING: could not restart asx-backend: {e}', file=sys.stderr)
            print('Please run: sudo systemctl restart asx-backend')


if __name__ == '__main__':
    main()
