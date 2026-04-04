#!/usr/bin/env python3
"""Recalculate volume brackets monthly and update config file."""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

STOCKDB = Path(__file__).parent.parent / 'stockdb' / 'stockdb.db'
CONFIG_FILE = Path(__file__).parent.parent / 'stockdb' / 'volume_config.json'

def calculate_breakpoints():
    """Calculate P60, P70, P80, P90 breakpoints from stockdb."""
    conn = sqlite3.connect(STOCKDB)
    c = conn.cursor()

    try:
        # Get the percentile breakpoints
        rows = c.execute('''
            WITH avg_volumes AS (
              SELECT AVG(close * volume) as avg_dollar_vol
              FROM endofday
              GROUP BY symbol
              HAVING avg_dollar_vol > 0
            ),
            ranked AS (
              SELECT
                avg_dollar_vol,
                ROW_NUMBER() OVER (ORDER BY avg_dollar_vol) as row_num,
                (SELECT COUNT(*) FROM avg_volumes) as total
              FROM avg_volumes
            )
            SELECT
              (SELECT avg_dollar_vol FROM ranked WHERE row_num = CAST((SELECT MAX(total) * 0.60 FROM ranked) AS INTEGER)) as p60,
              (SELECT avg_dollar_vol FROM ranked WHERE row_num = CAST((SELECT MAX(total) * 0.70 FROM ranked) AS INTEGER)) as p70,
              (SELECT avg_dollar_vol FROM ranked WHERE row_num = CAST((SELECT MAX(total) * 0.80 FROM ranked) AS INTEGER)) as p80,
              (SELECT avg_dollar_vol FROM ranked WHERE row_num = CAST((SELECT MAX(total) * 0.90 FROM ranked) AS INTEGER)) as p90
        ''').fetchone()

        p60, p70, p80, p90 = rows
        return {
            'p60': round(p60, 2),
            'p70': round(p70, 2),
            'p80': round(p80, 2),
            'p90': round(p90, 2)
        }
    finally:
        conn.close()

def update_config(breakpoints):
    """Update volume_config.json with new breakpoints."""
    config = {
        'last_updated': datetime.now().isoformat(),
        'brackets': [
            {
                'bucket': 1,
                'label': 'Low',
                'max': int(breakpoints['p60']),
                'description': f"<${breakpoints['p60']/1000:.1f}K (60% of stocks)"
            },
            {
                'bucket': 2,
                'label': 'Medium-Low',
                'max': int(breakpoints['p70']),
                'description': f"${breakpoints['p60']/1000:.1f}K-${breakpoints['p70']/1000:.1f}K (10% of stocks)"
            },
            {
                'bucket': 3,
                'label': 'Medium',
                'max': int(breakpoints['p80']),
                'description': f"${breakpoints['p70']/1000:.1f}K-${breakpoints['p80']/1000:.1f}K (10% of stocks)"
            },
            {
                'bucket': 4,
                'label': 'Medium-High',
                'max': int(breakpoints['p90']),
                'description': f"${breakpoints['p80']/1000:.1f}K-${breakpoints['p90']/1000:.1f}K (10% of stocks)"
            },
            {
                'bucket': 5,
                'label': 'High',
                'max': None,
                'description': f">${breakpoints['p90']/1000:.1f}K (10% of stocks)"
            }
        ]
    }

    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)

    print(f'Updated {CONFIG_FILE}')
    print(f'P60: ${breakpoints["p60"]:,.2f}')
    print(f'P70: ${breakpoints["p70"]:,.2f}')
    print(f'P80: ${breakpoints["p80"]:,.2f}')
    print(f'P90: ${breakpoints["p90"]:,.2f}')

if __name__ == '__main__':
    try:
        breakpoints = calculate_breakpoints()
        update_config(breakpoints)
    except Exception as e:
        print(f'Error: {e}')
        exit(1)
