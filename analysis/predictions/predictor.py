"""Generate current signal scores per symbol → ranked output JSON."""

import json
import os
import time
import numpy as np
import pandas as pd
import torch

from ..core.feature_matrix import FeatureMatrix
from ..signals.base import Signal


class Predictor:
    """Compute current signal scores for all symbols.

    Uses the most recent window of data (not split-filtered) to score all symbols.

    Args:
        signal: a fitted Signal instance
        fm: FeatureMatrix built from recent data ('all' split or 'backtest')
        symbols_df: DataFrame with [symbol, name, industry]
    """

    def __init__(self, signal: Signal, fm: FeatureMatrix, symbols_df: pd.DataFrame | None = None):
        self.signal = signal
        self.fm = fm
        self.symbols_df = symbols_df

    def predict(self) -> list[dict]:
        """Score all symbols on the latest available date.

        Returns list of dicts sorted by signal score descending.
        """
        features = self.fm.build()
        mask = self.fm.mask
        sig = self.signal.compute(features, mask)  # (N_sym, N_dates)

        # Take last date that has data
        N, T = sig.shape
        last_scores = sig[:, -1].cpu().numpy()
        symbols = self.fm.symbols
        last_date = int(self.fm.dates[-1])

        sym_meta = {}
        if self.symbols_df is not None:
            for _, row in self.symbols_df.iterrows():
                sym_meta[row['symbol']] = {
                    'name': row.get('name', ''),
                    'industry': row.get('industry', ''),
                }

        rows = []
        for i, sym in enumerate(symbols):
            score = last_scores[i]
            if np.isnan(score):
                continue
            row = dict(
                symbol=sym,
                score=round(float(score), 4),
                signal=self.signal.name,
                date=last_date,
            )
            if sym in sym_meta:
                row.update(sym_meta[sym])
            rows.append(row)

        rows.sort(key=lambda r: r['score'], reverse=True)
        return rows

    def save(self, output_dir: str = 'analysis/results') -> str:
        """Save predictions to JSON. Returns file path."""
        os.makedirs(output_dir, exist_ok=True)
        predictions = self.predict()
        out = dict(
            signal=self.signal.name,
            generated_at=int(time.time()),
            n_symbols=len(predictions),
            predictions=predictions,
        )
        fname = os.path.join(output_dir, f"predictions_{self.signal.name}.json")
        with open(fname, 'w') as f:
            json.dump(out, f, indent=2)
        print(f"[Predictor] Saved {len(predictions)} predictions → {fname}")
        return fname


def load_latest_predictions(results_dir: str = 'analysis/results') -> dict:
    """Load all prediction files and merge into combined output."""
    combined = {}
    for fname in os.listdir(results_dir):
        if fname.startswith('predictions_') and fname.endswith('.json'):
            fpath = os.path.join(results_dir, fname)
            with open(fpath) as f:
                data = json.load(f)
            signal_name = data.get('signal', fname)
            combined[signal_name] = data
    return combined
