# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""KronosSignal: 5-day return forecasts from fine-tuned Kronos model."""

import json
import os
import sqlite3
import time

import pandas as pd


class KronosSignal:
    """Kronos 5-day forward return forecasts for all ASX symbols.

    Standalone — does not inherit from Signal because it needs per-symbol OHLCV
    DataFrames rather than the (N_sym, N_dates) FeatureMatrix tensor layout.
    Outputs the same predictions JSON format as Predictor.save().
    """

    name = "kronos"
    description = "Kronos 5-day return forecast (fine-tuned on ASX data)"

    def __init__(
        self,
        db_path: str,
        model_dir: str,
        tokenizer_dir: str,
        lookback: int = 400,
        pred_len: int = 5,
        batch_size: int = 200,
        device: str = 'cuda',
    ):
        self.db_path = db_path
        self.model_dir = model_dir
        self.tokenizer_dir = tokenizer_dir
        self.lookback = lookback
        self.pred_len = pred_len
        self.batch_size = batch_size
        self.device = device

    def score_current(self) -> list[dict]:
        """Run inference for the latest available date; return sorted predictions."""
        from ..kronos.loader import load_all_ohlcv
        from ..kronos.inference import build_predictor, forecast_5d_returns

        print(f"[{self.name}] Loading OHLCV...")
        ohlcv = load_all_ohlcv(self.db_path)
        if not ohlcv:
            return []

        eval_date = max(df.index[-1] for df in ohlcv.values()).strftime('%Y-%m-%d')
        print(f"[{self.name}] Eval date: {eval_date}  symbols: {len(ohlcv)}")

        print(f"[{self.name}] Loading model from {self.model_dir}")
        predictor = build_predictor(self.model_dir, self.tokenizer_dir, self.device)

        forecasts = forecast_5d_returns(
            predictor,
            ohlcv,
            eval_date,
            lookback=self.lookback,
            pred_len=self.pred_len,
            batch_size=self.batch_size,
        )
        print(f"[{self.name}] Forecasts: {len(forecasts)} symbols")

        # Load symbol metadata for names/industries
        sym_meta = _load_symbol_meta(self.db_path)

        rows = []
        for sym, ret in forecasts.items():
            row = dict(symbol=sym, score=round(float(ret), 6), signal=self.name,
                       date=int(pd.Timestamp(eval_date).timestamp()))
            if sym in sym_meta:
                row.update(sym_meta[sym])
            rows.append(row)

        rows.sort(key=lambda r: r['score'], reverse=True)
        return rows

    def save(self, output_dir: str = 'analysis/results') -> str:
        """Score current date and save predictions JSON. Returns file path."""
        os.makedirs(output_dir, exist_ok=True)
        predictions = self.score_current()
        out = dict(
            signal=self.name,
            generated_at=int(time.time()),
            n_symbols=len(predictions),
            predictions=predictions,
        )
        fname = os.path.join(output_dir, f"predictions_{self.name}.json")
        with open(fname, 'w') as f:
            json.dump(out, f, indent=2)
        print(f"[{self.name}] Saved {len(predictions)} predictions → {fname}")
        return fname


def _load_symbol_meta(db_path: str) -> dict[str, dict]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT symbol, name, industry FROM symbols").fetchall()
    finally:
        conn.close()
    return {r[0]: {'name': r[1] or '', 'industry': r[2] or ''} for r in rows}
