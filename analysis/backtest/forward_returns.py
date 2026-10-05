# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""Pure forward-return computation, lifted out of BacktestEngine so other
modules (e.g. analysis.announcement_correlation) can reuse the exact same
vectorized math without instantiating a full BacktestEngine."""

import torch


def forward_returns(close: torch.Tensor, mask: torch.Tensor, horizon: int) -> torch.Tensor:
    """Compute horizon-day forward returns from a (N_sym, N_dates) close tensor.

    Returns a same-shape tensor; NaN wherever either the base or forward-horizon
    day is invalid per `mask`, or the horizon shift runs past the last column.
    """
    N, T = close.shape
    fwd = torch.full_like(close, float('nan'))
    if T > horizon:
        fwd_close = close[:, horizon:]
        base_close = close[:, :T - horizon]
        valid = mask[:, :T - horizon] & mask[:, horizon:]
        fwd[:, :T - horizon] = torch.where(
            valid,
            (fwd_close - base_close) / base_close.clamp(min=1e-8),
            torch.tensor(float('nan'), device=close.device)
        )
    return fwd
