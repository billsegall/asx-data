"""Vectorised lead-lag cross-correlation (CCF) using PyTorch.

All tensors are (N_sym, N_dates) float32. NaN values must be filled with 0
before calling these functions (liquidity filter ensures ≥90% coverage).
"""

import numpy as np
import torch
from scipy import stats


def compute_ccf_gpu(returns: torch.Tensor, max_lag: int = 20) -> torch.Tensor:
    """Compute Pearson cross-correlations for all (leader, follower, lag) triplets.

    Args:
        returns: (N_sym, N_dates) float32, NaN pre-filled with 0
        max_lag: maximum lag in days (lags 1..max_lag)

    Returns:
        (N_sym, N_sym, max_lag) float32 — r[i, j, k] = Pearson(sym_i[t], sym_j[t+k+1])
        Lag k=0 → 1 day, k=max_lag-1 → max_lag days.
    """
    N, T = returns.shape
    r_all = torch.zeros(N, N, max_lag, dtype=torch.float32, device=returns.device)

    for k in range(1, max_lag + 1):
        n = T - k
        if n < 30:
            break

        A = returns[:, :n]   # leader: times 0..T-k-1
        B = returns[:, k:]   # follower: times k..T-1

        # Z-score along time axis (each symbol independently)
        A_mean = A.mean(dim=1, keepdim=True)
        A_std  = A.std(dim=1, keepdim=True, unbiased=True)
        B_mean = B.mean(dim=1, keepdim=True)
        B_std  = B.std(dim=1, keepdim=True, unbiased=True)

        A_z = (A - A_mean) / A_std.clamp(min=1e-8)
        B_z = (B - B_mean) / B_std.clamp(min=1e-8)

        # Pearson r[i,j] = (1/(n-1)) * A_z[i,:] · B_z[j,:]
        r_k = (A_z @ B_z.T) / (n - 1)   # (N, N)
        r_all[:, :, k - 1] = r_k.clamp(-1.0, 1.0)

    return r_all


def ccf_pvalues(r: np.ndarray, max_lag: int, n_dates: int) -> np.ndarray:
    """Two-tailed p-values for Pearson r via t-distribution.

    Args:
        r: (N, N, max_lag) array of Pearson correlations
        max_lag: number of lags
        n_dates: total number of dates in the sample

    Returns:
        p: (N, N, max_lag) array of p-values
    """
    p = np.ones_like(r, dtype=np.float64)
    for k in range(max_lag):
        lag = k + 1
        n = n_dates - lag
        df = n - 2
        if df < 2:
            continue
        r_k = np.clip(r[:, :, k], -1 + 1e-8, 1 - 1e-8).astype(np.float64)
        t_stat = r_k * np.sqrt(df) / np.sqrt(1.0 - r_k ** 2)
        p[:, :, k] = 2.0 * stats.t.sf(np.abs(t_stat), df=df)
    return p
