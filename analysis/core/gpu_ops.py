"""GPU-accelerated operations using pure PyTorch.

All tensors: (N_sym, N_dates) float32 on CUDA.
mask: (N_sym, N_dates) bool — True = valid data point.
"""

import torch
import torch.nn.functional as F


def rolling_zscore(x: torch.Tensor, window: int) -> torch.Tensor:
    """Rolling z-score over the time axis (dim=1). Pads left with NaN."""
    N, T = x.shape
    out = torch.full_like(x, float('nan'))
    for t in range(window - 1, T):
        chunk = x[:, t - window + 1:t + 1]
        mu = chunk.mean(dim=1)
        std = chunk.std(dim=1, unbiased=True)
        valid = std > 1e-8
        out[:, t] = torch.where(valid, (x[:, t] - mu) / std, torch.tensor(float('nan'), device=x.device))
    return out


def rolling_zscore_fast(x: torch.Tensor, window: int) -> torch.Tensor:
    """Vectorized rolling z-score using conv1d. Much faster than loop version."""
    N, T = x.shape
    # Handle NaN by treating them as 0 for sum, but tracking count
    nan_mask = torch.isnan(x)
    x_fill = x.clone()
    x_fill[nan_mask] = 0.0

    weight = torch.ones(1, 1, window, device=x.device, dtype=x.dtype) / window

    # Compute rolling mean
    x3 = x_fill.unsqueeze(1)  # (N, 1, T)
    roll_mean = F.conv1d(x3, weight, padding=window - 1)[:, 0, :T]  # (N, T)

    # Compute rolling variance
    x_sq = x_fill ** 2
    x_sq3 = x_sq.unsqueeze(1)
    roll_mean_sq = F.conv1d(x_sq3, weight, padding=window - 1)[:, 0, :T]
    roll_var = (roll_mean_sq - roll_mean ** 2).clamp(min=0)
    roll_std = roll_var.sqrt()

    # Apply: first window-1 positions are invalid
    out = (x_fill - roll_mean) / roll_std.clamp(min=1e-8)
    out[:, :window - 1] = float('nan')
    # Propagate NaN mask
    out[nan_mask] = float('nan')
    return out


def rolling_slope(x: torch.Tensor, window: int) -> torch.Tensor:
    """Rolling linear slope over time axis via batch matmul.

    Returns slope coefficients (N_sym, N_dates); first window-1 cols = NaN.
    """
    N, T = x.shape
    device = x.device
    dtype = x.dtype

    # Precompute t values normalized to [-1, 1] for numerical stability
    t = torch.linspace(-1, 1, window, device=device, dtype=dtype)
    t_mean = t.mean()
    t_var = ((t - t_mean) ** 2).sum()

    out = torch.full_like(x, float('nan'))
    for start in range(T - window + 1):
        chunk = x[:, start:start + window]  # (N, window)
        nan_cols = torch.isnan(chunk).any(dim=1)
        chunk_c = chunk - chunk.mean(dim=1, keepdim=True)
        slopes = (chunk_c * (t - t_mean)).sum(dim=1) / t_var
        slopes[nan_cols] = float('nan')
        out[:, start + window - 1] = slopes

    return out


def cross_sectional_rank(x: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    """Normalize each column to [0,1] rank cross-sectionally.

    Args:
        x: (N_sym, N_dates)
        mask: (N_sym, N_dates) bool
    Returns:
        (N_sym, N_dates) float32; NaN where mask=False.
    """
    N, T = x.shape
    out = torch.full_like(x, float('nan'))
    for t in range(T):
        col = x[:, t]
        m = mask[:, t]
        valid_vals = col[m]
        if valid_vals.numel() < 2:
            continue
        # Argsort twice = rank
        order = valid_vals.argsort()
        ranks = torch.empty_like(order)
        ranks[order] = torch.arange(order.numel(), device=x.device)
        normalized = ranks.float() / (order.numel() - 1)
        out[m, t] = normalized
    return out


def _spearman_ic(feature_col: torch.Tensor, fwd_col: torch.Tensor, mask_col: torch.Tensor) -> float:
    """Spearman rank correlation between feature and forward_return at one date."""
    m = mask_col & ~torch.isnan(feature_col) & ~torch.isnan(fwd_col)
    vals = feature_col[m]
    fwd = fwd_col[m]
    if vals.numel() < 10:
        return float('nan')

    def _rank(v):
        order = v.argsort()
        r = torch.empty_like(order, dtype=torch.float32)
        r[order] = torch.arange(order.numel(), device=v.device, dtype=torch.float32)
        return r

    r1 = _rank(vals)
    r2 = _rank(fwd)
    r1 -= r1.mean(); r2 -= r2.mean()
    denom = (r1.norm() * r2.norm())
    if denom < 1e-10:
        return float('nan')
    return (r1 * r2).sum().item() / denom.item()


def compute_ic_series(
    feature: torch.Tensor,
    forward_returns: torch.Tensor,
    lag: int,
    mask: torch.Tensor,
) -> torch.Tensor:
    """Compute Spearman IC at each date — fully vectorized on GPU.

    Ranks all dates simultaneously via argsort on dim=0, then computes
    Pearson correlation of ranks (= Spearman) via batch dot products.
    No Python loop over dates; processes the full (N_sym × T) matrix at once.

    feature[sym, t] compared to forward_returns[sym, t+lag].
    Returns (N_dates,) tensor with NaN for dates with < 10 valid symbols.
    """
    N, T = feature.shape
    T2 = T - lag

    # Align: feature at t vs forward_return at t+lag
    feat = feature[:, :T2]
    fwd  = forward_returns[:, lag:lag + T2]
    m    = mask[:, :T2] & mask[:, lag:lag + T2] & ~torch.isnan(feat) & ~torch.isnan(fwd)

    # Fill invalid entries with -inf so they sort to the bottom
    SENTINEL = torch.tensor(float('-inf'), device=feature.device, dtype=feature.dtype)
    feat_fill = torch.where(m, feat, SENTINEL)
    fwd_fill  = torch.where(m, fwd,  SENTINEL)

    # Rank along N (symbols) for every date simultaneously: (N, T2)
    def _batch_rank(x):
        order = x.argsort(dim=0)          # ascending; -inf goes first
        ranks = torch.empty_like(order, dtype=torch.float32)
        rows  = torch.arange(N, device=x.device).unsqueeze(1).expand_as(order)
        ranks.scatter_(0, order, rows.float())
        return ranks

    rank_feat = _batch_rank(feat_fill)    # (N, T2)
    rank_fwd  = _batch_rank(fwd_fill)     # (N, T2)

    # Zero out invalid positions
    rank_feat = torch.where(m, rank_feat, torch.zeros_like(rank_feat))
    rank_fwd  = torch.where(m, rank_fwd,  torch.zeros_like(rank_fwd))

    n_valid = m.sum(dim=0).float()        # (T2,)

    # Centre ranks per date
    mu_feat = rank_feat.sum(dim=0) / n_valid.clamp(min=1)
    mu_fwd  = rank_fwd.sum(dim=0)  / n_valid.clamp(min=1)
    feat_c  = torch.where(m, rank_feat - mu_feat.unsqueeze(0), torch.zeros_like(rank_feat))
    fwd_c   = torch.where(m, rank_fwd  - mu_fwd.unsqueeze(0),  torch.zeros_like(rank_fwd))

    # Pearson on ranks = Spearman
    num = (feat_c * fwd_c).sum(dim=0)
    den = feat_c.norm(dim=0) * fwd_c.norm(dim=0)
    ic_vals = torch.where(den > 1e-8, num / den, torch.tensor(float('nan'), device=feature.device))
    ic_vals = torch.where(n_valid >= 10, ic_vals, torch.tensor(float('nan'), device=feature.device))

    # Pad back to length T
    result = torch.full((T,), float('nan'), device=feature.device)
    result[:T2] = ic_vals
    return result


def compute_ic_stats(ic_series: torch.Tensor) -> dict:
    """Summary stats from IC time series."""
    valid = ic_series[~torch.isnan(ic_series)]
    if valid.numel() < 5:
        return dict(mean_ic=float('nan'), std_ic=float('nan'), ic_ir=float('nan'),
                    t_stat=float('nan'), p_value=1.0, n=0)
    mean_ic = valid.mean().item()
    std_ic = valid.std(unbiased=True).item()
    n = valid.numel()
    ic_ir = mean_ic / (std_ic + 1e-10)
    t_stat = mean_ic / (std_ic / (n ** 0.5) + 1e-10)

    # Two-tailed p-value from t-distribution approximation
    import math
    # Using normal approximation for large n
    from scipy import stats as scipy_stats
    p_value = 2 * (1 - scipy_stats.t.cdf(abs(t_stat), df=n - 1))

    return dict(mean_ic=mean_ic, std_ic=std_ic, ic_ir=ic_ir, t_stat=t_stat, p_value=p_value, n=n)


def compute_pca(returns: torch.Tensor, n_components: int = 20) -> tuple:
    """SVD-based PCA on returns matrix (N_sym, N_dates).

    Returns (components, explained_var_ratio, scores):
        components: (n_components, N_dates)
        explained_var_ratio: (n_components,) numpy array
        scores: (N_sym, n_components) factor loadings per symbol
    """
    # Fill NaN with 0 (mean-impute after centering)
    r = returns.clone()
    nan_mask = torch.isnan(r)
    r[nan_mask] = 0.0

    # Center per symbol
    mu = r.mean(dim=1, keepdim=True)
    r = r - mu

    # SVD
    U, S, Vh = torch.linalg.svd(r, full_matrices=False)
    # U: (N_sym, K), S: (K,), Vh: (K, N_dates)
    K = min(n_components, S.shape[0])
    components = Vh[:K]                        # (K, N_dates)
    explained_var = (S[:K] ** 2) / (S ** 2).sum()
    scores = U[:, :K] * S[:K].unsqueeze(0)    # (N_sym, K)

    return components, explained_var.cpu().numpy(), scores


def gpu_monitor() -> dict:
    """Current VRAM usage. Uses torch.cuda (works in WSL2, no nvidia-smi needed)."""
    if not torch.cuda.is_available():
        return dict(vram_used_gb=0, vram_total_gb=0, vram_free_gb=0)
    props = torch.cuda.get_device_properties(0)
    total = props.total_memory
    used = torch.cuda.memory_allocated(0)
    reserved = torch.cuda.memory_reserved(0)
    return dict(
        vram_used_gb=round(used / 1e9, 3),
        vram_reserved_gb=round(reserved / 1e9, 3),
        vram_total_gb=round(total / 1e9, 3),
        vram_free_gb=round((total - reserved) / 1e9, 3),
    )
