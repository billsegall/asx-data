"""VolumeAnomalySignal: volume z-score × sign(5d return) → signed directional signal."""

import torch
from .base import Signal
from ..core.gpu_ops import rolling_zscore_fast, cross_sectional_rank


class VolumeAnomalySignal(Signal):
    name = "volume_anomaly"
    description = "Unusual volume spike in direction of recent price move"
    required_features = ['volume', 'returns']

    def __init__(self, vol_window: int = 20, ret_window: int = 5):
        self.vol_window = vol_window
        self.ret_window = ret_window

    def compute(self, features: dict[str, torch.Tensor], mask: torch.Tensor) -> torch.Tensor:
        volume = features['volume'].float()
        returns = features['returns']
        device = volume.device
        N, T = volume.shape

        # Log-transform volume to reduce skew
        log_vol = torch.log1p(volume)
        log_vol[~mask] = float('nan')

        vol_z = rolling_zscore_fast(log_vol, self.vol_window)

        # Rolling 5-day return sign
        ret_filled = returns.clone()
        ret_filled[torch.isnan(ret_filled)] = 0.0
        roll_ret = torch.zeros_like(returns)
        for lag in range(self.ret_window):
            if lag < T:
                roll_ret[:, lag:] += ret_filled[:, :T - lag]
        roll_ret[:, :self.ret_window] = float('nan')

        ret_sign = torch.sign(roll_ret)  # -1, 0, +1

        # Signal = vol_z × direction
        signal = vol_z * ret_sign
        signal[~mask] = float('nan')

        valid_mask = mask & ~torch.isnan(signal)
        return cross_sectional_rank(signal, valid_mask)
