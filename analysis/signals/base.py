"""Base class for all signals."""

from abc import ABC, abstractmethod
import torch


class Signal(ABC):
    name: str = "base"
    description: str = ""
    required_features: list[str] = []

    @abstractmethod
    def compute(self, features: dict[str, torch.Tensor], mask: torch.Tensor) -> torch.Tensor:
        """Compute signal.

        Returns (N_sym, N_dates) float32; higher = more bullish.
        No lookahead: signal[t] may only use data from indices <= t.
        """

    def validate_no_lookahead(
        self,
        features: dict[str, torch.Tensor],
        mask: torch.Tensor,
        forward_returns: torch.Tensor,
        lag: int = 1,
    ) -> float:
        """Sanity check: correlation of signal[t] with future data at [t+lag].

        A signal with lookahead bias would show near-perfect correlation.
        Returns Pearson r; should be small (|r| < 0.3) for clean signals.
        """
        sig = self.compute(features, mask)  # (N, T)
        N, T = sig.shape
        if T <= lag:
            return float('nan')

        s = sig[:, :-lag][mask[:, :-lag] & mask[:, lag:]]
        f = forward_returns[:, lag:][mask[:, :-lag] & mask[:, lag:]]
        s = s[~torch.isnan(s) & ~torch.isnan(f)]
        f = f[~torch.isnan(s) & ~torch.isnan(f)]
        if s.numel() < 10:
            return float('nan')

        s = s - s.mean(); f = f - f.mean()
        denom = s.norm() * f.norm()
        return ((s * f).sum() / denom.clamp(min=1e-10)).item()
