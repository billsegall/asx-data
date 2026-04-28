# Copyright (c) 2019-2026, Bill Segall
# All rights reserved. See LICENSE for details.
"""PCA factor extraction from returns covariance matrix via GPU SVD."""

import numpy as np
import pandas as pd
import torch

from ..core.gpu_ops import compute_pca


class PCAFactors:
    """Extract latent factors from returns via SVD.

    Args:
        returns: (N_sym, N_dates) float32 tensor on CUDA
        mask: (N_sym, N_dates) bool tensor
        symbols_df: DataFrame with columns [symbol, industry] for interpretability
        n_components: number of PCA components to extract
    """

    def __init__(
        self,
        returns: torch.Tensor,
        mask: torch.Tensor,
        symbols_df: pd.DataFrame | None = None,
        n_components: int = 20,
    ):
        self.returns = returns
        self.mask = mask
        self.symbols_df = symbols_df
        self.n_components = n_components

        self._components: torch.Tensor | None = None
        self._explained_var: np.ndarray | None = None
        self._scores: torch.Tensor | None = None

    def fit(self):
        """Run SVD and extract factors."""
        print(f"[PCAFactors] Running SVD for {self.n_components} components...")
        self._components, self._explained_var, self._scores = compute_pca(
            self.returns, self.n_components
        )
        cumvar = self._explained_var.cumsum()
        print(f"[PCAFactors] Top-1: {self._explained_var[0]*100:.1f}%  "
              f"Top-5: {cumvar[4]*100:.1f}%  Top-{self.n_components}: {cumvar[-1]*100:.1f}%")

    @property
    def components(self) -> torch.Tensor:
        if self._components is None:
            self.fit()
        return self._components

    @property
    def explained_var(self) -> np.ndarray:
        if self._explained_var is None:
            self.fit()
        return self._explained_var

    @property
    def scores(self) -> torch.Tensor:
        """(N_sym, n_components) factor loadings per symbol."""
        if self._scores is None:
            self.fit()
        return self._scores

    def factor_tensors(self) -> dict[str, torch.Tensor]:
        """Return dict of factor_N → (1, N_dates) tensors for use in IC sweep."""
        comps = self.components  # (K, T)
        return {f'pca_factor_{i}': comps[i:i+1].expand(self.returns.shape[0], -1)
                for i in range(comps.shape[0])}

    def top_industry_loadings(self, factor_idx: int = 0, top_n: int = 5) -> pd.DataFrame:
        """Which industries load most heavily on a given factor?"""
        if self.symbols_df is None:
            return pd.DataFrame()
        scores = self.scores[:, factor_idx].cpu().numpy()
        df = pd.DataFrame({'loading': scores})
        if 'industry' in self.symbols_df.columns:
            df['industry'] = self.symbols_df['industry'].values[:len(scores)]
            return df.groupby('industry')['loading'].mean().abs().sort_values(ascending=False).head(top_n).reset_index()
        return df
