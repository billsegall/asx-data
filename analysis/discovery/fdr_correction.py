"""Benjamini-Hochberg FDR correction wrapper."""

import numpy as np


def fdr_correct(p_values: np.ndarray, alpha: float = 0.05) -> tuple[np.ndarray, np.ndarray]:
    """Benjamini-Hochberg FDR correction.

    Args:
        p_values: array of p-values
        alpha: FDR threshold

    Returns:
        (reject, corrected_p_values) — reject[i]=True means significant after FDR
    """
    n = len(p_values)
    if n == 0:
        return np.array([], dtype=bool), np.array([])

    order = np.argsort(p_values)
    ranks = np.empty(n, dtype=int)
    ranks[order] = np.arange(1, n + 1)

    # BH corrected p-values
    corrected = np.minimum(1.0, p_values * n / ranks)
    # Ensure monotonicity (from right)
    for i in range(n - 2, -1, -1):
        corrected[order[i]] = min(corrected[order[i]], corrected[order[i + 1]])

    reject = corrected <= alpha
    return reject, corrected
