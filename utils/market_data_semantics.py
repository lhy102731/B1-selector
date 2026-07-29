"""Pure semantic checks for daily A-share market-data rows.

The checks in this module never write or repair source data.  They provide a
deterministic seam for deciding whether a byte-readable dataset is also safe to
use for research.  Cross-sectional aggregation is intentionally separate from
row checks because a single return mismatch can be a legitimate corporate
action, while a same-day market-wide spike indicates a broken data boundary.
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Iterable

import pandas as pd


DEFAULT_RETURN_ERROR_PP = 0.25
DEFAULT_AMPLITUDE_ERROR_PP = 0.25
DEFAULT_CROSS_SECTIONAL_SPIKE_RATIO = 0.20
DEFAULT_MIN_ELIGIBLE = 100


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(float("nan"), index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _finite_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def audit_frame(
    frame: pd.DataFrame,
    *,
    code: str,
    return_error_pp: float = DEFAULT_RETURN_ERROR_PP,
    amplitude_error_pp: float = DEFAULT_AMPLITUDE_ERROR_PP,
) -> list[dict[str, Any]]:
    """Return per-date semantic checks for one stock frame.

    ``change_pct`` is compared with the return implied by adjacent adjusted
    closes.  Provider amplitude follows the Eastmoney f58 convention:
    ``(high - low) / previous_close * 100``.  Missing optional fields are marked
    ineligible rather than silently treated as valid zeroes.
    """

    if "date" not in frame.columns or "close" not in frame.columns:
        return []

    work = frame.copy()
    work["_date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["_date"]).sort_values("_date").reset_index(drop=True)
    if work.empty:
        return []

    close = _numeric(work, "close")
    high = _numeric(work, "high")
    low = _numeric(work, "low")
    stored_return = _numeric(work, "change_pct")
    stored_amplitude = _numeric(work, "amplitude")
    previous_close = close.shift(1)

    calculated_return = (close / previous_close - 1.0) * 100.0
    calculated_amplitude = (high - low) / previous_close * 100.0

    rows: list[dict[str, Any]] = []
    normalized_code = str(code).zfill(6)
    for index in range(len(work)):
        prev = _finite_or_none(previous_close.iat[index])
        stored_ret = _finite_or_none(stored_return.iat[index])
        calculated_ret = _finite_or_none(calculated_return.iat[index])
        stored_amp = _finite_or_none(stored_amplitude.iat[index])
        calculated_amp = _finite_or_none(calculated_amplitude.iat[index])

        return_eligible = (
            prev is not None
            and prev > 0
            and stored_ret is not None
            and calculated_ret is not None
        )
        amplitude_eligible = (
            prev is not None
            and prev > 0
            and stored_amp is not None
            and calculated_amp is not None
        )
        return_error = (
            abs(calculated_ret - stored_ret) if return_eligible else None
        )
        amplitude_error = (
            abs(calculated_amp - stored_amp) if amplitude_eligible else None
        )

        rows.append(
            {
                "code": normalized_code,
                "date": work.at[index, "_date"].strftime("%Y-%m-%d"),
                "return_eligible": return_eligible,
                "stored_change_pct": stored_ret,
                "calculated_change_pct": calculated_ret,
                "return_error_pp": return_error,
                "return_bad": bool(
                    return_eligible
                    and return_error is not None
                    and return_error > float(return_error_pp)
                ),
                "amplitude_eligible": amplitude_eligible,
                "stored_amplitude_pct": stored_amp,
                "calculated_amplitude_pct": calculated_amp,
                "amplitude_error_pp": amplitude_error,
                "amplitude_bad": bool(
                    amplitude_eligible
                    and amplitude_error is not None
                    and amplitude_error > float(amplitude_error_pp)
                ),
            }
        )
    return rows


def summarize_checks(
    checks: Iterable[dict[str, Any]],
    *,
    cross_sectional_spike_ratio: float = DEFAULT_CROSS_SECTIONAL_SPIKE_RATIO,
    min_eligible: int = DEFAULT_MIN_ELIGIBLE,
) -> dict[str, Any]:
    """Aggregate row checks by date and assign a fail-closed health status."""

    by_date: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "return_eligible": 0,
            "return_bad": 0,
            "amplitude_eligible": 0,
            "amplitude_bad": 0,
        }
    )
    total_rows = 0
    for row in checks:
        date = str(row.get("date") or "")
        if not date:
            continue
        total_rows += 1
        bucket = by_date[date]
        for prefix in ("return", "amplitude"):
            if bool(row.get(f"{prefix}_eligible")):
                bucket[f"{prefix}_eligible"] += 1
                if bool(row.get(f"{prefix}_bad")):
                    bucket[f"{prefix}_bad"] += 1

    dates: list[dict[str, Any]] = []
    quarantined = False
    for date in sorted(by_date):
        bucket = by_date[date]
        return_eligible = int(bucket["return_eligible"])
        amplitude_eligible = int(bucket["amplitude_eligible"])
        return_bad = int(bucket["return_bad"])
        amplitude_bad = int(bucket["amplitude_bad"])
        return_ratio = return_bad / return_eligible if return_eligible else 0.0
        amplitude_ratio = (
            amplitude_bad / amplitude_eligible if amplitude_eligible else 0.0
        )
        flags: list[str] = []
        if (
            return_eligible >= int(min_eligible)
            and return_ratio >= float(cross_sectional_spike_ratio)
        ):
            flags.append("RETURN_CROSS_SECTIONAL_SPIKE")
        if (
            amplitude_eligible >= int(min_eligible)
            and amplitude_ratio >= float(cross_sectional_spike_ratio)
        ):
            flags.append("AMPLITUDE_CROSS_SECTIONAL_SPIKE")
        if flags:
            quarantined = True
        dates.append(
            {
                "date": date,
                "return_eligible": return_eligible,
                "return_bad": return_bad,
                "return_bad_ratio": return_ratio,
                "amplitude_eligible": amplitude_eligible,
                "amplitude_bad": amplitude_bad,
                "amplitude_bad_ratio": amplitude_ratio,
                "flags": flags,
            }
        )

    return {
        "schema_version": 1,
        "status": "SEMANTIC_QUARANTINE" if quarantined else "NO_MARKET_WIDE_SPIKE",
        "total_rows_checked": total_rows,
        "cross_sectional_spike_ratio": float(cross_sectional_spike_ratio),
        "min_eligible": int(min_eligible),
        "dates": dates,
    }


__all__ = [
    "DEFAULT_AMPLITUDE_ERROR_PP",
    "DEFAULT_CROSS_SECTIONAL_SPIKE_RATIO",
    "DEFAULT_MIN_ELIGIBLE",
    "DEFAULT_RETURN_ERROR_PP",
    "audit_frame",
    "summarize_checks",
]
