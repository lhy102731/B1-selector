"""Backfill genuine historical bars from the Eastmoney desktop TCP cache.

The desktop protocol supplies unadjusted OHLCV and amount.  This repair maps
those raw prices into the committed THS backward-adjusted price space with
three fail-closed methods, in priority order:

1. a local affine transform fitted on untouched ``data_ths`` rows in the same
   legacy adjustment regime;
2. a leave-one-out affine transform fitted on independently queried THS
   adjusted closes; or
3. an affine corporate-action chain validated against the full untouched THS
   history.

Legacy rows identify missing dates, cross-check trade units, provide the
adjustment-regime fingerprint, and supply point-in-time valuation fields.  No
legacy OHLC value is written.  Eastmoney is preferred; a complete THS raw bar
is used when Eastmoney fails the unchanged source-consistency gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_missing_bars_ths import invalidate_caches
from tools.backfill_valuation_fields import (
    VALUATION_COLUMNS,
    _atomic_csv,
    _atomic_json,
    _date_keys,
    _read_csv,
    _update_manifest,
    code_path,
    valid_valuation,
)
from utils.ths_data_source import THSDataSource


PRICE_FIELDS = ("open", "high", "low", "close")
# Explicit decisions accepted from the user's 2026-07-28 manual review.  They
# stay enumerated so no unreviewed row can enter a relaxed path accidentally.
APPROVED_LOCAL_SEGMENT_TARGETS: dict[tuple[str, str], float | None] = {
    ("000004", "1991-02-01"): None,
    ("000004", "1991-04-01"): None,
    ("000004", "1992-05-05"): None,
    ("600608", "1996-04-01"): None,
    ("600621", "1996-04-01"): 221.43,
}
APPROVED_FULL_BAR_OVERRIDES: dict[tuple[str, str], dict[str, float]] = {
    ("000004", "1991-02-01"): {
        "open": 14.80,
        "high": 15.20,
        "low": 14.60,
        "close": 15.00,
        "volume": 1_264_000.0,
        "amount": 18_762_000.0,
        "turnover": 25.28,
        "market_cap": 75_000_000.0,
    },
    ("000004", "1991-04-01"): {
        "open": 17.30,
        "high": 17.95,
        "low": 17.10,
        "close": 17.80,
        "volume": 2_087_500.0,
        "amount": 36_416_000.0,
        "turnover": 41.75,
        "market_cap": 89_000_000.0,
    },
}
APPROVED_LEGACY_CROSSCHECK_BYPASS = {
    ("000004", "1991-02-01"),
    ("000004", "1991-04-01"),
    ("000004", "1992-05-05"),
}
APPROVED_FORCE_THS_KEYS = {("000002", "1992-04-20")}
APPROVED_DATE_RELABELS = {("000002", "1992-04-20"): "1992-04-18"}
APPROVED_NON_TRADING_DAYS = {("002500", "2020-06-17")}
APPROVED_FIXED_TRANSFORMS = {
    ("600665", "1993-07-09"): (1.0, 0.0, "manual_identity"),
}
UNSUPPORTED_ACTION_MARKERS = ("拆成", "缩股", "合并", "换股")


@dataclass(frozen=True)
class AffineTransform:
    slope: float
    intercept: float
    method: str
    diagnostics: dict[str, Any]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def relative_gap(left: float, right: float) -> float:
    denominator = max(abs(float(left)), abs(float(right)), 1e-12)
    return abs(float(left) - float(right)) / denominator


def numeric(value: Any) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed) or not math.isfinite(float(parsed)):
        return None
    return float(parsed)


def normalise_date(value: Any) -> str | None:
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else pd.Timestamp(parsed).strftime("%Y-%m-%d")


def strict_trade_source_gate(
    row: pd.Series,
    ths_row: pd.Series | None,
    *,
    trade_relative_tolerance: float = 0.01,
    price_relative_tolerance: float = 0.001,
    price_absolute_tolerance: float = 0.01,
    require_legacy_trade_crosscheck: bool = True,
) -> tuple[bool, str, dict[str, Any]]:
    values: dict[str, float] = {}
    for field in PRICE_FIELDS:
        value = numeric(row.get(f"em_{field}_raw"))
        if value is None or value <= 0:
            return False, f"missing_em_{field}", {}
        values[field] = value
    volume = numeric(row.get("em_volume"))
    amount = numeric(row.get("em_amount"))
    if volume is None or volume <= 0:
        return False, "missing_em_volume", {}
    if amount is None or amount <= 0:
        return False, "missing_em_amount", {}
    if values["low"] > values["high"]:
        return False, "em_low_above_high", {}
    tolerance = max(price_absolute_tolerance, price_relative_tolerance * values["high"])
    if min(values["open"], values["close"]) < values["low"] - tolerance:
        return False, "em_body_below_low", {}
    if max(values["open"], values["close"]) > values["high"] + tolerance:
        return False, "em_body_above_high", {}
    vwap = amount / volume
    if not (
        vwap >= values["low"] * (1.0 - price_relative_tolerance) - price_absolute_tolerance
        and vwap <= values["high"] * (1.0 + price_relative_tolerance) + price_absolute_tolerance
    ):
        return False, "em_vwap_outside_ohlc", {"vwap": vwap}

    legacy_volume = numeric(row.get("legacy_volume"))
    legacy_amount = numeric(row.get("legacy_amount"))
    if require_legacy_trade_crosscheck and (legacy_volume is None or legacy_volume <= 0):
        return False, "missing_legacy_volume_crosscheck", {"vwap": vwap}
    if require_legacy_trade_crosscheck and (legacy_amount is None or legacy_amount <= 0):
        return False, "missing_legacy_amount_crosscheck", {"vwap": vwap}
    volume_gap = (
        relative_gap(volume, legacy_volume)
        if legacy_volume is not None and legacy_volume > 0
        else None
    )
    amount_gap = (
        relative_gap(amount, legacy_amount)
        if legacy_amount is not None and legacy_amount > 0
        else None
    )
    if (
        require_legacy_trade_crosscheck
        and volume_gap is not None
        and volume_gap > trade_relative_tolerance
    ):
        return False, "em_legacy_volume_conflict", {
            "volume_gap": volume_gap,
            "amount_gap": amount_gap,
        }
    if (
        require_legacy_trade_crosscheck
        and amount_gap is not None
        and amount_gap > trade_relative_tolerance
    ):
        return False, "em_legacy_amount_conflict", {
            "volume_gap": volume_gap,
            "amount_gap": amount_gap,
        }

    ths_status = str(row.get("ths_raw_status") or "")
    ths_max_gap = 0.0
    if ths_status == "complete":
        if ths_row is None:
            return False, "missing_ths_complete_crosscheck", {}
        for field in PRICE_FIELDS:
            ths_value = numeric(ths_row.get(f"{field}_raw"))
            if ths_value is None or ths_value <= 0:
                return False, "incomplete_ths_price_crosscheck", {}
            difference = abs(values[field] - ths_value)
            allowed = max(price_absolute_tolerance, price_relative_tolerance * abs(ths_value))
            ths_max_gap = max(ths_max_gap, relative_gap(values[field], ths_value))
            if difference > allowed + 1e-7:
                return False, "em_ths_price_conflict", {
                    "field": field,
                    "difference": difference,
                    "allowed": allowed,
                }
    return True, "trade_source_passed", {
        "vwap": vwap,
        "volume_gap": volume_gap,
        "amount_gap": amount_gap,
        "ths_max_price_gap": ths_max_gap,
    }


def validated_trade_source(
    row: pd.Series,
    ths_row: pd.Series | None,
    *,
    require_legacy_trade_crosscheck: bool = True,
    prefer_ths: bool = False,
) -> tuple[pd.Series, str, bool, str, dict[str, Any]]:
    """Prefer Eastmoney when it passes; otherwise retry with one complete THS bar."""
    passed, reason, diagnostics = strict_trade_source_gate(
        row,
        ths_row,
        require_legacy_trade_crosscheck=require_legacy_trade_crosscheck,
    )
    if passed and not prefer_ths:
        return row, "eastmoney_desktop", True, reason, diagnostics
    if ths_row is None or str(row.get("ths_raw_status") or "") != "complete":
        return row, "eastmoney_desktop", passed, reason, diagnostics

    replacement = row.copy()
    for field in PRICE_FIELDS:
        value = numeric(ths_row.get(f"{field}_raw"))
        if value is None or value <= 0:
            return row, "eastmoney_desktop", False, reason, diagnostics
        replacement[f"em_{field}_raw"] = value
    for source_field, target_field in (
        ("volume", "em_volume"),
        ("amount", "em_amount"),
    ):
        value = numeric(ths_row.get(source_field))
        if value is None or value <= 0:
            return row, "eastmoney_desktop", False, reason, diagnostics
        replacement[target_field] = value

    fallback_passed, fallback_reason, fallback_diagnostics = strict_trade_source_gate(
        replacement,
        ths_row,
        require_legacy_trade_crosscheck=require_legacy_trade_crosscheck,
    )
    if fallback_passed:
        fallback_diagnostics = {
            **fallback_diagnostics,
            "eastmoney_rejection": reason,
        }
        return (
            replacement,
            "ths_wencai_fallback",
            True,
            fallback_reason,
            fallback_diagnostics,
        )
    return row, "eastmoney_desktop", False, reason, diagnostics


def nearby_trade_fingerprint(
    current: pd.DataFrame,
    *,
    target_date: str,
    close_raw: float,
    volume: float,
    amount: float,
    calendar_days: int = 7,
) -> str | None:
    if current.empty or not {"date", "close_raw", "volume", "amount"}.issubset(current.columns):
        return None
    dates = pd.to_datetime(current["date"], errors="coerce")
    target = pd.Timestamp(target_date)
    nearby = current.loc[(dates - target).abs().dt.days.le(calendar_days) & dates.ne(target)].copy()
    if nearby.empty:
        return None
    nearby["_date"] = _date_keys(nearby["date"])
    for field in ("close_raw", "volume", "amount"):
        nearby[field] = pd.to_numeric(nearby[field], errors="coerce")
    close_match = (nearby["close_raw"] - close_raw).abs().le(max(0.011, abs(close_raw) * 0.001))
    volume_match = (nearby["volume"] / volume - 1.0).abs().le(0.001)
    amount_match = (nearby["amount"] / amount - 1.0).abs().le(0.01)
    matches = nearby.loc[close_match & volume_match & amount_match, "_date"].dropna()
    return None if matches.empty else str(matches.iloc[0])


def fit_affine(
    frame: pd.DataFrame,
    *,
    x_column: str,
    y_column: str,
    min_points: int,
    min_variation: float,
    max_absolute_error: float,
    max_relative_error: float,
    quantization_floor: float = 0.0,
) -> tuple[tuple[float, float] | None, dict[str, Any]]:
    work = frame[[x_column, y_column]].apply(pd.to_numeric, errors="coerce").dropna()
    work = work[(work[x_column] > 0) & (work[y_column] > 0)].reset_index(drop=True)
    diagnostics: dict[str, Any] = {"anchor_points": int(len(work))}
    if len(work) < min_points:
        diagnostics["fit_status"] = "insufficient_affine_anchors"
        return None, diagnostics
    x = work[x_column].to_numpy(dtype=float)
    y = work[y_column].to_numpy(dtype=float)
    variation = (float(x.max()) - float(x.min())) / max(abs(float(np.median(x))), 1e-12)
    diagnostics["anchor_variation"] = variation
    if variation < min_variation:
        diagnostics["fit_status"] = "insufficient_affine_variation"
        return None, diagnostics

    train = np.arange(len(work)) % 2 == 0
    test = ~train
    if int(train.sum()) < 2 or int(test.sum()) < 2:
        diagnostics["fit_status"] = "insufficient_holdout_anchors"
        return None, diagnostics
    train_design = np.column_stack([x[train], np.ones(int(train.sum()))])
    train_slope, train_intercept = np.linalg.lstsq(train_design, y[train], rcond=None)[0]
    holdout_prediction = train_slope * x[test] + train_intercept
    holdout_absolute = np.abs(holdout_prediction - y[test])
    holdout_relative = np.abs(holdout_prediction / y[test] - 1.0)
    design = np.column_stack([x, np.ones(len(x))])
    slope, intercept = np.linalg.lstsq(design, y, rcond=None)[0]
    prediction = slope * x + intercept
    absolute = np.abs(prediction - y)
    relative = np.abs(prediction / y - 1.0)
    holdout_allowed = np.minimum(
        max_absolute_error,
        np.maximum(max_relative_error * np.abs(y[test]), quantization_floor),
    )
    fit_allowed = np.minimum(
        max_absolute_error,
        np.maximum(max_relative_error * np.abs(y), quantization_floor),
    )
    diagnostics.update(
        {
            "slope": float(slope),
            "intercept": float(intercept),
            "holdout_max_absolute_error": float(holdout_absolute.max()),
            "holdout_max_relative_error": float(holdout_relative.max()),
            "fit_max_absolute_error": float(absolute.max()),
            "fit_p99_absolute_error": float(np.quantile(absolute, 0.99)),
            "fit_max_relative_error": float(relative.max()),
            "quantization_floor": float(quantization_floor),
            "holdout_max_error_budget_ratio": float(
                np.max(holdout_absolute / np.maximum(holdout_allowed, 1e-12))
            ),
            "fit_max_error_budget_ratio": float(
                np.max(absolute / np.maximum(fit_allowed, 1e-12))
            ),
        }
    )
    if not math.isfinite(float(slope)) or float(slope) <= 0:
        diagnostics["fit_status"] = "invalid_affine_slope"
        return None, diagnostics
    if np.any(holdout_absolute > holdout_allowed + 1e-12) or np.any(
        absolute > fit_allowed + 1e-12
    ):
        diagnostics["fit_status"] = "unstable_affine"
        return None, diagnostics
    diagnostics["fit_status"] = "stable_affine"
    return (float(slope), float(intercept)), diagnostics


def trusted_anchor_pool(trusted: pd.DataFrame, legacy: pd.DataFrame) -> pd.DataFrame:
    """Build a per-code anchor table once instead of once per missing row."""
    if trusted.empty or legacy.empty:
        return pd.DataFrame()
    left = trusted.copy()
    right = legacy.copy()
    left["date"] = _date_keys(left["date"])
    right["date"] = _date_keys(right["date"])
    anchors = left[["date", "close", "close_raw"]].merge(
        right[["date", "close"]], on="date", suffixes=("_new", "_legacy")
    )
    for field in ("close_new", "close_raw", "close_legacy"):
        anchors[field] = pd.to_numeric(anchors[field], errors="coerce")
    anchors = anchors.dropna().query("close_new > 0 and close_raw > 0 and close_legacy > 0")
    anchors["legacy_factor"] = anchors["close_legacy"] / anchors["close_raw"]
    anchors["date_ts"] = pd.to_datetime(anchors["date"], errors="coerce")
    return anchors.dropna(subset=["date_ts"]).reset_index(drop=True)


def trusted_segment_transform_from_pool(
    anchors: pd.DataFrame,
    *,
    target_date: str,
    target_legacy_close: float,
    target_raw_close: float,
    factor_tolerance: float = 0.0001,
) -> AffineTransform | None:
    if anchors.empty:
        return None
    target_factor = target_legacy_close / target_raw_close
    selected = anchors[(anchors["legacy_factor"] / target_factor - 1.0).abs() <= factor_tolerance].copy()
    if selected.empty:
        return None
    selected["distance"] = (selected["date_ts"] - pd.Timestamp(target_date)).abs().dt.days
    selected = selected.nsmallest(60, "distance").sort_values("date").reset_index(drop=True)
    transform, diagnostics = fit_affine(
        selected,
        x_column="close_raw",
        y_column="close_new",
        min_points=5,
        min_variation=0.005,
        max_absolute_error=0.00151,
        max_relative_error=0.0005,
    )
    if transform is None:
        return None
    diagnostics["target_legacy_factor"] = target_factor
    return AffineTransform(*transform, "trusted_ths_segment", diagnostics)


def trusted_segment_transform(
    trusted: pd.DataFrame,
    legacy: pd.DataFrame,
    **kwargs: Any,
) -> AffineTransform | None:
    return trusted_segment_transform_from_pool(trusted_anchor_pool(trusted, legacy), **kwargs)


def bracketed_local_transform(
    trusted: pd.DataFrame,
    *,
    target_date: str,
    target_raw_close: float,
    target_adjusted_close: float | None = None,
    minimum_each_side: int = 5,
    maximum_each_side: int = 30,
) -> AffineTransform | None:
    """Prove one local adjustment segment with independent anchors on both sides."""
    if trusted.empty:
        return None
    frame = trusted[["date", "close_raw", "close"]].copy()
    frame["date_ts"] = pd.to_datetime(frame["date"], errors="coerce")
    frame[["close_raw", "close"]] = frame[["close_raw", "close"]].apply(
        pd.to_numeric, errors="coerce"
    )
    frame = frame.dropna().query("close_raw > 0 and close > 0")
    target = pd.Timestamp(target_date)
    before = frame.loc[frame["date_ts"].lt(target)].nlargest(
        maximum_each_side, "date_ts"
    )
    after = frame.loc[frame["date_ts"].gt(target)].nsmallest(
        maximum_each_side, "date_ts"
    )
    if len(before) < minimum_each_side or len(after) < minimum_each_side:
        return None
    selected = pd.concat([before, after], ignore_index=True).sort_values(
        "date_ts", kind="stable"
    )
    transform, diagnostics = fit_affine(
        selected,
        x_column="close_raw",
        y_column="close",
        min_points=minimum_each_side * 2,
        min_variation=0.005,
        max_absolute_error=0.00151,
        max_relative_error=0.0005,
        quantization_floor=0.00055,
    )
    if transform is None:
        return None
    slope, intercept = transform
    diagnostics.update(
        {
            "before_anchor_points": int(len(before)),
            "after_anchor_points": int(len(after)),
            "anchor_start": str(selected["date_ts"].min().date()),
            "anchor_end": str(selected["date_ts"].max().date()),
        }
    )
    if target_adjusted_close is not None:
        error = abs(slope * target_raw_close + intercept - target_adjusted_close)
        diagnostics["target_adjusted_close"] = target_adjusted_close
        diagnostics["target_adjusted_close_error"] = error
        diagnostics["target_adjusted_close_tolerance"] = 0.011
        if error > 0.011 + 1e-12:
            return None
    return AffineTransform(slope, intercept, "bracketed_ths_segment", diagnostics)


def adjusted_anchor_pool(
    pool: pd.DataFrame,
    adjusted_close: pd.DataFrame,
    legacy: pd.DataFrame,
    *,
    code: str,
) -> pd.DataFrame:
    """Build independent adjusted-close anchors once per code."""
    if pool.empty or adjusted_close.empty or legacy.empty:
        return pd.DataFrame()
    raw = pool.loc[pool["code"].eq(code), ["code", "date", "close"]].copy()
    adjusted = adjusted_close.loc[
        adjusted_close["code"].eq(code), ["code", "date", "close_adjusted"]
    ].copy()
    old = legacy[["date", "close"]].copy()
    raw["date"] = _date_keys(raw["date"])
    adjusted["date"] = _date_keys(adjusted["date"])
    old["date"] = _date_keys(old["date"])
    anchors = adjusted.merge(raw, on=["code", "date"]).merge(
        old, on="date", suffixes=("_raw", "_legacy")
    )
    for field in ("close_adjusted", "close_raw", "close_legacy"):
        anchors[field] = pd.to_numeric(anchors[field], errors="coerce")
    anchors = anchors.dropna().query("close_adjusted > 0 and close_raw > 0 and close_legacy > 0")
    anchors["legacy_factor"] = anchors["close_legacy"] / anchors["close_raw"]
    anchors["date_ts"] = pd.to_datetime(anchors["date"], errors="coerce")
    return anchors.dropna(subset=["date_ts"]).reset_index(drop=True)


def adjusted_close_transform_from_pool(
    anchors: pd.DataFrame,
    *,
    target_date: str,
    target_legacy_close: float,
    target_raw_close: float,
    target_validation_raw_close: float | None = None,
    target_adjusted_close: float | None = None,
    factor_tolerance: float = 0.0001,
) -> AffineTransform | None:
    if anchors.empty or target_adjusted_close is None:
        return None
    target_factor = target_legacy_close / target_raw_close
    selected = anchors[
        anchors["date"].ne(target_date)
        & ((anchors["legacy_factor"] / target_factor - 1.0).abs() <= factor_tolerance)
    ].copy()
    if selected.empty:
        return None
    selected["distance"] = (selected["date_ts"] - pd.Timestamp(target_date)).abs().dt.days
    selected = selected.nsmallest(80, "distance").sort_values("date").reset_index(drop=True)
    transform, diagnostics = fit_affine(
        selected,
        x_column="close_raw",
        y_column="close_adjusted",
        min_points=8,
        min_variation=0.005,
        max_absolute_error=0.011,
        max_relative_error=0.0025,
    )
    if transform is None:
        return None
    slope, intercept = transform
    validation_raw_close = (
        target_raw_close
        if target_validation_raw_close is None
        else target_validation_raw_close
    )
    crosscheck = abs(slope * validation_raw_close + intercept - target_adjusted_close)
    model_error = max(
        float(diagnostics.get("fit_max_absolute_error", 0.0) or 0.0),
        float(diagnostics.get("holdout_max_absolute_error", 0.0) or 0.0),
    )
    crosscheck_tolerance = (
        0.011
        if target_validation_raw_close is None
        else min(0.011, 0.0051 + model_error)
    )
    diagnostics.update(
        {
            "target_adjusted_close": target_adjusted_close,
            "target_adjusted_close_error": crosscheck,
            "target_adjusted_close_tolerance": crosscheck_tolerance,
            "target_validation_raw_close": validation_raw_close,
            "target_legacy_factor": target_factor,
        }
    )
    if crosscheck > crosscheck_tolerance + 1e-12:
        return None
    return AffineTransform(slope, intercept, "ths_adjusted_close_affine", diagnostics)


def adjusted_close_transform(
    pool: pd.DataFrame,
    adjusted_close: pd.DataFrame,
    legacy: pd.DataFrame,
    *,
    code: str,
    target_date: str,
    target_legacy_close: float,
    target_raw_close: float,
    target_validation_raw_close: float | None = None,
    factor_tolerance: float = 0.0001,
) -> AffineTransform | None:
    target = adjusted_close.loc[
        adjusted_close["code"].eq(code) & adjusted_close["date"].eq(target_date),
        "close_adjusted",
    ] if not adjusted_close.empty else pd.Series(dtype=float)
    return adjusted_close_transform_from_pool(
        adjusted_anchor_pool(pool, adjusted_close, legacy, code=code),
        target_date=target_date,
        target_legacy_close=target_legacy_close,
        target_raw_close=target_raw_close,
        target_validation_raw_close=target_validation_raw_close,
        target_adjusted_close=None if target.empty else numeric(target.iloc[0]),
        factor_tolerance=factor_tolerance,
    )


def action_parameters(row: pd.Series | Any) -> tuple[float, float]:
    get = row.get if isinstance(row, pd.Series) else lambda key, default=0.0: getattr(row, key, default)
    bonus = float(get("bonus_ratio", 0.0) or 0.0)
    rights = float(get("rights_ratio", 0.0) or 0.0)
    consideration_stock = float(get("consideration_stock_ratio", 0.0) or 0.0)
    cash = float(get("cash_per_share", 0.0) or 0.0)
    consideration_cash = float(get("consideration_cash_per_share", 0.0) or 0.0)
    rights_price = float(get("rights_price", 0.0) or 0.0)
    return 1.0 + bonus + rights + consideration_stock, cash + consideration_cash - rights * rights_price


def canonical_affine_for_date(actions: pd.DataFrame, day: str | pd.Timestamp) -> tuple[float, float]:
    slope, intercept = 1.0, 0.0
    target = pd.Timestamp(day)
    if actions is None or actions.empty:
        return slope, intercept
    events = actions.copy()
    events["date"] = pd.to_datetime(events["date"], errors="coerce")
    events = events.dropna(subset=["date"]).sort_values("date", kind="stable")
    for event in events.loc[events["date"].le(target)].itertuples(index=False):
        multiplier, additive = action_parameters(event)
        if not math.isfinite(multiplier) or multiplier <= 0:
            raise ValueError("invalid corporate-action multiplier")
        intercept = intercept + slope * additive
        slope = slope * multiplier
    return slope, intercept


def corporate_action_calibration(
    trusted: pd.DataFrame,
    actions: pd.DataFrame,
) -> tuple[float, float, dict[str, Any]] | None:
    """Validate one corporate-action chain once for every target of a code."""
    if trusted.empty or len(trusted) < 20:
        return None
    if actions is None:
        return None
    descriptions = actions.get("description", pd.Series(dtype=str)).fillna("").astype(str)
    if any(marker in text for text in descriptions for marker in UNSUPPORTED_ACTION_MARKERS):
        return None

    frame = trusted[["date", "close", "close_raw"]].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["close_raw"] = pd.to_numeric(frame["close_raw"], errors="coerce")
    frame = frame.dropna().query("close > 0 and close_raw > 0").sort_values("date")
    events = actions.copy()
    if "date" not in events:
        events = pd.DataFrame(columns=["date"])
    events["date"] = pd.to_datetime(events["date"], errors="coerce")
    events = events.dropna(subset=["date"]).sort_values("date", kind="stable").reset_index(drop=True)
    canonical: list[float] = []
    action_slope, action_intercept = 1.0, 0.0
    event_index = 0
    for row in frame.itertuples(index=False):
        while event_index < len(events) and pd.Timestamp(events.iloc[event_index]["date"]) <= row.date:
            multiplier, additive = action_parameters(events.iloc[event_index])
            if not math.isfinite(multiplier) or multiplier <= 0:
                return None
            action_intercept = action_intercept + action_slope * additive
            action_slope = action_slope * multiplier
            event_index += 1
        canonical.append(action_slope * float(row.close_raw) + action_intercept)
    frame["canonical_close"] = canonical
    transform, diagnostics = fit_affine(
        frame,
        x_column="canonical_close",
        y_column="close",
        min_points=20,
        min_variation=0.005,
        max_absolute_error=0.002,
        max_relative_error=0.0002,
        quantization_floor=0.00055,
    )
    if transform is None:
        return None
    base_slope, base_intercept = transform
    diagnostics.update(
        {
            "base_slope": base_slope,
            "base_intercept": base_intercept,
            "corporate_action_count": int(len(actions)),
        }
    )
    if abs(base_slope - 1.0) > 0.005 or abs(base_intercept) > 0.02:
        return None
    return base_slope, base_intercept, diagnostics


def corporate_action_transform_from_calibration(
    calibration: tuple[float, float, dict[str, Any]] | None,
    actions: pd.DataFrame,
    *,
    target_date: str,
    target_raw_close: float,
    target_validation_raw_close: float | None = None,
    target_adjusted_close: float | None = None,
) -> AffineTransform | None:
    if calibration is None or actions is None:
        return None
    base_slope, base_intercept, base_diagnostics = calibration
    diagnostics = dict(base_diagnostics)
    action_slope, action_intercept = canonical_affine_for_date(actions, target_date)
    slope = base_slope * action_slope
    intercept = base_slope * action_intercept + base_intercept
    if target_adjusted_close is not None:
        validation_raw_close = (
            target_raw_close
            if target_validation_raw_close is None
            else target_validation_raw_close
        )
        crosscheck = abs(slope * validation_raw_close + intercept - target_adjusted_close)
        model_error = max(
            float(diagnostics.get("fit_max_absolute_error", 0.0) or 0.0),
            float(diagnostics.get("holdout_max_absolute_error", 0.0) or 0.0),
        )
        crosscheck_tolerance = (
            0.011
            if target_validation_raw_close is None
            else min(0.011, 0.0051 + model_error)
        )
        diagnostics["target_adjusted_close_error"] = crosscheck
        diagnostics["target_adjusted_close_tolerance"] = crosscheck_tolerance
        diagnostics["target_validation_raw_close"] = validation_raw_close
        if crosscheck > crosscheck_tolerance + 1e-12:
            return None
    return AffineTransform(slope, intercept, "corporate_action_affine", diagnostics)


def corporate_action_transform(
    trusted: pd.DataFrame,
    actions: pd.DataFrame,
    *,
    target_date: str,
    target_raw_close: float,
    target_validation_raw_close: float | None = None,
    target_adjusted_close: float | None = None,
) -> AffineTransform | None:
    return corporate_action_transform_from_calibration(
        corporate_action_calibration(trusted, actions),
        actions,
        target_date=target_date,
        target_raw_close=target_raw_close,
        target_validation_raw_close=target_validation_raw_close,
        target_adjusted_close=target_adjusted_close,
    )


def validate_adjusted_bar(
    transform: AffineTransform,
    raw: dict[str, float],
) -> tuple[dict[str, float] | None, str | None]:
    adjusted = {
        field: transform.slope * raw[field] + transform.intercept
        for field in PRICE_FIELDS
    }
    if any(not math.isfinite(value) or value <= 0 for value in adjusted.values()):
        return None, "invalid_adjusted_price"
    tolerance = 0.0011
    if adjusted["low"] > adjusted["high"] + tolerance:
        return None, "adjusted_low_above_high"
    if min(adjusted["open"], adjusted["close"]) < adjusted["low"] - tolerance:
        return None, "adjusted_body_below_low"
    if max(adjusted["open"], adjusted["close"]) > adjusted["high"] + tolerance:
        return None, "adjusted_body_above_high"
    return {field: round(value, 3) for field, value in adjusted.items()}, None


def source_turnover_and_cap(
    row: pd.Series,
    ths_row: pd.Series | None,
) -> tuple[tuple[float, float] | None, str]:
    approved_turnover = numeric(row.get("approved_turnover"))
    approved_market_cap = numeric(row.get("approved_market_cap"))
    if (
        approved_turnover is not None
        and 0 < approved_turnover <= 1000
        and approved_market_cap is not None
        and approved_market_cap > 0
    ):
        return (approved_turnover, approved_market_cap), "manual_review"
    volume = float(row["em_volume"])
    close_raw = float(row["em_close_raw"])
    turnover = numeric(ths_row.get("turnover")) if ths_row is not None else None
    source = "ths_wencai"
    if turnover is None or turnover <= 0 or turnover > 1000:
        turnover = numeric(row.get("legacy_turnover"))
        source = "legacy_exact_date"
    if turnover is None or turnover <= 0 or turnover > 1000:
        return None, "missing_valid_turnover"
    shares = volume * 100.0 / turnover
    derived_cap = close_raw * shares
    direct_cap = numeric(ths_row.get("market_cap")) if ths_row is not None else None
    if direct_cap is not None and direct_cap > 0 and relative_gap(direct_cap, derived_cap) <= 0.01:
        return (turnover, direct_cap), source + "+ths_cap"
    return (turnover, derived_cap), source + "+derived_cap"


def legacy_valuation_values(legacy_row: pd.Series) -> dict[str, float | pd._libs.missing.NAType]:
    output: dict[str, float | pd._libs.missing.NAType] = {}
    for field in VALUATION_COLUMNS:
        value = pd.Series([legacy_row.get(field)])
        output[field] = float(pd.to_numeric(value, errors="coerce").iloc[0]) if valid_valuation(value, field).iloc[0] else pd.NA
    return output


def build_insert_row(
    columns: list[str],
    source: pd.Series,
    adjusted: dict[str, float],
    turnover: float,
    market_cap: float,
    legacy_row: pd.Series,
) -> dict[str, Any]:
    row: dict[str, Any] = {column: pd.NA for column in columns}
    row["date"] = str(source["date"])
    for field in PRICE_FIELDS:
        row[field] = adjusted[field]
    row["close_raw"] = float(source["em_close_raw"])
    row["volume"] = float(source["em_volume"])
    row["amount"] = float(source["em_amount"])
    row["turnover"] = turnover
    row["market_cap"] = market_cap
    row.update({key: value for key, value in legacy_valuation_values(legacy_row).items() if key in row})
    return row


def insert_rows(
    current: pd.DataFrame,
    rows: Iterable[dict[str, Any]],
) -> pd.DataFrame:
    result = current.copy()
    columns = list(result.columns)
    pending = list(rows)
    if not pending:
        return result
    existing = set(_date_keys(result["date"]).dropna())
    pending = [row for row in pending if str(row["date"]) not in existing]
    if not pending:
        return result
    inserted_days = {str(row["date"]) for row in pending}
    result = pd.concat([result, pd.DataFrame(pending, columns=columns)], ignore_index=True)
    result["_date"] = _date_keys(result["date"])
    result = result.dropna(subset=["_date"]).drop_duplicates("_date", keep="last")
    result = result.sort_values("_date").reset_index(drop=True)
    affected = set(inserted_days)
    for day in inserted_days:
        later = result.loc[result["_date"].gt(day), "_date"]
        if not later.empty:
            affected.add(str(later.iloc[0]))
    previous = pd.to_numeric(result["close"], errors="coerce").shift(1)
    close = pd.to_numeric(result["close"], errors="coerce")
    high = pd.to_numeric(result["high"], errors="coerce")
    low = pd.to_numeric(result["low"], errors="coerce")
    mask = result["_date"].isin(affected) & previous.gt(0)
    if "change" in result:
        result.loc[mask, "change"] = close - previous
    if "change_pct" in result:
        result.loc[mask, "change_pct"] = (close / previous - 1.0) * 100.0
    if "amplitude" in result:
        result.loc[mask, "amplitude"] = (high - low) / previous * 100.0
    result["date"] = result["_date"]
    return result.sort_values("_date", ascending=False).drop(columns="_date")[columns]


def atomic_backup(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as handle:
            temporary = Path(handle.name)
        shutil.copy2(source, temporary)
        os.replace(temporary, destination)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def actions_to_json(actions_by_code: dict[str, pd.DataFrame], path: Path) -> None:
    payload: dict[str, list[dict[str, Any]]] = {}
    for code, frame in actions_by_code.items():
        rows: list[dict[str, Any]] = []
        for row in frame.to_dict("records"):
            rows.append(
                {
                    key: (pd.Timestamp(value).strftime("%Y-%m-%d") if key == "date" and pd.notna(value) else value)
                    for key, value in row.items()
                }
            )
        payload[code] = rows
    _atomic_json(payload, path)


def actions_from_json(path: Path) -> dict[str, pd.DataFrame]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    output: dict[str, pd.DataFrame] = {}
    for code, rows in payload.items():
        frame = pd.DataFrame(rows)
        if "date" in frame:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        output[str(code).zfill(6)] = frame
    return output


def fetch_actions(codes: Iterable[str]) -> tuple[dict[str, pd.DataFrame], list[dict[str, str]]]:
    output: dict[str, pd.DataFrame] = {}
    failures: list[dict[str, str]] = []
    with THSDataSource() as source:
        for index, code in enumerate(sorted(set(codes)), 1):
            try:
                output[code] = source.fetch_corporate_actions(code)
            except Exception as exc:
                failures.append({"code": code, "type": type(exc).__name__, "error": str(exc)[:500]})
            if index == 1 or index % 25 == 0:
                print(f"actions {index}/{len(set(codes))} failures={len(failures)}", flush=True)
    return output, failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--trusted-ths-dir", default="data_ths")
    parser.add_argument("--legacy-dir", default="data_pre_ths_backup_20260727_110350")
    parser.add_argument(
        "--targets",
        default="artifacts/maintenance/all_data_gaps/eastmoney_desktop_remaining_results.csv",
    )
    parser.add_argument(
        "--fetched-bars",
        default="artifacts/maintenance/all_data_gaps/eastmoney_desktop_fetched_bars.csv",
    )
    parser.add_argument(
        "--ths-raw",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_bars.csv",
    )
    parser.add_argument(
        "--ths-adjusted-close",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_bars_adjusted.csv",
    )
    parser.add_argument(
        "--actions-cache",
        default="artifacts/maintenance/all_data_gaps/eastmoney_desktop_actions.json",
    )
    parser.add_argument(
        "--report",
        default="artifacts/maintenance/all_data_gaps/eastmoney_desktop_write_report.json",
    )
    parser.add_argument(
        "--details",
        default="artifacts/maintenance/all_data_gaps/eastmoney_desktop_write_details.csv",
    )
    parser.add_argument(
        "--backup-dir",
        default="artifacts/maintenance/all_data_gaps/eastmoney_desktop_write_backup",
    )
    parser.add_argument("--reuse-actions", action="store_true")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir).resolve()
    trusted_dir = Path(args.trusted_ths_dir).resolve()
    legacy_dir = Path(args.legacy_dir).resolve()
    target_path = (ROOT / args.targets).resolve()
    fetched_path = (ROOT / args.fetched_bars).resolve()
    ths_raw_path = (ROOT / args.ths_raw).resolve()
    adjusted_path = (ROOT / args.ths_adjusted_close).resolve()
    actions_path = (ROOT / args.actions_cache).resolve()
    report_path = (ROOT / args.report).resolve()
    details_path = (ROOT / args.details).resolve()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = (ROOT / args.backup_dir).resolve() / run_id
    started = time.time()

    targets = pd.read_csv(target_path, encoding="utf-8-sig", dtype={"code": str, "date": str})
    fetched = pd.read_csv(fetched_path, encoding="utf-8-sig", dtype={"code": str, "date": str})
    ths_raw = pd.read_csv(ths_raw_path, encoding="utf-8-sig", dtype={"code": str, "date": str})
    adjusted_close = pd.read_csv(adjusted_path, encoding="utf-8-sig", dtype={"code": str, "date": str})
    for frame in (targets, fetched, ths_raw, adjusted_close):
        frame["code"] = frame["code"].astype(str).str.zfill(6)
        frame["date"] = _date_keys(frame["date"])
    ths_lookup = ths_raw.drop_duplicates(["code", "date"], keep="last").set_index(["code", "date"])
    adjusted_lookup = adjusted_close.drop_duplicates(["code", "date"], keep="last").set_index(["code", "date"])

    frames: dict[str, pd.DataFrame] = {}
    trusted_frames: dict[str, pd.DataFrame] = {}
    legacy_frames: dict[str, pd.DataFrame] = {}
    legacy_indexes: dict[str, pd.DataFrame] = {}
    trusted_pools: dict[str, pd.DataFrame] = {}
    adjusted_pools: dict[str, pd.DataFrame] = {}
    signatures: dict[str, tuple[int, int, str]] = {}
    for code in sorted(targets["code"].unique()):
        current_path = code_path(data_dir, code)
        frames[code] = _read_csv(current_path)
        trusted_frames[code] = _read_csv(code_path(trusted_dir, code))
        legacy_frames[code] = _read_csv(code_path(legacy_dir, code))
        legacy_index = legacy_frames[code].copy()
        legacy_index["_date"] = _date_keys(legacy_index["date"])
        legacy_indexes[code] = legacy_index.dropna(subset=["_date"]).drop_duplicates("_date", keep="last").set_index("_date")
        trusted_pools[code] = trusted_anchor_pool(
            trusted_frames[code], legacy_frames[code]
        )
        adjusted_pools[code] = adjusted_anchor_pool(
            fetched, adjusted_close, legacy_frames[code], code=code
        )
        stat = current_path.stat()
        signatures[code] = (stat.st_size, stat.st_mtime_ns, sha256(current_path))

    preliminary: list[dict[str, Any]] = []
    candidates_needing_actions: set[str] = set()
    direct_transforms: dict[tuple[str, str], AffineTransform] = {}
    source_rows: dict[tuple[str, str], pd.Series] = {}
    legacy_rows: dict[tuple[str, str], pd.Series] = {}
    ths_rows: dict[tuple[str, str], pd.Series | None] = {}
    validation_raw_closes: dict[tuple[str, str], float | None] = {}
    bar_sources: dict[tuple[str, str], str] = {}
    date_relabels: dict[tuple[str, str], str] = {}
    rejected: dict[tuple[str, str], str] = {}

    for _, row in targets.iterrows():
        code, day = str(row["code"]), str(row["date"])
        key = (code, day)
        if key in APPROVED_NON_TRADING_DAYS:
            rejected[key] = "approved_non_trading_day"
            preliminary.append(
                {"code": code, "date": day, "status": "approved_non_trading_day"}
            )
            continue
        ths_row = ths_lookup.loc[key] if key in ths_lookup.index else None
        approved_source_label: str | None = None
        if key in APPROVED_FULL_BAR_OVERRIDES:
            row = row.copy()
            override = APPROVED_FULL_BAR_OVERRIDES[key]
            for field in PRICE_FIELDS:
                row[f"em_{field}_raw"] = override[field]
            row["em_volume"] = override["volume"]
            row["em_amount"] = override["amount"]
            row["approved_turnover"] = override["turnover"]
            row["approved_market_cap"] = override["market_cap"]
            approved_source_label = "manual_review"
        elif key == ("600665", "1993-07-09") and ths_row is not None:
            row = row.copy()
            for field in PRICE_FIELDS:
                row[f"em_{field}_raw"] = ths_row.get(f"{field}_raw")
            row["em_volume"] = ths_row.get("volume")
            row["em_amount"] = row.get("legacy_amount")
            approved_source_label = "ths_wencai+legacy_amount"
        validation_raw_close = (
            numeric(ths_row.get("close_raw"))
            if ths_row is not None and str(row.get("ths_raw_status") or "") == "complete"
            else None
        )
        row, bar_source, passed, reason, gate = validated_trade_source(
            row,
            ths_row,
            require_legacy_trade_crosscheck=(
                key not in APPROVED_LEGACY_CROSSCHECK_BYPASS
            ),
            prefer_ths=key in APPROVED_FORCE_THS_KEYS,
        )
        if approved_source_label is not None and passed:
            bar_source = approved_source_label
        if not passed:
            rejected[key] = reason
            preliminary.append({"code": code, "date": day, "status": reason, **gate})
            continue
        relabel_from = APPROVED_DATE_RELABELS.get(key)
        if relabel_from is not None:
            frame_dates = _date_keys(frames[code]["date"])
            if day not in set(frame_dates.dropna()) and relabel_from in set(
                frame_dates.dropna()
            ):
                frames[code] = frames[code].loc[frame_dates.ne(relabel_from)].copy()
                date_relabels[key] = relabel_from
        existing = set(_date_keys(frames[code]["date"]).dropna())
        if day in existing:
            rejected[key] = "already_present"
            preliminary.append({"code": code, "date": day, "status": "already_present"})
            continue
        duplicate = nearby_trade_fingerprint(
            frames[code],
            target_date=day,
            close_raw=float(row["em_close_raw"]),
            volume=float(row["em_volume"]),
            amount=float(row["em_amount"]),
        )
        if duplicate:
            rejected[key] = "nearby_trade_fingerprint"
            preliminary.append({"code": code, "date": day, "status": "nearby_trade_fingerprint", "duplicate_date": duplicate})
            continue
        if day not in legacy_indexes[code].index:
            rejected[key] = "missing_legacy_row"
            preliminary.append({"code": code, "date": day, "status": "missing_legacy_row"})
            continue
        old_row = legacy_indexes[code].loc[day]
        transform = trusted_segment_transform_from_pool(
            trusted_pools[code],
            target_date=day,
            target_legacy_close=float(row["legacy_close"]),
            target_raw_close=float(row["em_close_raw"]),
        )
        if transform is None:
            target_adjusted = None
            if key in adjusted_lookup.index:
                target_adjusted = numeric(adjusted_lookup.loc[key].get("close_adjusted"))
            transform = adjusted_close_transform_from_pool(
                adjusted_pools[code],
                target_date=day,
                target_legacy_close=float(row["legacy_close"]),
                target_raw_close=float(row["em_close_raw"]),
                target_validation_raw_close=validation_raw_close,
                target_adjusted_close=target_adjusted,
            )
        if transform is None:
            candidates_needing_actions.add(code)
        else:
            direct_transforms[key] = transform
        source_rows[key] = row
        legacy_rows[key] = old_row
        ths_rows[key] = ths_row
        validation_raw_closes[key] = validation_raw_close
        bar_sources[key] = bar_source

    if args.reuse_actions:
        actions_by_code = actions_from_json(actions_path)
        action_failures: list[dict[str, str]] = []
    else:
        actions_by_code, action_failures = fetch_actions(candidates_needing_actions)
        actions_to_json(actions_by_code, actions_path)

    action_calibrations = {
        code: corporate_action_calibration(
            trusted_frames[code], actions_by_code.get(code)
        )
        for code in candidates_needing_actions
    }

    pending_by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    details = list(preliminary)
    method_counts: Counter[str] = Counter()
    turnover_counts: Counter[str] = Counter()
    bar_source_counts: Counter[str] = Counter()
    for key in sorted(source_rows):
        code, day = key
        row = source_rows[key]
        transform = direct_transforms.get(key)
        if transform is None:
            target_adjusted = None
            if key in adjusted_lookup.index:
                target_adjusted = numeric(adjusted_lookup.loc[key].get("close_adjusted"))
            transform = corporate_action_transform_from_calibration(
                action_calibrations.get(code),
                actions_by_code.get(code),
                target_date=day,
                target_raw_close=float(row["em_close_raw"]),
                target_validation_raw_close=validation_raw_closes.get(key),
                target_adjusted_close=target_adjusted,
            )
        if transform is None and key in APPROVED_LOCAL_SEGMENT_TARGETS:
            transform = bracketed_local_transform(
                trusted_frames[code],
                target_date=day,
                target_raw_close=float(row["em_close_raw"]),
                target_adjusted_close=APPROVED_LOCAL_SEGMENT_TARGETS[key],
            )
        if transform is None and key in APPROVED_FIXED_TRANSFORMS:
            slope, intercept, method = APPROVED_FIXED_TRANSFORMS[key]
            transform = AffineTransform(
                slope,
                intercept,
                method,
                {"approval": "manual_review"},
            )
        if transform is None:
            details.append({"code": code, "date": day, "status": "adjustment_transform_unproven"})
            continue
        raw = {field: float(row[f"em_{field}_raw"]) for field in PRICE_FIELDS}
        adjusted, reason = validate_adjusted_bar(transform, raw)
        if adjusted is None:
            details.append({"code": code, "date": day, "status": reason or "invalid_adjusted_bar"})
            continue
        turnover_cap, turnover_source = source_turnover_and_cap(row, ths_rows[key])
        if turnover_cap is None:
            details.append({"code": code, "date": day, "status": turnover_source})
            continue
        turnover, market_cap = turnover_cap
        inserted = build_insert_row(
            list(frames[code].columns),
            row,
            adjusted,
            turnover,
            market_cap,
            legacy_rows[key],
        )
        pending_by_code[code].append(inserted)
        method_counts[transform.method] += 1
        turnover_counts[turnover_source] += 1
        bar_source_counts[bar_sources[key]] += 1
        details.append(
            {
                "code": code,
                "date": day,
                "status": "auto_apply",
                "method": transform.method,
                "slope": transform.slope,
                "intercept": transform.intercept,
                "turnover_source": turnover_source,
                "bar_source": bar_sources[key],
                "date_relabel_from": date_relabels.get(key),
                "anchor_points": transform.diagnostics.get("anchor_points"),
                "fit_max_absolute_error": transform.diagnostics.get("fit_max_absolute_error"),
                "holdout_max_absolute_error": transform.diagnostics.get("holdout_max_absolute_error"),
                "target_adjusted_close_error": transform.diagnostics.get("target_adjusted_close_error"),
            }
        )

    pending_frames = {
        code: insert_rows(frames[code], rows) for code, rows in pending_by_code.items()
    }
    source_conflicts: list[str] = []
    if args.apply:
        for code in pending_frames:
            path = code_path(data_dir, code)
            stat = path.stat()
            current_signature = (stat.st_size, stat.st_mtime_ns, sha256(path))
            if current_signature != signatures[code]:
                source_conflicts.append(code)
    applied_files = 0
    if args.apply and not source_conflicts:
        for code, frame in pending_frames.items():
            path = code_path(data_dir, code)
            atomic_backup(path, backup_dir / path.relative_to(data_dir))
            _atomic_csv(frame, path)
            invalidate_caches(data_dir, code)
            applied_files += 1

    details_frame = pd.DataFrame(details).sort_values(["code", "date"]).reset_index(drop=True)
    details_path.parent.mkdir(parents=True, exist_ok=True)
    details_frame.to_csv(details_path, index=False, encoding="utf-8-sig")
    status_counts = details_frame["status"].value_counts().to_dict()
    partial = bool(action_failures or source_conflicts)
    report = {
        "status": "PARTIAL" if partial else "COMPLETED",
        "requested_apply": bool(args.apply),
        "applied": bool(args.apply and not source_conflicts),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "Eastmoney desktop TCP raw OHLCV/amount with THS affine adjustment validation",
        "serial_requests": True,
        "data_dir": str(data_dir),
        "trusted_ths_dir": str(trusted_dir),
        "legacy_dir": str(legacy_dir),
        "inputs": {
            "targets": {"path": str(target_path), "sha256": sha256(target_path)},
            "fetched_bars": {"path": str(fetched_path), "sha256": sha256(fetched_path)},
            "ths_raw": {"path": str(ths_raw_path), "sha256": sha256(ths_raw_path)},
            "ths_adjusted_close": {"path": str(adjusted_path), "sha256": sha256(adjusted_path)},
            "actions": {"path": str(actions_path), "sha256": sha256(actions_path)},
        },
        "outputs": {"details": str(details_path), "backup_dir": str(backup_dir)},
        "counts": {
            "targets": int(len(targets)),
            "auto_apply": int(status_counts.get("auto_apply", 0)),
            "rejected": int(len(targets) - status_counts.get("auto_apply", 0)),
            "changed_files": len(pending_frames),
            "applied_files": applied_files,
            "status": {str(key): int(value) for key, value in status_counts.items()},
            "methods": dict(method_counts),
            "turnover_sources": dict(turnover_counts),
            "bar_sources": dict(bar_source_counts),
        },
        "action_failures": action_failures,
        "source_conflicts": source_conflicts,
        "policy": {
            "legacy_volume_amount_relative_tolerance": 0.01,
            "ths_raw_price_relative_tolerance": 0.001,
            "ths_raw_price_absolute_tolerance": 0.01,
            "nearby_fingerprint_calendar_days": 7,
            "legacy_adjustment_factor_tolerance": 0.0001,
            "action_base_slope_tolerance": 0.005,
            "action_base_intercept_tolerance": 0.02,
            "action_full_max_absolute_error": 0.002,
        },
        "elapsed_seconds": round(time.time() - started, 3),
    }
    _atomic_json(report, report_path)
    if args.apply and not source_conflicts:
        _update_manifest(
            data_dir,
            "historical_missing_bars_eastmoney_desktop",
            {
                "source": report["source"],
                "inserted": report["counts"]["auto_apply"],
                "methods": dict(method_counts),
                "turnover_sources": dict(turnover_counts),
                "report": str(report_path),
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"counts={report['counts']} failures={len(action_failures)} conflicts={len(source_conflicts)}", flush=True)
    return 2 if partial else 0


if __name__ == "__main__":
    raise SystemExit(main())
