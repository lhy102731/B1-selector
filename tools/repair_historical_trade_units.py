"""Repair proven historical THS volume-unit regimes in the committed CSVs.

The THS archive has a small number of old intervals where ``volume`` uses a
different unit while ``amount`` and raw prices remain correct.  This repair
delegates regime detection to :meth:`THSDataSource._repair_trade_units`, then
recomputes turnover from the already-committed circulating market cap.  It
does not change dates, prices, valuation fields, or isolated ambiguous rows.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_valuation_fields import (
    _atomic_csv,
    _atomic_json,
    _read_csv,
    _update_manifest,
    stock_files,
)
from utils.ths_data_source import THSDataSource


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(
        frame.get(column, pd.Series(index=frame.index, dtype=float)),
        errors="coerce",
    )


def repair_frame(
    frame: pd.DataFrame,
    reference_amounts: dict[str, float] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Return one repaired stock frame plus row-level audit counts."""
    result = frame.copy()
    required = {
        "date",
        "low",
        "high",
        "close",
        "close_raw",
        "volume",
        "amount",
        "turnover",
        "market_cap",
    }
    if result.empty or not required.issubset(result.columns):
        return result, {"invalid_schema": 1, "volume_changed": 0}

    adjusted_close = _numeric(result, "close")
    raw_close = _numeric(result, "close_raw")
    factor = adjusted_close / raw_close
    target_dates = pd.to_datetime(result["date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    current_amount = _numeric(result, "amount")
    working_amount = current_amount.copy()
    reference_used = pd.Series(False, index=result.index)
    if reference_amounts:
        reference = pd.to_numeric(target_dates.map(reference_amounts), errors="coerce")
        reference_used = reference.notna() & (reference > 0)
        # These reference values came directly from THS Wencai for the exact
        # code/date and are preferred over a rounded legacy fallback.  They are
        # still subject to the same VWAP/regime gate below before persistence.
        working_amount = reference.where(reference_used, working_amount)

    repair_input = pd.DataFrame(
        {
            "date": pd.to_datetime(result["date"], errors="coerce"),
            "low_raw": _numeric(result, "low") / factor,
            "high_raw": _numeric(result, "high") / factor,
            "close_raw": raw_close,
            "volume": _numeric(result, "volume"),
            "amount": working_amount,
        }
    )
    repaired, source_audit = THSDataSource._repair_trade_units(repair_input)
    repaired_dates = pd.to_datetime(repaired["date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    repaired = repaired.assign(_date=repaired_dates).set_index("_date")
    mapped_volume = pd.to_numeric(target_dates.map(repaired["volume"]), errors="coerce")
    mapped_amount = pd.to_numeric(target_dates.map(repaired["amount"]), errors="coerce")

    old_volume = _numeric(result, "volume")
    old_amount = _numeric(result, "amount")
    volume_changed = (
        old_volume.notna()
        & mapped_volume.notna()
        & ~np.isclose(old_volume, mapped_volume, rtol=1e-12, atol=1e-9)
    )
    amount_cleared = old_amount.notna() & mapped_amount.isna()
    amount_filled = old_amount.isna() & mapped_amount.notna()
    amount_replaced = (
        old_amount.notna()
        & mapped_amount.notna()
        & reference_used
        & ~np.isclose(old_amount, mapped_amount, rtol=1e-7, atol=0.01)
    )

    result.loc[volume_changed, "volume"] = mapped_volume.loc[volume_changed]
    result.loc[amount_cleared, "amount"] = pd.NA
    persist_amount = amount_filled | amount_replaced
    result.loc[persist_amount, "amount"] = mapped_amount.loc[persist_amount]

    # The committed market cap is derived from THS outstanding-share history
    # and is therefore the independent share-count anchor for a volume repair.
    # If it is unavailable, preserve the old turnover-implied share count.
    market_cap = _numeric(result, "market_cap")
    old_turnover = _numeric(frame, "turnover")
    shares_from_cap = market_cap / raw_close
    shares_from_turnover = old_volume * 100.0 / old_turnover
    shares = shares_from_cap.where(
        shares_from_cap.notna() & (shares_from_cap > 0), shares_from_turnover
    )
    recalc = volume_changed & shares.notna() & (shares > 0)
    result.loc[recalc, "turnover"] = (
        mapped_volume.loc[recalc] * 100.0 / shares.loc[recalc]
    )
    rebuild_cap = recalc & ~(market_cap.notna() & (market_cap > 0)) & (raw_close > 0)
    result.loc[rebuild_cap, "market_cap"] = raw_close.loc[rebuild_cap] * shares.loc[
        rebuild_cap
    ]

    return result, {
        **source_audit,
        "volume_changed": int(volume_changed.sum()),
        "turnover_recomputed": int(recalc.sum()),
        "market_cap_rebuilt": int(rebuild_cap.sum()),
        "amount_cleared": int(amount_cleared.sum()),
        "amount_filled_from_ths": int(amount_filled.sum()),
        "amount_replaced_from_ths": int(amount_replaced.sum()),
        "reference_amount_rows": int(reference_used.sum()),
    }


def run(args: argparse.Namespace) -> int:
    data_dir = Path(args.data_dir).resolve()
    backup_dir = (ROOT / args.backup_dir).resolve()
    paths = stock_files(data_dir)
    reference_path = (ROOT / args.ths_amounts).resolve()
    reference_by_code: dict[str, dict[str, float]] = {}
    if reference_path.exists():
        reference_frame = pd.read_csv(
            reference_path,
            encoding="utf-8-sig",
            dtype={"code": str, "date": str},
        )
        reference_frame["code"] = reference_frame["code"].str.zfill(6)
        reference_frame["amount"] = pd.to_numeric(
            reference_frame["amount"], errors="coerce"
        )
        reference_frame = reference_frame.dropna(subset=["code", "date", "amount"])
        reference_frame = reference_frame[reference_frame["amount"] > 0]
        for code, group in reference_frame.groupby("code", sort=False):
            reference_by_code[str(code)] = dict(
                zip(group["date"].astype(str), group["amount"].astype(float))
            )
    total: Counter[str] = Counter()
    changed_files = 0
    regimes: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    started = time.time()
    for index, path in enumerate(paths, 1):
        try:
            frame = _read_csv(path)
            repaired, audit = repair_frame(
                frame, reference_by_code.get(path.stem)
            )
            total.update(
                {
                    key: int(value)
                    for key, value in audit.items()
                    if isinstance(value, (int, np.integer))
                }
            )
            for regime in audit.get("volume_unit_regimes", []):
                regimes.append({"code": path.stem, **regime})
            changed = bool(
                audit.get("volume_changed")
                or audit.get("amount_cleared")
                or audit.get("amount_filled_from_ths")
                or audit.get("amount_replaced_from_ths")
            )
            if changed:
                changed_files += 1
                if args.apply:
                    relative = path.relative_to(data_dir)
                    backup_path = backup_dir / relative
                    backup_path.parent.mkdir(parents=True, exist_ok=True)
                    if not backup_path.exists():
                        shutil.copy2(path, backup_path)
                    _atomic_csv(repaired, path)
        except Exception as exc:
            failed.append(
                {
                    "code": path.stem,
                    "type": type(exc).__name__,
                    "error": str(exc)[:500],
                }
            )
        if index == 1 or index % 200 == 0:
            print(
                f"trade-units {index}/{len(paths)} changed_rows={total['volume_changed']} "
                f"failed={len(failed)} elapsed={time.time() - started:.1f}s",
                flush=True,
            )

    report = {
        "status": "COMPLETED" if not failed else "FAILED",
        "applied": bool(args.apply),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_dir": str(data_dir),
        "backup_dir": str(backup_dir),
        "files": len(paths),
        "ths_amounts": str(reference_path),
        "ths_amount_codes": len(reference_by_code),
        "changed_files": changed_files,
        "counts": dict(total),
        "regimes": regimes,
        "failed": failed,
        "elapsed_seconds": round(time.time() - started, 3),
        "policy": {
            "minimum_evidence_rows": THSDataSource.MIN_VOLUME_REGIME_ROWS,
            "minimum_regime_share": THSDataSource.MIN_VOLUME_REGIME_SHARE,
            "basis_factors": list(THSDataSource.VOLUME_UNIT_CANDIDATES),
            "directions": ["multiply", "divide"],
        },
    }
    report_path = (ROOT / args.report).resolve()
    _atomic_json(report, report_path)
    if args.apply and not failed:
        _update_manifest(
            data_dir,
            "historical_ths_trade_unit_repair",
            {
                "method": "stable VWAP-compatible volume regimes",
                "directions": ["multiply", "divide"],
                "minimum_evidence_rows": THSDataSource.MIN_VOLUME_REGIME_ROWS,
                "minimum_regime_share": THSDataSource.MIN_VOLUME_REGIME_SHARE,
                "changed_rows": int(total["volume_changed"]),
                "amount_cleared": int(total["amount_cleared"]),
                "amount_filled_from_ths": int(total["amount_filled_from_ths"]),
                "amount_replaced_from_ths": int(total["amount_replaced_from_ths"]),
                "report": str(report_path),
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"counts={dict(total)} failed={len(failed)}", flush=True)
    return 0 if not failed else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument(
        "--ths-amounts",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_amount_values.csv",
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--backup-dir",
        default="artifacts/maintenance/all_data_gaps/trade_unit_backup",
    )
    parser.add_argument(
        "--report",
        default="artifacts/maintenance/all_data_gaps/trade_unit_repair_report.json",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
