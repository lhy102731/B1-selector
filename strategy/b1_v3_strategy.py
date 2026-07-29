# -*- coding: utf-8 -*-
"""
B1 V3 unified strategy.
Layer 0: wave analysis + signal extraction
Layer 1: feature scoring (FAC)
Layer 2: filter & rank
Layer 3: backtest simulation

All thresholds parameterized via B1V3Params. No hardcoded magic numbers.
"""

import sys, os, pickle, hashlib, time, json, tempfile
from dataclasses import asdict
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
import numpy as np
import multiprocessing as mp
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, str(Path(__file__).parent.parent))

from strategy.b1_v3_config import B1V3Params, build_fac_list

# ============================================================
# Constants
# ============================================================
INDICATORS_DIR = Path("data/indicators_cache")
RAW_CACHE_DIR = Path("data/signal_cache")
FUND_CACHE_PATH = Path("data/fund_cache.pkl")
PE_CACHE_PATH = Path("data/baostock_pepb_daily.pkl")
RAW_CACHE_AUXILIARY_PATHS = (
    ("fund-cache", FUND_CACHE_PATH),
    ("pe-cache", PE_CACHE_PATH),
    ("history-bonus", Path("data/stock_scoring/bonus_lookup.json")),
    ("concept-map", Path("data/block/concept.json")),
)

# Raw-cache identity version (Phase 1.1 fix). The identity binds parameters,
# universe, input data, and extraction code so a hit cannot silently reuse stale
# candidates. Bump CACHE_VERSION if the serialized cache contract changes.
CACHE_VERSION = "v3-identity-1"


def _canonical_json_value(value):
    if isinstance(value, dict):
        return {str(key): _canonical_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_json_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_canonical_json_value(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(item, sort_keys=True, default=str),
        )
    return value


def _param_fingerprint(p):
    """Return (fingerprint_hex, params_dict) over ALL B1V3Params fields.

    Option (1): full-field fingerprint -- always correct (any param change ->
    distinct cache key -> re-extraction). Slightly conservative: changing a
    post-cache-only param (e.g. top_n) also re-extracts, which is acceptable.
    """
    try:
        params_dict = asdict(p)
    except TypeError:
        params_dict = {k: getattr(p, k) for k in vars(p)}
    params_dict = _canonical_json_value(params_dict)
    blob = json.dumps(params_dict, sort_keys=True, default=str, separators=(",", ":"))
    fp = hashlib.sha256((CACHE_VERSION + "|" + blob).encode()).hexdigest()
    return fp, params_dict


def _universe_fingerprint(stock_codes):
    """Return a stable identity for the exact stock universe passed by the caller."""
    blob = json.dumps([str(code) for code in stock_codes], separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()


def _sha256_path(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _data_snapshot_fingerprint(stock_codes):
    """Bind a raw cache to every selected indicator and auxiliary data file."""
    digest = hashlib.sha256()
    inputs = [
        (f"indicator:{code}", INDICATORS_DIR / f"{code}.parquet")
        for code in stock_codes
    ]
    inputs.extend(RAW_CACHE_AUXILIARY_PATHS)
    for label, path in inputs:
        digest.update(str(label).encode("utf-8"))
        digest.update(b"\0")
        digest.update(
            _sha256_path(path).encode("ascii") if path.exists() else b"MISSING"
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _feature_contract_fingerprint():
    """Bind a raw cache to the source files that define signal extraction."""
    root = Path(__file__).resolve().parent.parent
    paths = [
        Path(__file__).resolve(),
        root / "strategy" / "b1_v3_config.py",
        root / "strategy" / "b1_v3_dtw_fusion.py",
        root / "utils" / "s1_filter.py",
    ]
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(
            _sha256_path(path).encode("ascii") if path.exists() else b"MISSING"
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _atomic_write_pickle(path, payload):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.replace(temporary, destination)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _atomic_write_json(path, payload):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n"
    ).encode("utf-8")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.replace(temporary, destination)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _load_raw_cache(cache_file, meta_file, expected_identity):
    try:
        metadata = json.loads(Path(meta_file).read_text(encoding="utf-8"))
        if metadata.get("identity") != expected_identity:
            return None
        with Path(cache_file).open("rb") as handle:
            payload = pickle.load(handle)
    except Exception:
        return None
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        return None
    if payload.get("identity") != expected_identity:
        return None
    by_date = payload.get("by_date")
    stock_codes = payload.get("stock_codes")
    if not isinstance(by_date, dict) or not isinstance(stock_codes, list):
        return None
    return by_date, stock_codes

PE_CACHE = None
FUND_CACHE = None
HIST_BONUS = None
CONCEPT_COUNT = None
STOCK_CONCEPTS = None
CONCEPT_STOCKS = None

def _load_pe_cache():
    global PE_CACHE
    if PE_CACHE is None and PE_CACHE_PATH.exists():
        with open(PE_CACHE_PATH, 'rb') as f:
            PE_CACHE = pickle.load(f)
    return PE_CACHE

def _load_fund_cache():
    global FUND_CACHE
    if FUND_CACHE is None and FUND_CACHE_PATH.exists():
        with open(FUND_CACHE_PATH, 'rb') as f:
            FUND_CACHE = pickle.load(f)
    return FUND_CACHE

def _load_hist_bonus():
    global HIST_BONUS
    if HIST_BONUS is None:
        import json
        bonus_path = Path("data/stock_scoring/bonus_lookup.json")
        if bonus_path.exists():
            with open(bonus_path, 'r') as f:
                HIST_BONUS = json.load(f)
        else:
            HIST_BONUS = {}
    return HIST_BONUS

def _load_concept_count():
    """Build stock -> concept count lookup from concept.json."""
    global CONCEPT_COUNT
    if CONCEPT_COUNT is None:
        import json
        concept_path = Path("data/block/concept.json")
        if concept_path.exists():
            with open(concept_path, 'r', encoding='utf-8') as f:
                d = json.load(f)
            stock_map = d.get('stock_map', {})
            CONCEPT_COUNT = {}
            for cid, stocks in stock_map.items():
                if isinstance(stocks, list):
                    for s in stocks:
                        if isinstance(s, str) and ':' in s:
                            code = s.split(':')[1].rstrip('*')
                            if len(code) == 6 and code.isdigit():
                                CONCEPT_COUNT[code] = CONCEPT_COUNT.get(code, 0) + 1
        else:
            CONCEPT_COUNT = {}
    return CONCEPT_COUNT


# ============================================================
# WAVE LIFECYCLE ANALYSIS (V1 logic, fully parameterized)
# ============================================================

def analyze_wave(df_asc: pd.DataFrame, p: B1V3Params) -> dict:
    """
    Analyze build-position wave lifecycle from V1.
    df_asc must be sorted ascending by date.
    Returns dict of wave features.
    """
    n = len(df_asc)
    if n < 20:
        return _empty_wave_result()

    close = df_asc['close'].values
    volume = df_asc['volume'].values
    opens = df_asc['open'].values
    high = df_asc['high'].values
    low = df_asc['low'].values
    yellow = df_asc.get('yellow_line', pd.Series([np.nan]*n)).values
    turnover = df_asc.get('turnover', pd.Series([np.nan]*n)).values

    # === MODULE 1: Surge day detection ===
    avg_vol_5 = pd.Series(volume).rolling(p.surge_ma5_lookback, min_periods=1).mean().shift(1).values
    pct_chg = pd.Series(close).pct_change().values * 100
    gain_cond = pct_chg >= p.surge_min_gain_pct
    positive_cond = close > opens if p.surge_require_positive else np.ones(n, dtype=bool)
    volume_cond = volume > avg_vol_5 * p.surge_vol_vs_ma5
    is_surge_day = gain_cond & positive_cond & volume_cond

    surge_indices = np.where(is_surge_day)[0].tolist()
    if not surge_indices:
        return _empty_wave_result()

    # === MODULE 2: Group formation ===
    groups = []
    current = [surge_indices[0]]
    for idx in surge_indices[1:]:
        if idx - current[-1] <= p.group_forward_gap_max:
            current.append(idx)
        else:
            groups.append(current)
            current = [idx]
    groups.append(current)

    # Group summaries
    group_info = []
    for gi, g in enumerate(groups):
        pre_idx = max(0, g[0] - 1)
        start_close = close[pre_idx] if pre_idx < g[0] else opens[g[0]]
        next_start = groups[gi + 1][0] if gi + 1 < len(groups) else n
        seg_high = high[g[0]:next_start]
        max_high_idx_in_seg = g[0] + int(np.argmax(seg_high))
        surge_max_vol = float(np.max(volume[g[0]:max_high_idx_in_seg + 1]))
        group_info.append({
            'start_idx': int(g[0]),
            'end_idx': int(max_high_idx_in_seg),
            'surge_max_vol': surge_max_vol,
            'start_close': float(start_close),
        })

    # Backward merge from last group
    last = group_info[-1]
    last_start_vol = last['surge_max_vol']
    for g in reversed(group_info[:-1]):
        gap_days = last['start_idx'] - g['end_idx'] - 1
        if gap_days > p.group_back_merge_gap_max:
            break

        gap_slice = df_asc.iloc[g['end_idx'] + 1:last['start_idx']]

        # Wave break check in gap
        if p.group_back_merge_check_wave_break and len(gap_slice) > 0:
            check_range = df_asc.iloc[g['end_idx'] + 1:last['start_idx'] + 1]
            yv = check_range['yellow_line'].values
            cv = check_range['close'].values
            lv = check_range['low'].values
            broken = False
            for gi_idx in range(len(cv)):
                if not np.isnan(yv[gi_idx]) and cv[gi_idx] < yv[gi_idx]:
                    stop_ref = lv[gi_idx] - p.wave_break_stop_width
                    if gi_idx + 1 < len(cv) and np.any(cv[gi_idx+1:] < stop_ref):
                        broken = True
                        break
            if broken:
                break

        # Volume contraction check
        late_n = min(5, max(3, len(gap_slice) // 2))
        if late_n > 0:
            late_vol = gap_slice['volume'].tail(late_n).mean()
            if late_vol >= g['surge_max_vol'] * p.group_back_merge_vol_ratio:
                break

        # Price retention check
        gap_low = gap_slice['close'].min()
        if gap_low < g['start_close'] * p.group_back_merge_price_retention:
            break

        # Merge
        last['start_idx'] = g['start_idx']
        last['start_close'] = g['start_close']
        end_vol = float(np.max(volume[g['start_idx']:last['end_idx'] + 1]))
        last['surge_max_vol'] = max(last['surge_max_vol'], g['surge_max_vol'])
        last_start_vol = last['surge_max_vol']

    start_idx = last['start_idx']

    # === MODULE 4: Upper shadow limit ===
    if p.shadow_limit_enabled:
        def is_long_upper_shadow(row):
            total_range = row['high'] - row['low']
            if total_range == 0:
                return False
            upper_shadow = row['high'] - max(row['close'], row['open'])
            return (upper_shadow / total_range) > p.shadow_upper_ratio

        long_shadow_count = sum(1 for i in range(start_idx, n)
                                if is_long_upper_shadow(df_asc.iloc[i]))
        shadow_limit = max(p.shadow_min_allowance,
                          (n - start_idx) // p.shadow_max_per_n_days)
        if long_shadow_count > shadow_limit:
            return _empty_wave_result()

    # === MODULE 3: Peak detection ===
    pre_idx = max(0, start_idx - 1)
    start_price = close[pre_idx] if pre_idx < start_idx else opens[start_idx]
    max_high_idx = start_idx
    max_close = start_price
    for i in range(start_idx, n):
        if close[i] > max_close:
            max_close = close[i]
            max_high_idx = i

    if p.peak_cut_enabled:
        for i in range(start_idx, max_high_idx):
            if i > 0:
                day_gain = (close[i] - close[i-1]) / close[i-1] * 100
                if day_gain <= p.peak_cut_loss_pct and volume[i] > avg_vol_5[i] * p.peak_cut_vol_ratio:
                    max_high_idx = i - 1
                    break

    end_price = close[max_high_idx]
    total_gain = (end_price / start_price - 1) * 100

    # === MODULE 5: Wave qualification ===
    surge_turnover_sum = 0.0
    if p.wave_qualification_enabled:
        for i in range(start_idx, max_high_idx + 1):
            if is_surge_day[i] and not np.isnan(turnover[i]) and turnover[i] > 0:
                surge_turnover_sum += turnover[i]

        period_df = df_asc.iloc[start_idx:max_high_idx + 1]
        positive_vol = period_df[period_df['close'] > period_df['open']]['volume'].sum()
        negative_vol = period_df[period_df['close'] < period_df['open']]['volume'].sum()
        red_green_ok = positive_vol > negative_vol * p.wave_red_green_vol_ratio

        gain_ok = total_gain <= p.wave_max_gain_pct
        turnover_ok = surge_turnover_sum <= p.wave_max_turnover_sum
        is_qualified = gain_ok and turnover_ok and red_green_ok
    else:
        red_green_ok = True
        is_qualified = True

    # === MODULE 6: Wave health ===
    wave_health_val = False
    surge_quality_score = 0.0
    if p.wave_health_enabled and start_idx > 0:
        pre_range = max(0, start_idx - p.wave_health_accum_days)
        pre_slice = df_asc.iloc[pre_range:start_idx]
        if len(pre_slice) >= p.wave_health_min_accum_bars:
            vol_base = pre_slice['volume'].median()
            vol_surge = volume[start_idx]
            if vol_base > 0 and vol_surge / vol_base > p.wave_health_surge_vol_ratio:
                wave_health_val = True

    # Surge quality 0-10 score derived from V1 analysis
    if is_qualified:
        surge_quality_score = 5.0  # base: qualified
        # Gain quality: 15-40% ideal
        if 15 <= total_gain <= 40:
            surge_quality_score += 3.0
        elif 40 < total_gain <= p.wave_max_gain_pct:
            surge_quality_score += 1.0
        # Turnover quality
        if surge_turnover_sum < p.wave_max_turnover_sum * 0.5:
            surge_quality_score += 1.0
        # Red/green quality
        if positive_vol > negative_vol * (p.wave_red_green_vol_ratio + 0.3):
            surge_quality_score += 1.0
        surge_quality_score = min(surge_quality_score, 10.0)

    # === MODULE 8: Wave break check ===
    wave_broken = False
    new_surge_idx = None
    if p.wave_break_enabled and start_idx is not None:
        wb_result = _check_wave_break(df_asc, start_idx, p)
        wave_broken = wb_result['broken']
        new_surge_idx = wb_result['new_surge_idx']

    # === MODULE 7: Volume-price ranking ===
    max_high_vol_rank = 0
    vol_resonance = 0.0
    has_limit_up = False
    has_shrink_limit_up = False
    has_one_word_limit = False

    if start_idx is not None:
        full_slice = df_asc.iloc[start_idx:]
        sorted_vols = sorted(full_slice['volume'].values, reverse=True)
        n_vols = len(sorted_vols)

        if p.vol_rank_enabled and n_vols >= 2:
            mh_idx_local = full_slice['high'].idxmax()
            mh_vol = df_asc.loc[mh_idx_local, 'volume']

            # Three-condition volume-price resonance (V1 original design)
            resonance_a = True   # max vol day is bullish
            resonance_b = True   # 2nd max vol day is bullish
            resonance_c = False  # max high day is bullish AND vol rank <= 2

            # A: Max vol must be bullish
            if p.vol_rank_max_must_bullish:
                max_vol_idx = full_slice['volume'].idxmax()
                if df_asc.loc[max_vol_idx, 'close'] <= df_asc.loc[max_vol_idx, 'open']:
                    resonance_a = False
                    is_qualified = False

            # B: 2nd max vol must be bullish
            if p.vol_rank_2nd_must_bullish and n_vols >= 2:
                second_vol = sorted_vols[1]
                second_mask = full_slice['volume'] == second_vol
                second_idx = full_slice[second_mask].index[0]
                if df_asc.loc[second_idx, 'close'] <= df_asc.loc[second_idx, 'open']:
                    resonance_b = False
                    if is_qualified:
                        is_qualified = False

            # C: Max high day bullish (gate) + vol rank (scoring quality)
            if p.vol_rank_high_price_must_bullish:
                if df_asc.loc[mh_idx_local, 'close'] > df_asc.loc[mh_idx_local, 'open']:
                    resonance_c = True  # max high is bullish = passes gate
                    threshold = sorted_vols[1] if n_vols >= 2 else sorted_vols[0]
                    if mh_vol >= sorted_vols[0]:
                        max_high_vol_rank = 1
                        vol_resonance = 1.0
                    elif mh_vol >= sorted_vols[1]:
                        max_high_vol_rank = 2
                        vol_resonance = 0.5
                    else:
                        max_high_vol_rank = 0
                        vol_resonance = 0.0  # bullish but low vol, weak resonance
                else:
                    resonance_c = False  # not bullish = fail gate
                if not resonance_c and is_qualified:
                        is_qualified = False

        # Limit-up detection (for V1 fusion scoring only)
        for i in range(start_idx, min(max_high_idx, n - 1) + 1):
            row = df_asc.iloc[i]
            if i > start_idx:
                day_gain = (close[i] - close[i-1]) / close[i-1] * 100
            else:
                day_gain = 0
            is_limit = (day_gain >= 9.8) or (
                (row['high'] - row['low']) / row['low'] * 100 < 0.1 and row['close'] == row['high'])
            if is_limit:
                has_limit_up = True
                if abs(row['open'] - row['close']) / row['open'] * 100 < 0.5 and \
                   abs(row['high'] - row['low']) / row['low'] * 100 < 0.5:
                    has_one_word_limit = True
                    has_shrink_limit_up = True
                elif row['volume'] < avg_vol_5[i] * 1:
                    has_shrink_limit_up = True

    return {
        'total_gain': round(total_gain, 2),
        'surge_turnover_sum': round(surge_turnover_sum, 2),
        'is_qualified': is_qualified,
        'wave_healthy': wave_health_val,
        'surge_quality': surge_quality_score,
        'has_limit_up': has_limit_up,
        'has_shrink_limit_up': has_shrink_limit_up,
        'has_one_word_limit': has_one_word_limit,
        'surge_start_idx': start_idx,
        'wave_broken': wave_broken,
        'new_surge_idx': new_surge_idx,
        'max_high_vol_rank': max_high_vol_rank,
        'vol_resonance': vol_resonance,
        'resonance_a': resonance_a if 'resonance_a' in dir() else True,
        'resonance_b': resonance_b if 'resonance_b' in dir() else True,
        'resonance_c': resonance_c if 'resonance_c' in dir() else False,
        'red_green_ok': red_green_ok if 'red_green_ok' in dir() else True,
        'positive_vol_sum': float(period_df[period_df['close'] > period_df['open']]['volume'].sum()) if 'period_df' in dir() else 0,
        'negative_vol_sum': float(period_df[period_df['close'] < period_df['open']]['volume'].sum()) if 'period_df' in dir() else 0,
    }


def _calc_retrace(vs_60d_high, total_gain):
    """Gradual retrace score. 1.0=ideal pullback, 0.5=too-shallow or too-deep."""
    if total_gain < 0.01:
        return 1.5
    retrace_pct = abs(vs_60d_high) / total_gain * 100
    if 20 <= retrace_pct <= 60:
        return min(3.0, retrace_pct / 20)  # gradual 1.0-3.0
    else:
        return 1.5


def _empty_wave_result():
    return {
        'total_gain': 0, 'surge_turnover_sum': 0, 'is_qualified': False,
        'wave_healthy': False, 'surge_quality': 0.0,
        'has_limit_up': False, 'has_shrink_limit_up': False, 'has_one_word_limit': False,
        'surge_start_idx': None, 'wave_broken': False, 'new_surge_idx': None,
        'max_high_vol_rank': 0, 'vol_resonance': 0.0,
        'red_green_ok': True, 'positive_vol_sum': 0, 'negative_vol_sum': 0,
    }


def _check_wave_break(df_asc: pd.DataFrame, surge_start_idx: int, p: B1V3Params) -> dict:
    """MODULE 8: detect if wave has been broken (from V1)."""
    if surge_start_idx is None:
        return {'broken': False, 'new_surge_idx': None}

    n = len(df_asc)
    if 'yellow_line' not in df_asc.columns:
        return {'broken': False, 'new_surge_idx': None}

    close = df_asc['close'].values
    low = df_asc['low'].values
    yellow = df_asc['yellow_line'].values
    volume = df_asc['volume'].values
    opens = df_asc['open'].values

    for i in range(surge_start_idx + 1, n):
        if np.isnan(yellow[i]):
            continue
        if close[i] < yellow[i]:
            stop_ref = low[i] - p.wave_break_stop_width
            broke_stop = False
            break_day = None
            for j in range(i + 1, n):
                if close[j] < stop_ref:
                    broke_stop = True
                    break_day = j
                    break
            if not broke_stop:
                return {'broken': False, 'new_surge_idx': None}

            # Check for new surge after break
            after_break = df_asc.iloc[break_day:]
            if len(after_break) < 5:
                return {'broken': True, 'new_surge_idx': None}

            avg_vol_5_ab = after_break['volume'].rolling(5, min_periods=1).mean().shift(1).values
            pct_chg_ab = after_break['close'].pct_change().values * 100
            surge_ab = (pct_chg_ab >= p.surge_min_gain_pct) & \
                       (after_break['close'].values > after_break['open'].values if p.surge_require_positive else True) & \
                       (after_break['volume'].values > avg_vol_5_ab * p.surge_vol_vs_ma5)
            surge_indices_ab = np.where(surge_ab)[0].tolist()

            if surge_indices_ab:
                # Find start of new surge group
                new_idx_raw = surge_indices_ab[0]
                start = new_idx_raw
                gap = 0
                for k in range(new_idx_raw - 1, -1, -1):
                    if k < len(surge_ab) and surge_ab[k]:
                        if gap <= p.wave_break_new_surge_gap_max:
                            start = k
                            gap = 0
                        else:
                            break
                    else:
                        gap += 1
                abs_start = int(after_break.index[start])
                return {'broken': True, 'new_surge_idx': abs_start}
            else:
                return {'broken': True, 'new_surge_idx': None}

    return {'broken': False, 'new_surge_idx': None}


def detect_washout(df: pd.DataFrame, surge_start_date, p: B1V3Params) -> bool:
    """MODULE 10: Washout exception (from V1)."""
    if not p.washout_enabled:
        return False
    if len(df) < 40:
        return False

    df_asc = df.sort_values('date').reset_index(drop=True)
    if 'yellow_line' not in df_asc.columns:
        return False

    close = df_asc['close'].values
    low = df_asc['low'].values
    yellow = df_asc['yellow_line'].values
    volume = df_asc['volume'].values
    n = len(df_asc)

    latest_close = close[-1]
    if latest_close >= yellow[-1]:
        return False  # Not below yellow, no washout

    # Find the most recent day where close broke below yellow
    break_idx = None
    for i in range(n - 1, max(0, n - 20) - 1, -1):
        if not np.isnan(yellow[i]) and close[i] < yellow[i]:
            break_idx = i
            break
    if break_idx is None:
        return False

    # Must be within max days
    if (n - 1 - break_idx) > p.washout_max_days_since_break:
        return False

    # Volume must be shrinking on break day (if required)
    if p.washout_vol_must_shrink and break_idx >= 5:
        break_vol = volume[break_idx]
        avg_vol_prior = np.mean(volume[max(0, break_idx-5):break_idx])
        if avg_vol_prior > 0 and break_vol > avg_vol_prior * 1.2:
            return False

    # Stop loss not hit since break (if required)
    if p.washout_stop_not_hit:
        stop_ref = low[break_idx] - p.wave_break_stop_width
        for j in range(break_idx + 1, n):
            if close[j] < stop_ref:
                return False

    return True


def detect_super_b1(df: pd.DataFrame, surge_start_date, p: B1V3Params) -> bool:
    """MODULE 11: Super B1 detection (from V1)."""
    if len(df) < 40:
        return False

    asc = df.sort_values('date').reset_index(drop=True)
    n = len(asc)

    if surge_start_date is None:
        return False
    surge_dt = pd.to_datetime(surge_start_date)
    mask = asc['date'] >= surge_dt
    if mask.sum() < 5:
        return False

    start_idx = mask.idxmax()
    recent = asc.iloc[start_idx:]
    b1_indices = []
    for i in range(1, len(recent)):
        row = recent.iloc[i]
        j_ok = p.j_min <= row['J'] <= p.j_max
        if j_ok and row['white_gt_yellow'] and row['volume_shrink']:
            b1_indices.append(recent.index[i])

    if not b1_indices:
        return False

    last_b1_idx = b1_indices[-1]
    after_b1 = asc.loc[last_b1_idx:]
    if not (after_b1['J'] >= p.super_b1_j_rebound_min).any():
        return False

    cur_j = asc.iloc[-1]['J']
    if cur_j >= p.super_b1_j_current_max:
        return False

    # Slope flatten check
    if p.super_b1_slope_flatten and n >= 3:
        j0 = asc.iloc[-1]['J']
        j1 = asc.iloc[-2]['J']
        j2 = asc.iloc[-3]['J']
        if (j0 - j1) < (j1 - j2):
            return False

    b1_price = asc.loc[last_b1_idx, 'close']
    cur_price = asc.iloc[-1]['close']
    return abs(cur_price / b1_price - 1) * 100 < p.super_b1_cost_distance_pct


# ============================================================
# LAYER 0: SIGNAL EXTRACTION
# ============================================================

def extract_signals_single(code, start_date, end_date, p: B1V3Params):
    """Extract all B1 candidate signals for a single stock."""
    cache_path = INDICATORS_DIR / f"{code}.parquet"
    if not cache_path.exists():
        return []

    df = pd.read_parquet(cache_path)
    if df.empty or len(df) < 61:
        return []

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    mask = df['date'] >= pd.Timestamp(start_date)
    start_idx = df[mask].index[0] if mask.any() else len(df)
    eval_df = df.iloc[max(0, start_idx - p.prefilter_lookback_days):].reset_index(drop=True)
    n = len(eval_df)

    if n < p.prefilter_lookback_days + 1:
        return []

    # Extract arrays
    dates = eval_df['date'].values
    closes = eval_df['close'].values; opens = eval_df['open'].values
    highs = eval_df['high'].values; lows = eval_df['low'].values
    whites = eval_df['white_line'].values; yellows = eval_df['yellow_line'].values
    Js = eval_df['J'].values; Ks = eval_df['K'].values; Ds = eval_df['D'].values
    DIFs = eval_df['DIF'].values; DEAs = eval_df['DEA'].values
    vols = eval_df['volume'].values
    caps = eval_df.get('market_cap', pd.Series([np.nan]*n)).values
    turnovers = eval_df.get('turnover', pd.Series([np.nan]*n)).values
    doubled = eval_df.get('doubled', pd.Series([False]*n)).values
    MA5s = eval_df.get('MA5', pd.Series([np.nan]*n)).values
    MA10s = eval_df.get('MA10', pd.Series([np.nan]*n)).values
    MA20s = eval_df.get('MA20', pd.Series([np.nan]*n)).values
    MA30s = eval_df.get('MA30', pd.Series([np.nan]*n)).values
    MA40s = eval_df.get('MA40', pd.Series([np.nan]*n)).values
    amount = eval_df.get('amount', pd.Series([0]*n)).values

    candidates = []
    for i in range(p.prefilter_lookback_days, n):
        # A1: white > yellow
        if p.require_white_gt_yellow and whites[i] <= yellows[i]:
            continue
        if yellows[i] <= 0 or whites[i] <= 0:
            continue

        # A2: DIF > min
        if DIFs[i] < p.dif_min:
            continue

        # A3: no double in 60d
        if p.require_no_double_60d and doubled[i]:
            continue

        # A4: market cap
        cap = caps[i]
        if pd.notna(cap) and cap > 0 and cap < p.cap_min:
            continue

        # Prefilter: wide J + K<D
        if Js[i] >= p.prefilter_j_max:
            continue
        if p.prefilter_k_lt_d and Ks[i] >= Ds[i]:
            continue

        # Must be within target date range
        if eval_df['date'].iloc[i] < pd.Timestamp(start_date):
            continue
        if eval_df['date'].iloc[i] > pd.Timestamp(end_date):
            continue

        # === V1 Wave analysis ===
        wave = analyze_wave(eval_df.iloc[:i+1].copy(), p)

        # Apply wave result checks
        if p.require_wave_qualified and not wave['is_qualified']:
            continue
        if p.require_wave_healthy and not wave['wave_healthy']:
            continue

        start_idx_wave = wave['surge_start_idx']
        if p.require_no_wave_break and start_idx_wave is not None:
            if wave['wave_broken']:
                if wave['new_surge_idx'] is None:
                    continue
                start_idx_wave = wave['new_surge_idx']

        # A5: Bowl position
        latest = eval_df.iloc[i]
        vs_white = (closes[i] / whites[i] - 1) * 100
        vs_yellow = (closes[i] / yellows[i] - 1) * 100

        if p.position_mode == "bowl":
            fall_in_bowl = (closes[i] >= yellows[i]) and (closes[i] <= whites[i])
            near_pct_val = p.bowl_near_pct / 100.0
            near_white = abs(closes[i] - whites[i]) / whites[i] <= near_pct_val
            position_ok = fall_in_bowl or near_white
        elif p.position_mode == "vs_white":
            position_ok = (p.vs_white_low <= vs_white <= p.vs_white_high) and \
                         (vs_yellow >= p.vs_yellow_min)
        else:  # "both"
            fall_in_bowl = (closes[i] >= yellows[i]) and (closes[i] <= whites[i])
            near_pct_val = p.bowl_near_pct / 100.0
            near_white = abs(closes[i] - whites[i]) / whites[i] <= near_pct_val
            bowl_ok = fall_in_bowl or near_white
            vs_ok = (p.vs_white_low <= vs_white <= p.vs_white_high) and \
                    (vs_yellow >= p.vs_yellow_min)
            position_ok = bowl_ok and vs_ok

        # Washout channel
        surge_start_date = None
        if start_idx_wave is not None and start_idx_wave < len(eval_df):
            surge_start_date = eval_df.iloc[start_idx_wave]['date']

        is_washout = detect_washout(eval_df.iloc[:i+1].copy().sort_values('date').reset_index(drop=True),
                                    surge_start_date, p)

        if not position_ok and not is_washout:
            continue

        # S1 distribution filter (from V1, detect smart-money distribution)
        has_s1 = False
        s1_type = None
        if p.require_no_s1:
            from utils.s1_filter import detect_s1_signal
            df_desc = eval_df.iloc[:i+1].copy().sort_values('date', ascending=False).reset_index(drop=True)
            has_s1, _, s1_type = detect_s1_signal(df_desc, surge_start_date)
            if has_s1:
                continue

        # B1: J threshold
        j_val = Js[i]
        if j_val >= p.j_max or j_val <= p.j_min:
            continue

        # B2/B3: Volume shrink
        # V1 style: vol / 20d max from wave start
        vol_vs_peak_ok = True
        vol_vs_20d_peak = 1.0
        if start_idx_wave is not None and start_idx_wave < n:
            vol_window = vols[max(0, start_idx_wave):i+1]
            vol_peak = np.max(vol_window[-p.vol_peak_lookback:]) if len(vol_window) >= 1 else vols[i]
            vol_vs_20d_peak = vols[i] / vol_peak if vol_peak > 0 else 1.0
            vol_vs_peak_ok = vol_vs_20d_peak <= p.vol_vs_wave_peak_max

        # V2 style: vol / 5d avg
        vol_ma5 = np.mean(vols[max(0, i-4):i+1]) if i >= 4 else vols[i]
        vol_ratio_ma5 = vols[i] / vol_ma5 if vol_ma5 > 0 else 1.0
        vol_ma5_ok = vol_ratio_ma5 <= p.vol_ratio_ma5_max

        if p.vol_shrink_mode == "v1" and not vol_vs_peak_ok:
            continue
        elif p.vol_shrink_mode == "v2" and not vol_ma5_ok:
            continue
        elif p.vol_shrink_mode == "both" and not (vol_vs_peak_ok and vol_ma5_ok):
            continue
        elif p.vol_shrink_mode == "either" and not (vol_vs_peak_ok or vol_ma5_ok):
            continue

        # B4: Turnover
        t = turnovers[i] if pd.notna(turnovers[i]) else -1
        if t > p.turnover_max:
            continue

        # B5: ret_5d
        ret_5d = (closes[i] / closes[i-5] - 1) * 100 if i >= 5 and closes[i-5] > 0 else 0
        if ret_5d < p.ret_5d_min:
            continue

        # B6: white slope
        w_slope_5 = (whites[i] / whites[i-5] - 1) * 100 if i >= 5 and whites[i-5] > 0 else 0
        if w_slope_5 < p.white_slope_min:
            continue

        # B7/B8: PE/PB
        pe_val, pb_val = -1.0, -1.0
        pc = _load_pe_cache()
        if pc and str(code) in pc:
            df_pe = pc[str(code)]
            sig_dt = pd.Timestamp(dates[i]).strftime('%Y-%m-%d')
            m_pe = df_pe['date'] == sig_dt
            if m_pe.any():
                row = df_pe[m_pe].iloc[0]
                pe_val = row['peTTM'] if pd.notna(row['peTTM']) else -1.0
                pb_val = row['pbMRQ'] if pd.notna(row['pbMRQ']) else -1.0

        if pe_val > p.pe_max:
            continue
        if pb_val > p.pb_max:
            continue

        # B9: CS shadow max (applied later in cross-section)
        # C: Optional hard filters
        # K<D duration
        k_bd = 0
        for j in range(i, max(0, i-30)-1, -1):
            if Ks[j] < Ds[j]:
                k_bd += 1
            else:
                break
        if p.require_k_lt_d_days > 0 and k_bd < p.require_k_lt_d_days:
            continue

        # C8-C17: more optional gates computed below in features section
        # (deferred to compute_features + apply_gates)

        # Super B1 marker (respects p.super_b1_marker gate)
        is_super_b1 = False
        if p.super_b1_marker and surge_start_date:
            is_super_b1 = detect_super_b1(eval_df.iloc[:i+1].copy(), surge_start_date, p)

        # === COMPUTE ALL FEATURES ===
        feat = _compute_features(eval_df, i, code, dates, closes, opens, highs, lows,
                                 whites, yellows, Js, Ks, Ds, DIFs, DEAs,
                                 vols, caps, turnovers, MA5s, MA10s, MA20s, MA30s, MA40s,
                                 amount, pe_val, pb_val, wave, p)
        feat['date'] = dates[i]
        feat['code'] = code
        feat['close'] = closes[i]
        feat['low'] = lows[i]
        feat['high'] = highs[i]
        feat['open'] = opens[i]
        feat['white_line'] = whites[i]
        feat['yellow_line'] = yellows[i]
        feat['volume'] = vols[i]
        feat['turnover'] = t
        feat['market_cap'] = cap if pd.notna(cap) else -1
        feat['is_washout'] = is_washout
        feat['is_super_b1'] = is_super_b1
        feat['has_s1'] = has_s1
        feat['s1_type'] = s1_type
        feat['surge_start_date'] = surge_start_date
        feat['wave'] = wave
        feat['pe'] = pe_val
        feat['pb'] = pb_val

        # Apply C optional gates
        if not _pass_optional_gates(feat, p):
            continue

        # DTW pattern similarity (V1 fusion, only when enabled)
        if p.q_pattern_sim:
            from strategy.b1_v3_dtw_fusion import compute_similarity
            df_desc = eval_df.iloc[:i+1].copy().sort_values('date', ascending=False).reset_index(drop=True)
            best_score, best_case, all_scores = compute_similarity(df_desc)
            feat['pattern_similarity'] = best_score
            feat['best_case'] = best_case
            feat.update(all_scores)  # store per-case scores: case_sim_case_001, etc.

        candidates.append(feat)

    return candidates


def _compute_features(eval_df, i, code, dates, closes, opens, highs, lows,
                      whites, yellows, Js, Ks, Ds, DIFs, DEAs,
                      vols, caps, turnovers, MA5s, MA10s, MA20s, MA30s, MA40s,
                      amount, pe_val, pb_val, wave, p):
    """Compute all feature values for a single signal day."""
    f = {}

    vs_white = (closes[i] / whites[i] - 1) * 100
    vs_yellow = (closes[i] / yellows[i] - 1) * 100

    # K<D duration
    k_bd = 0
    for j in range(i, max(0, i-30)-1, -1):
        if Ks[j] < Ds[j]: k_bd += 1
        else: break

    # Volume ratios
    vol_ma5 = np.mean(vols[max(0, i-4):i+1]) if i >= 4 else vols[i]
    vol_ratio_ma5 = vols[i] / vol_ma5 if vol_ma5 > 0 else 1.0
    w_slope_5 = (whites[i] / whites[i-5] - 1) * 100 if i >= 5 and whites[i-5] > 0 else 0
    ret_5d = (closes[i] / closes[i-5] - 1) * 100 if i >= 5 and closes[i-5] > 0 else 0

    # Candle
    candle_body = (closes[i] / opens[i] - 1) * 100
    lower_shadow = (min(opens[i], closes[i]) - lows[i]) / closes[i] * 100
    upper_shadow = (highs[i] - max(opens[i], closes[i])) / closes[i] * 100

    # Bar quality (Qlib)
    bar_range = max(highs[i] - lows[i], 0.001)
    qlib_klow2 = (min(opens[i], closes[i]) - lows[i]) / bar_range
    qlib_kup2 = (highs[i] - max(opens[i], closes[i])) / bar_range
    qlib_kmid2 = (closes[i] - opens[i]) / bar_range
    qlib_klen = bar_range / closes[i] * 100
    qlib_ksft2 = (2*closes[i] - highs[i] - lows[i]) / bar_range if bar_range > 0 else 0

    # MA alignment
    ma_10_gt_20 = 1 if pd.notna(MA10s[i]) and pd.notna(MA20s[i]) and MA10s[i] > MA20s[i] else 0
    ma_20_gt_30 = 1 if pd.notna(MA20s[i]) and pd.notna(MA30s[i]) and MA20s[i] > MA30s[i] else 0
    ma_30_gt_40 = 1 if pd.notna(MA30s[i]) and pd.notna(MA40s[i]) and MA30s[i] > MA40s[i] else 0
    ma_5_lt_10 = 1 if pd.notna(MA5s[i]) and pd.notna(MA10s[i]) and MA5s[i] < MA10s[i] else 0

    dif_gt_dea = 1 if DIFs[i] > DEAs[i] else 0
    high_60d = highs[max(0, i-60):i+1].max() if i >= 0 else highs[i]
    vs_60d_high = (closes[i] / high_60d - 1) * 100 if high_60d > 0 else 0

    # Volatility
    rets_10d = np.diff(closes[max(0, i-10):i+1]) / closes[max(0, i-10):i] * 100
    vol_10d = np.std(rets_10d) if len(rets_10d) > 1 else 0

    # DIF bull divergence
    dif_bull_div = 0
    if i >= 10:
        close_10d = closes[max(0, i-10):i]
        dif_10d = DIFs[max(0, i-10):i]
        close_10d_low = close_10d.min()
        dif_at_close_low_idx_idx = close_10d.argmin()
        if closes[i] < close_10d_low * 1.03 and DIFs[i] > dif_10d[dif_at_close_low_idx_idx]:
            dif_bull_div = 1

    # MA structure
    ma_structure_bull = 0
    if pd.notna(MA5s[i]) and pd.notna(MA10s[i]) and pd.notna(MA20s[i]) and pd.notna(MA30s[i]):
        if MA5s[i] < MA10s[i] and MA10s[i] > MA20s[i] and MA20s[i] > MA30s[i]:
            ma_structure_bull = 1

    # J bouncing
    j_bouncing = 0
    if i >= 10:
        j_min_10d = Js[max(0, i-10):i].min()
        if j_min_10d < 0 and Js[i] > 0 and Js[i] < 15:
            j_bouncing = 1

    # Near MA20
    near_ma20 = 0
    if pd.notna(MA20s[i]) and MA20s[i] > 0:
        if abs(closes[i] / MA20s[i] - 1) * 100 < 3:
            near_ma20 = 1

    # Green candle
    is_green = 1 if closes[i] > opens[i] else 0

    # Momentum improving
    mom_improving = 0
    if i >= 10 and closes[i-10] > 0:
        r5 = (closes[i] / closes[i-5] - 1) * 100 if closes[i-5] > 0 else 0
        r10 = (closes[i] / closes[i-10] - 1) * 100
        if r5 < 0 and r10 < 0 and r5 > r10:
            mom_improving = 1

    # MA5/10 tight
    ma5_10_tight = 0
    if pd.notna(MA5s[i]) and pd.notna(MA10s[i]) and MA10s[i] > 0:
        if abs(MA5s[i] / MA10s[i] - 1) * 100 < 2:
            ma5_10_tight = 1

    # Moderate oversold
    moderate_oversold = 0
    if 0 <= Js[i] <= 10 and DIFs[i] > 0.5:
        moderate_oversold = 1

    # PB last green (previous day green = pullback ending with strength)
    pb_last_green = 0
    if i >= 2 and closes[i-1] > opens[i-1]:
        pb_last_green = 1

    # Vol contracting
    vol_contracting = 0
    if i >= 40:
        rets_10 = np.diff(closes[max(0, i-10):i+1]) / closes[max(0, i-10):i] * 100
        rets_40 = np.diff(closes[max(0, i-40):i+1]) / closes[max(0, i-40):i] * 100
        vol10 = np.std(rets_10) if len(rets_10) > 1 else 999
        vol40 = np.std(rets_40) if len(rets_40) > 1 else 1
        if vol40 > 0 and vol10 / vol40 < 0.8:
            vol_contracting = 1

    # Vol shrinking recent
    vol_shrinking_recent = 0
    if i >= 10:
        vol_3d = np.mean(vols[i-2:i+1])
        vol_10d_avg = np.mean(vols[i-9:i+1])
        if vol_10d_avg > 0 and vol_3d < vol_10d_avg * 0.8:
            vol_shrinking_recent = 1

    # No distribution 10d
    no_dist_10d = 1
    dist_check_start = max(0, i - 10)
    for j in range(dist_check_start, i):
        if j >= 0:
            up_shadow_val = (highs[j] - max(opens[j], closes[j])) / closes[j] * 100 if closes[j] > 0 else 0
            vol_j = vols[j]
            vol_ma_j = np.mean(vols[max(0, j-4):j+1])
            if up_shadow_val > 3 and vol_j > vol_ma_j * 1.5:
                no_dist_10d = 0
                break

    # Vol vs 20d peak
    vol_vs_20d_peak = 1.0
    if i >= 20:
        vol_peak_20d = vols[max(0, i-20):i+1].max()
        vol_vs_20d_peak = vols[i] / vol_peak_20d if vol_peak_20d > 0 else 1.0

    # DIF momentum
    dif_momentum = DIFs[i] - DIFs[i-5] if i >= 5 else 0.0

    # GTJA-inspired
    vol_price_corr = 0.0
    if i >= 6:
        dlogvol = np.diff(np.log(np.maximum(vols[i-6:i+1], 1)))
        intra_ret = (closes[i-6:i+1] - opens[i-6:i+1]) / np.maximum(opens[i-6:i+1], 0.01)
        if len(dlogvol) >= 5 and np.std(dlogvol) > 1e-10 and np.std(intra_ret[1:]) > 1e-10:
            vol_price_corr = np.corrcoef(dlogvol, intra_ret[1:])[0, 1]
    feat_vol_price_improving = 1 if vol_price_corr > -0.3 else 0

    anti_dist_score = 0.0
    if i >= 5:
        h_rank = np.argsort(np.argsort(highs[i-4:i+1]))
        v_rank = np.argsort(np.argsort(vols[i-4:i+1]))
        h_rank = h_rank.astype(float); v_rank = v_rank.astype(float)
        cov_val = np.cov(h_rank, v_rank)[0, 1] if np.std(h_rank) > 0 and np.std(v_rank) > 0 else 0
        anti_dist_score = -cov_val
    feat_anti_dist = 1 if anti_dist_score > 0 else 0

    vwap_dist_score = 0.0
    if i >= 10 and amount[i] > 0:
        vwap_10d = np.sum(amount[i-9:i+1]) / np.sum(vols[i-9:i+1]) if np.sum(vols[i-9:i+1]) > 0 else closes[i]
        if vwap_10d > 0:
            open_gap = (opens[i] / vwap_10d - 1) * 100
            close_gap = abs(closes[i] / vwap_10d - 1) * 100
            if open_gap < 0 and close_gap < abs(open_gap):
                vwap_dist_score = min(1.0, abs(open_gap) / 5.0)

    net_up_days = 0
    if i >= 10:
        for j in range(i-9, i+1):
            if closes[j] > opens[j]: net_up_days += 1
            else: net_up_days -= 1
    feat_net_up_positive = 1 if net_up_days > -2 else 0

    # RSI(14)
    rsi_14 = 50.0
    if i >= 14:
        gains = [max(closes[j]-closes[j-1], 0) for j in range(i-13, i+1)]
        losses = [max(closes[j-1]-closes[j], 0) for j in range(i-13, i+1)]
        avg_gain = sum(gains) / 14; avg_loss = sum(losses) / 14
        rsi_14 = 100 - 100/(1 + avg_gain/avg_loss) if avg_loss > 0 else 100

    # ATR(14)
    atr_14 = 0.0
    if i >= 14:
        trs = [max(highs[j]-lows[j], abs(highs[j]-closes[j-1]), abs(lows[j]-closes[j-1]))
               for j in range(i-13, i+1)]
        atr_14 = sum(trs) / 14 / closes[i] * 100

    # Up/Down vol ratio
    up_vol_ratio = 0.5
    if i >= 20:
        rets = [(closes[j]/closes[j-1]-1)*100 for j in range(i-19, i+1)]
        up_rets = [r for r in rets if r > 0]; dn_rets = [abs(r) for r in rets if r < 0]
        up_std = np.std(up_rets) if len(up_rets) > 2 else 0
        dn_std = np.std(dn_rets) if len(dn_rets) > 2 else 0
        up_vol_ratio = up_std / (up_std + dn_std) if (up_std + dn_std) > 0 else 0.5

    # OBV divergence
    obv_div = 0
    if i >= 10:
        obv_now = sum(vols[j] * (1 if closes[j] > closes[j-1] else -1) for j in range(i-9, i+1))
        obv_prev = sum(vols[j] * (1 if closes[j] > closes[j-1] else -1) for j in range(i-19, i-9))
        obv_div = 1 if (closes[i] < closes[i-10] and obv_now > obv_prev) else 0

    # Ret-vol efficiency
    ret_vol_eff = 0.0
    if i >= 10:
        abs_rets = [abs((closes[j]/closes[j-1]-1)*100) for j in range(i-9, i+1)]
        avg_abs_ret = np.mean(abs_rets)
        avg_vol_10d = np.mean(vols[i-9:i+1])
        ret_vol_eff = avg_abs_ret / (avg_vol_10d / 1e6) if avg_vol_10d > 0 else 99

    # Mom acceleration
    mom_accel = 0.0
    if i >= 15:
        r5 = (closes[i]/closes[i-5]-1)*100 if closes[i-5] > 0 else 0
        r10 = (closes[i]/closes[i-10]-1)*100 if closes[i-10] > 0 else 0
        r15 = (closes[i]/closes[i-15]-1)*100 if closes[i-15] > 0 else 0
        mom_accel = (r5 - r10) - (r10 - r15)

    # Up day vol share
    up_day_vol_share = 0.5
    if i >= 20:
        up_vol = sum(vols[j] for j in range(i-19, i+1) if closes[j] > opens[j])
        total_vol = sum(vols[j] for j in range(i-19, i+1))
        up_day_vol_share = up_vol / total_vol if total_vol > 0 else 0.5

    # Vol dec days
    vol_dec_days = 0
    if i >= 5:
        for j in range(i, max(0, i-5), -1):
            if j > max(0, i-5) and vols[j] < vols[j-1]:
                vol_dec_days += 1
            else:
                break

    # Close to low
    close_to_low = (closes[i] - lows[i]) / max(highs[i] - lows[i], 0.001)

    # White-yellow gap (FIXED: previously never computed)
    white_yellow_gap = (whites[i] / yellows[i] - 1) * 100 if yellows[i] > 0 else 0

    # ---- NEW Tier1 factors ----
    # WR (Williams %R)
    wr_val = 50.0
    if i >= 14:
        high_14d = highs[max(0, i-13):i+1].max()
        low_14d = lows[max(0, i-13):i+1].min()
        wr_val = (high_14d - closes[i]) / max(high_14d - low_14d, 0.001) * 100

    # BIAS (20-day)
    bias_20 = 0.0
    if pd.notna(MA20s[i]) and MA20s[i] > 0:
        bias_20 = (closes[i] / MA20s[i] - 1) * 100

    # Bollinger %B
    bb_pct = 0.5
    if i >= 20:
        ma20_window = closes[max(0, i-19):i+1]
        ma20_val = np.mean(ma20_window)
        std20 = np.std(ma20_window)
        bb_upper = ma20_val + 2 * std20
        bb_lower = ma20_val - 2 * std20
        if bb_upper - bb_lower > 0:
            bb_pct = (closes[i] - bb_lower) / (bb_upper - bb_lower)

    # Volume lowest in N days
    vol_lowest_20d = 0
    if i >= 20:
        vol_20d_min = vols[max(0, i-19):i+1].min()
        vol_lowest_20d = 1 if vols[i] <= vol_20d_min * 1.01 else 0

    # Pullback green ratio
    pb_green_ratio = 0.0
    max_dd_day = 0.0
    if wave and wave['surge_start_idx'] is not None:
        w_start = wave['surge_start_idx']
        if i > w_start + 1:
            pb_slice = eval_df.iloc[w_start:i]
            if len(pb_slice) > 0:
                green_count = (pb_slice['close'].values > pb_slice['open'].values).sum()
                pb_green_ratio = green_count / len(pb_slice)
                # Max single-day drawdown during pullback
                pb_rets = pb_slice['close'].pct_change().values * 100
                pb_rets = pb_rets[~np.isnan(pb_rets)]
                max_dd_day = pb_rets.min() if len(pb_rets) > 0 else 0.0

    # Red volume declining
    red_vol_dec = 0.0
    if wave and wave['surge_start_idx'] is not None:
        w_start = wave['surge_start_idx']
        if i > w_start + 3:
            pb_slice = eval_df.iloc[w_start:i]
            red_days = pb_slice[pb_slice['close'] < pb_slice['open']]
            if len(red_days) >= 3:
                red_vols = red_days['volume'].tail(3).values
                if len(red_vols) >= 3 and red_vols[0] > 0:
                    slope = np.polyfit(range(len(red_vols)), red_vols, 1)[0]
                    red_vol_dec = 1.0 if slope < 0 else 0.0

    # Vol deceleration (2nd derivative of volume)
    vol_dec_accel = 0.0
    if i >= 8:
        vol_4day_avg = [np.mean(vols[j-3:j+1]) for j in range(i-4, i+1)]
        if len(vol_4day_avg) >= 4:
            first_deriv = np.diff(vol_4day_avg)
            second_deriv = np.diff(first_deriv)
            vol_dec_accel = -np.mean(second_deriv)  # positive = accelerating decline

    # Yellow line slope
    yellow_slope_5d = (yellows[i] / yellows[i-5] - 1) * 100 if i >= 5 and yellows[i-5] > 0 else 0

    # ADX (simplified)
    adx_14 = 0.0
    if i >= 28:
        trs_adx = [max(highs[j]-lows[j], abs(highs[j]-closes[j-1]), abs(lows[j]-closes[j-1]))
                   for j in range(i-27, i+1)]
        atr_adx = np.mean(trs_adx[-14:])
        up_move = [highs[j]-highs[j-1] if highs[j] > highs[j-1] else 0 for j in range(i-27, i+1)]
        dn_move = [lows[j-1]-lows[j] if lows[j] < lows[j-1] else 0 for j in range(i-27, i+1)]
        plus_di = np.mean(up_move[-14:]) / atr_adx * 100 if atr_adx > 0 else 0
        minus_di = np.mean(dn_move[-14:]) / atr_adx * 100 if atr_adx > 0 else 0
        dx = abs(plus_di - minus_di) / max(plus_di + minus_di, 0.01) * 100
        adx_14 = dx

    # RSI turn (5d change)
    rsi_5d_change = 0.0
    if i >= 19:
        gains_p = [max(closes[j]-closes[j-1], 0) for j in range(i-18, i+1)]
        losses_p = [max(closes[j-1]-closes[j], 0) for j in range(i-18, i+1)]
        avg_gain_p = sum(gains_p) / 14; avg_loss_p = sum(losses_p) / 14
        rsi_prev = 100 - 100/(1 + avg_gain_p/avg_loss_p) if avg_loss_p > 0 else 100
        rsi_5d_change = rsi_14 - rsi_prev

    # Distance to recent low (for risk/reward)
    dist_to_low = 0.0
    if i >= 20:
        low_20d = lows[max(0, i-19):i+1].min()
        dist_to_low = (closes[i] / low_20d - 1) * 100 if low_20d > 0 else 0

    # Hard filter binary features
    feat_vol_dec_3 = 1 if vol_dec_days >= 3 else 0
    feat_small_body = 1 if abs(candle_body) < 3 else 0
    feat_small_us = 1 if upper_shadow < 2 else 0
    feat_upvol_share = 1 if up_day_vol_share > 0.5 else 0
    feat_close_high = 1 if close_to_low > 0.5 else 0

    # === Build feature dict ===
    f.update({
        'J': Js[i], 'K': Ks[i], 'D': Ds[i],
        'DIF': DIFs[i], 'DEA': DEAs[i],
        'vs_white': vs_white, 'vs_yellow': vs_yellow,
        'ret_5d': ret_5d,
        'k_lt_d_days': k_bd,
        'vol_ratio_ma5': vol_ratio_ma5,
        'white_slope_5d': w_slope_5,
        'candle_body': candle_body, 'lower_shadow': lower_shadow, 'upper_shadow': upper_shadow,
        'qlib_klow2': qlib_klow2, 'qlib_kup2': qlib_kup2,
        'qlib_kmid2': qlib_kmid2, 'qlib_ksft2': qlib_ksft2,
        'qlib_klen': qlib_klen,
        'ma_10_gt_20': ma_10_gt_20, 'ma_20_gt_30': ma_20_gt_30,
        'ma_30_gt_40': ma_30_gt_40, 'ma_5_lt_10': ma_5_lt_10,
        'dif_gt_dea': dif_gt_dea,
        'vs_60d_high': vs_60d_high,
        # Raw MA values for threshold sweeping
        'MA5_raw': MA5s[i] if pd.notna(MA5s[i]) else 0,
        'MA10_raw': MA10s[i] if pd.notna(MA10s[i]) else 0,
        'MA20_raw': MA20s[i] if pd.notna(MA20s[i]) else 0,
        'near_ma20_dist': abs(closes[i] / max(MA20s[i], 0.01) - 1) * 100 if pd.notna(MA20s[i]) and MA20s[i] > 0 else 999,
        'volatility_10d': vol_10d,
        # Surge/retrace
        'surge_quality': wave['surge_quality'],
        'surge_max_gain': wave['total_gain'],
        'retrace_score': _calc_retrace(vs_60d_high, wave['total_gain']),
        'no_dist_10d': no_dist_10d,
        # New dimensions
        'dif_bull_div': dif_bull_div,
        'ma_structure_bull': ma_structure_bull,
        'vol_vs_20d_peak': vol_vs_20d_peak,
        'dif_momentum': dif_momentum,
        'j_bouncing': j_bouncing,
        'near_ma20': near_ma20,
        'is_green': is_green,
        'mom_improving': mom_improving,
        'ma5_10_tight': ma5_10_tight,
        'vol_shrinking_recent': vol_shrinking_recent,
        'vol_contracting': vol_contracting,
        'moderate_oversold': moderate_oversold,
        'pb_last_green': pb_last_green,
        # GTJA
        'vol_price_improving': feat_vol_price_improving,
        'anti_dist': feat_anti_dist,
        'vwap_mean_revert': vwap_dist_score,
        'net_up_positive': feat_net_up_positive,
        'rsi_14': rsi_14, 'atr_14': atr_14, 'up_vol_ratio': up_vol_ratio, 'obv_div': obv_div,
        'ret_vol_eff': ret_vol_eff, 'mom_accel': mom_accel,
        'up_day_vol_share': up_day_vol_share, 'vol_dec_days': vol_dec_days,
        'close_to_low': close_to_low,
        'vol_dec_3': feat_vol_dec_3, 'small_body': feat_small_body,
        'small_us': feat_small_us, 'upvol_share_h': feat_upvol_share,
        'close_high': feat_close_high,
        # FIXED: white_yellow_gap (was missing)
        'white_yellow_gap': white_yellow_gap,
        # NEW Tier1
        'wr_14': wr_val, 'bias_20': bias_20, 'bb_pct': bb_pct,
        'vol_lowest_20d': vol_lowest_20d,
        'pb_green_ratio': pb_green_ratio, 'max_dd_day': max_dd_day,
        'red_vol_dec': red_vol_dec, 'vol_dec_accel': vol_dec_accel,
        'yellow_slope_5d': yellow_slope_5d, 'adx_14': adx_14,
        'rsi_5d_change': rsi_5d_change, 'dist_to_low': dist_to_low,
    })

    return f


def _pass_optional_gates(feat, p: B1V3Params) -> bool:
    """Apply Category C optional hard filters."""
    if p.require_no_dist_10d and feat.get('no_dist_10d', 1) == 0:
        return False
    if p.require_dif_bull_div and feat.get('dif_bull_div', 0) == 0:
        return False
    if p.require_ma_structure and feat.get('ma_structure_bull', 0) == 0:
        return False
    if p.require_j_bouncing and feat.get('j_bouncing', 0) == 0:
        return False
    if p.require_near_ma20 and feat.get('near_ma20', 0) == 0:
        return False
    if p.require_green and feat.get('is_green', 0) == 0:
        return False
    if p.require_mom_improving and feat.get('mom_improving', 0) == 0:
        return False
    if p.require_ma5_10_tight and feat.get('ma5_10_tight', 0) == 0:
        return False
    if p.require_vol_contracting and feat.get('vol_contracting', 0) == 0:
        return False
    if p.require_moderate_oversold and feat.get('moderate_oversold', 0) == 0:
        return False
    if p.require_pb_last_green and feat.get('pb_last_green', 0) == 0:
        return False
    if p.require_vol_price_improving and feat.get('vol_price_improving', 0) == 0:
        return False
    if p.require_anti_dist and feat.get('anti_dist', 0) == 0:
        return False
    if p.require_net_up_positive and feat.get('net_up_positive', 0) == 0:
        return False
    if p.require_vwap_mean_revert and feat.get('vwap_mean_revert', 0) < 0.1:
        return False
    if p.require_vol_dec_3 and feat.get('vol_dec_3', 0) == 0:
        return False
    if p.require_small_body and feat.get('small_body', 0) == 0:
        return False
    if p.require_small_us and feat.get('small_us', 0) == 0:
        return False
    if p.require_upvol_share_h and feat.get('upvol_share_h', 0) == 0:
        return False
    if p.require_close_high and feat.get('close_high', 0) == 0:
        return False
    if p.require_no_s1:
        # S1 check deferred — not implemented in Phase 1
        pass
    return True


# ============================================================
# LAYER 1: QUALITY SCORING
# ============================================================

def compute_factor_scores(signal: dict, p: B1V3Params) -> dict:
    """Compute individual factor score values for a single signal."""
    s = signal
    sc = {}

    # G0: Original core
    sc['J_score'] = max(0, 15 - abs(s['J'])) / 15
    sc['bowl_score'] = max(0, 4 - abs(s['vs_white'])) / 4
    sc['vol_sh_score'] = max(0, 1.5 - s['vol_ratio_ma5']) / 1.5
    sc['kd_dur_score'] = min(s['k_lt_d_days'], 10) / 10
    sc['dif_score'] = min(max(s['DIF'], 0), 3) / 3
    sc['slope_score'] = max(0, min(s['white_slope_5d'], 2)) / 2
    sc['dif_dea_score'] = s['dif_gt_dea']
    sc['ret5_score'] = max(0, 10 + s['ret_5d']) / 10 if s['ret_5d'] < 0 else 0.5
    sc['surge_score'] = s['surge_quality'] / 10
    sc['retrace_score'] = s['retrace_score'] / 3
    sc['nodist_score'] = s['no_dist_10d']

    # G1: Vol structure
    sc['dif_bull_score'] = s['dif_bull_div']
    sc['vol_rec_score'] = s['vol_shrinking_recent']
    sc['vol_ctr_score'] = s['vol_contracting']
    sc['vol_price_score'] = s['vol_price_improving']
    sc['net_up_score'] = s['net_up_positive']

    # G2: MA + form
    sc['ma_struct_score'] = s['ma_structure_bull']
    sc['j_bounce_score'] = s['j_bouncing']
    sc['near_ma20_score'] = s['near_ma20']
    sc['green_score'] = s['is_green']
    sc['mom_imp_score'] = s['mom_improving']
    sc['ma5_10_score'] = s['ma5_10_tight']
    sc['mod_over_score'] = s['moderate_oversold']
    sc['pb_green_score'] = s['pb_last_green']
    sc['anti_dist_score'] = s['anti_dist']
    sc['vwap_score'] = s['vwap_mean_revert']

    # G5: New (some pre-computed)
    sc['ret_vol_eff_score'] = max(0, 5 - s['ret_vol_eff']) / 5
    sc['mom_accel_score'] = max(0, s['mom_accel']) / 3
    sc['pe_score'] = max(0, 80 - s['pe']) / 80 if s.get('pe', -1) > 0 else 0
    sc['pb_score'] = max(0, 8 - s['pb']) / 8 if s.get('pb', -1) > 0 else 0
    sc['upvol_share_score'] = s['up_day_vol_share']
    sc['vol_dec_days_score'] = min(s['vol_dec_days'], 5) / 5
    # close2low: reward closing near the low of the day (pullback exhaustion)
    sc['close2low_score'] = 1.0 - s['close_to_low']
    sc['vs_yellow_score'] = max(0, 10 - abs(s['vs_yellow'])) / 10
    sc['body_small_score'] = max(0, 3 - abs(s['candle_body'])) / 3
    sc['us_small_score'] = max(0, 2 - s['upper_shadow']) / 2
    sc['vs_60h_score'] = max(0, abs(s['vs_60d_high']) - 5) / 10
    sc['vol10_score'] = max(0, 5 - s['volatility_10d']) / 5
    sc['dif_mom_score'] = max(0, -s['dif_momentum']) / 3

    # G4: Qlib
    sc['rsi_score'] = max(0, 40 - s['rsi_14']) / 40
    sc['atr_score'] = max(0, 3 - s['atr_14']) / 3
    sc['upvol_score'] = s['up_vol_ratio']
    sc['obv_div_score'] = s['obv_div']

    # G6: Tier1 new
    sc['wr_score'] = (100 - min(s['wr_14'], 100)) / 100
    sc['bias_score'] = max(0, 15 - abs(s['bias_20'])) / 15
    sc['bb_pct_score'] = 1 - abs(s['bb_pct'] - 0.5) * 2
    sc['vol_lowest_score'] = s['vol_lowest_20d']
    sc['pb_green_ratio_score'] = s['pb_green_ratio']
    sc['max_dd_day_score'] = max(0, 8 + s['max_dd_day']) / 8
    sc['red_vol_dec_score'] = s['red_vol_dec']
    # vol_dec_accel: clip to [0, 1] range (raw values ~0-500)
    sc['vol_dec_accel_score'] = min(max(s['vol_dec_accel'], 0) / 500, 1.0)
    sc['yellow_slope_score'] = max(0, min(s['yellow_slope_5d'], 3)) / 3
    sc['adx_score'] = min(s['adx_14'], 40) / 40

    # G7: Tier1 supplement
    sc['rsi_turn_score'] = 1.0 if s['rsi_5d_change'] > 0 and s['rsi_14'] < 40 else 0.0
    sc['dist_low_score'] = min(s['dist_to_low'], 15) / 15

    # V1 fusion: three-condition volume-price resonance
    # A=max_vol_bullish, B=2nd_max_vol_bullish, C=max_high_bullish+rank
    wave_dict = s.get('wave', {})
    count = sum([wave_dict.get('resonance_a', True),
                 wave_dict.get('resonance_b', True),
                 wave_dict.get('resonance_c', False)])
    if count == 3:
        sc['vol_resonance_score'] = 1.0   # full resonance
    elif count == 2:
        sc['vol_resonance_score'] = 0.5   # partial
    elif count == 1:
        sc['vol_resonance_score'] = 0.0   # weak
    else:
        sc['vol_resonance_score'] = -0.5  # divergence

    sc['pattern_sim_score'] = s.get('pattern_similarity', 0) / 100.0
    has_one_word = wave_dict.get('has_one_word_limit', False)
    has_shrink = wave_dict.get('has_shrink_limit_up', False)
    sc['limit_penalty_score'] = -2.0 if has_one_word else (-1.0 if has_shrink else 0)

    # V1 hist_bonus: lookup-based adjustment (-3 to +3), normalized to [-1, 1]
    hb = _load_hist_bonus()
    sc['hist_bonus_score'] = hb.get(str(s.get('code', '')), 0) / 3.0

    # Concept count (Phase 2): number of THS concepts the stock belongs to
    cc = _load_concept_count()
    sc['concept_cnt_score'] = min(cc.get(str(s.get('code', '')), 0), 20) / 20.0

    # Concept rank/dev (Phase 2): cross-sectional within same concept
    sc['ind_rank_score'] = s.get('concept_rank', 0.5)
    sc['ind_dev_score'] = s.get('concept_dev', 0.0)

    return sc


def score_candidates(signals: list, p: B1V3Params) -> list:
    """Layer 1: compute weighted quality score for each candidate."""
    fac_list = build_fac_list(p)
    for s in signals:
        if 'factor_scores' not in s:
            s['factor_scores'] = compute_factor_scores(s, p)

        score = 0.0
        tw = 0.0
        for name, feat_key, weight, enabled, group in fac_list:
            if enabled:
                val = s['factor_scores'].get(feat_key, 0)
                score += val * weight
                tw += weight

        s['quality_score'] = min(score / max(tw, 0.1) * 100, 100)

    return signals


# ============================================================
# LAYER 1.5: CROSS-SECTIONAL RANKING
# ============================================================

def add_cs_ranks(candidates: list) -> list:
    """Add cross-sectional percentile ranks to candidates on the same date."""
    n = len(candidates)
    if n < 3:
        for s in candidates:
            for cs_key in ['cs_close_pos', 'cs_lower_shadow', 'cs_upper_shadow_small',
                          'cs_bowl', 'cs_vol_shrink', 'cs_dif_strong', 'cs_J_mid',
                          'cs_small_body', 'cs_range_tight', 'cs_bar_reversal',
                          'cs_upper_tight', 'cs_retrace_depth', 'cs_trend_strong',
                          'cs_oversold_duration', 'cs_klow2', 'cs_kup2_small',
                          'cs_kmid2', 'cs_ksft2', 'cs_klen_tight']:
                s[cs_key] = 50.0
        return candidates

    def pct_rank(arr, ascending=True):
        arr = np.array(arr, dtype=float)
        if ascending:
            result = np.array([(arr > v).mean() * 100 for v in arr])
        else:
            result = np.array([(arr < v).mean() * 100 for v in arr])
        return result

    close_pos_arr = np.array([(s['close'] - s['low']) / max(s['high'] - s['low'], 0.001) for s in candidates])
    lower_shadow_arr = np.array([s.get('lower_shadow', 0) for s in candidates])
    upper_shadow_arr = np.array([s.get('upper_shadow', 0) for s in candidates])
    vs_white_arr = np.array([s['vs_white'] for s in candidates])
    vol_ratio_arr = np.array([s['vol_ratio_ma5'] for s in candidates])
    dif_arr = np.array([s['DIF'] for s in candidates])
    J_arr = np.array([s['J'] for s in candidates])
    candle_body_arr = np.array([s['candle_body'] for s in candidates])
    range_arr = np.array([(s['high'] - s['low']) / s['close'] * 100 for s in candidates])
    ksft2_arr = np.array([(2*s['close'] - s['high'] - s['low']) / max(s['high'] - s['low'], 0.001) for s in candidates])
    ret_depth_arr = np.array([-s['vs_60d_high'] for s in candidates])
    wy_gap_arr = np.array([s.get('white_yellow_gap', 0) for s in candidates])  # FIXED
    kbd_arr = np.array([s['k_lt_d_days'] for s in candidates])
    klow2_arr = np.array([s.get('qlib_klow2', 0) for s in candidates])
    kup2_arr = np.array([s.get('qlib_kup2', 0) for s in candidates])
    kmid2_arr = np.array([s.get('qlib_kmid2', 0) for s in candidates])
    ksft2_arr_cs = np.array([s.get('qlib_ksft2', 0) for s in candidates])
    klen_arr = np.array([s.get('qlib_klen', 0) for s in candidates])

    cs_close_pos = pct_rank(close_pos_arr, ascending=False)
    cs_lower_shadow = pct_rank(lower_shadow_arr, ascending=False)
    cs_upper_shadow_small = pct_rank(upper_shadow_arr, ascending=True)
    cs_bowl = pct_rank(np.abs(vs_white_arr), ascending=True)
    cs_vol_shrink = pct_rank(vol_ratio_arr, ascending=True)
    cs_dif_strong = pct_rank(dif_arr, ascending=False)
    cs_J_mid = pct_rank(np.abs(J_arr - 5), ascending=True)
    cs_small_body = pct_rank(np.abs(candle_body_arr), ascending=True)
    cs_range_tight = pct_rank(range_arr, ascending=True)
    cs_bar_reversal = pct_rank(ksft2_arr, ascending=False)
    cs_upper_tight = pct_rank(upper_shadow_arr, ascending=True)
    cs_retrace_depth = pct_rank(ret_depth_arr, ascending=True)
    cs_trend_strong = pct_rank(wy_gap_arr, ascending=False)
    cs_oversold_duration = pct_rank(kbd_arr, ascending=False)
    cs_klow2 = pct_rank(klow2_arr, ascending=False)
    cs_kup2_small = pct_rank(kup2_arr, ascending=True)
    cs_kmid2 = pct_rank(kmid2_arr, ascending=False)
    cs_ksft2 = pct_rank(ksft2_arr_cs, ascending=False)
    cs_klen_tight = pct_rank(klen_arr, ascending=True)  # Bug #3 fix

    for i, s in enumerate(candidates):
        s['cs_close_pos'] = cs_close_pos[i]
        s['cs_lower_shadow'] = cs_lower_shadow[i]
        s['cs_upper_shadow_small'] = cs_upper_shadow_small[i]
        s['cs_bowl'] = cs_bowl[i]
        s['cs_vol_shrink'] = cs_vol_shrink[i]
        s['cs_dif_strong'] = cs_dif_strong[i]
        s['cs_J_mid'] = cs_J_mid[i]
        s['cs_small_body'] = cs_small_body[i]
        s['cs_range_tight'] = cs_range_tight[i]
        s['cs_bar_reversal'] = cs_bar_reversal[i]
        s['cs_upper_tight'] = cs_upper_tight[i]
        s['cs_retrace_depth'] = cs_retrace_depth[i]
        s['cs_trend_strong'] = cs_trend_strong[i]
        s['cs_oversold_duration'] = cs_oversold_duration[i]
        s['cs_klow2'] = cs_klow2[i]
        s['cs_kup2_small'] = cs_kup2_small[i]
        s['cs_kmid2'] = cs_kmid2[i]
        s['cs_ksft2'] = cs_ksft2[i]
        s['cs_klen_tight'] = cs_klen_tight[i]  # Bug #3 fix

    # Concept-based cross-sectional ranking (Phase 2)
    # Load concept map once
    global STOCK_CONCEPTS, CONCEPT_STOCKS
    if STOCK_CONCEPTS is None:
        import json
        concept_path = Path("data/block/concept.json")
        if concept_path.exists():
            with open(concept_path, 'r', encoding='utf-8') as f:
                d = json.load(f)
            STOCK_CONCEPTS = {}
            CONCEPT_STOCKS = {}
            for cid, stocks in d.get('stock_map', {}).items():
                if isinstance(stocks, list):
                    codes = set()
                    for s in stocks:
                        if isinstance(s, str) and ':' in s:
                            code = s.split(':')[1].rstrip('*')
                            if len(code) == 6 and code.isdigit():
                                codes.add(code)
                                STOCK_CONCEPTS.setdefault(code, set()).add(cid)
                    if codes:
                        CONCEPT_STOCKS[cid] = codes

    if STOCK_CONCEPTS:
        # Compute concept-based rank and deviation for each candidate
        concept_scores = {}  # concept -> [(idx, quality_score)]
        for i, s in enumerate(candidates):
            code = str(s.get('code', ''))
            for cid in STOCK_CONCEPTS.get(code, set()):
                concept_scores.setdefault(cid, []).append((i, s.get('quality_score', 0)))

        concept_rank_sum = [0.0] * len(candidates)
        concept_dev_sum = [0.0] * len(candidates)
        concept_count = [0] * len(candidates)

        for cid, items in concept_scores.items():
            if len(items) < 3:
                continue
            scores = np.array([sc for _, sc in items])
            mean_sc = scores.mean()
            std_sc = scores.std() if scores.std() > 1e-6 else 1.0
            ranks = np.array([(scores < v).mean() * 100 for v in scores])  # percentile
            for j, (idx, _) in enumerate(items):
                concept_rank_sum[idx] += ranks[j]
                concept_dev_sum[idx] += (scores[j] - mean_sc) / std_sc
                concept_count[idx] += 1

        for i, s in enumerate(candidates):
            if concept_count[i] > 0:
                s['concept_rank'] = concept_rank_sum[i] / concept_count[i] / 100.0  # 0-1
                s['concept_dev'] = max(-3.0, min(3.0, concept_dev_sum[i] / concept_count[i])) / 3.0  # -1 to 1
            else:
                s['concept_rank'] = 0.5
                s['concept_dev'] = 0.0

    return candidates


# ============================================================
# LAYER 2: FILTER & RANK
# ============================================================

def filter_and_rank(by_date: dict, p: B1V3Params) -> dict:
    """Filter signals by params and rank by quality_score."""
    filtered = {}

    for date_val, candidates in by_date.items():
        # Add cross-sectional ranks
        candidates = add_cs_ranks(candidates)

        # Score all candidates
        candidates = score_candidates(candidates, p)

        # B9: CS shadow filter
        kept = []
        for s in candidates:
            if s.get('cs_lower_shadow', 100) > p.cs_shadow_max:
                continue
            kept.append(s)

        # Sort by quality_score
        kept.sort(key=lambda x: x['quality_score'], reverse=True)

        # quality_score_min + top_n
        kept = [s for s in kept if s['quality_score'] >= p.quality_score_min]
        kept = kept[:p.top_n_per_day]

        if kept:
            filtered[date_val] = kept

    return filtered


# ============================================================
# LAYER 3: BACKTEST SIMULATION
# ============================================================

def _detect_ddi(df_desc):
    """DDT/滴滴 signal: large bullish candle with upper shadow and volume spike."""
    if len(df_desc) < 4:
        return False
    r0 = df_desc.iloc[0]; r1 = df_desc.iloc[1]
    pct = (r0['close'] - r1['close']) / r1['close'] * 100
    us = (r0['high'] - max(r0['close'], r0['open'])) / r0['close'] * 100
    return pct > 3 and us > 2 and r0['volume'] > df_desc.iloc[1:4]['volume'].mean() * 1.2


def _detect_distribution(df_desc):
    """Distribution: T close < T-2 open — 3-day failed rally / reversal."""
    if len(df_desc) < 3:
        return False
    t_close = df_desc.iloc[0]['close']
    t2_open = df_desc.iloc[2]['open']
    return t_close < t2_open


def _detect_s1_exit(df_desc, entry_date):
    """
    S1 exit Type 1: 放量巨阴 (aligned with utils/s1_filter.py)
    - MA5 > MA10 > MA20
    - close <= open (true bearish, no fake bearish, no 一字板)
    - Heavy volume: vol/prev >= 1.5 OR vol/MA20 >= 2.0 OR vol >= max_bull_vol * 0.9
    - Price in upper 70% of 20d range (or within 3d of top-3 high)
    - Must have gained >4% from entry at some point.
    """
    entry_dt = pd.Timestamp(entry_date)
    df_desc = df_desc.sort_values('date').reset_index(drop=True)

    entry_idx = df_desc[df_desc['date'] >= entry_dt].index[0]
    hold_start = max(0, entry_idx - 20)
    hold = df_desc.iloc[hold_start:].copy().reset_index(drop=True)
    entry_pos = entry_idx - hold_start
    n = len(hold)

    if n - entry_pos < 2:
        return False, None, None

    entry_price = hold.iloc[entry_pos]['close']
    if (hold['close'].iloc[entry_pos:].max() / entry_price - 1) * 100 < 4.0:
        return False, None, None

    has_ma = 'MA5' in hold.columns and 'MA10' in hold.columns and 'MA20' in hold.columns

    # 20d MA volume from entry_pos only (avoid pre-entry low vol bias)
    vol_ma20 = hold['volume'].copy()
    vol_ma20.iloc[:entry_pos] = np.nan
    vol_ma20 = vol_ma20.rolling(20, min_periods=1).mean()
    hold['vol_ratio_ma20'] = hold['volume'] / vol_ma20.shift(1)
    hold['vol_ratio_prev'] = hold['volume'] / hold['volume'].shift(1)

    # Max bull volume in position period
    pos_data = hold.iloc[entry_pos:]
    bullish_mask = pos_data['close'] > pos_data['open']
    max_bull_vol = pos_data.loc[bullish_mask, 'volume'].max() if bullish_mask.any() else 0

    # Top-3 high neighbors for price position exemption
    top3_hi = set(pos_data['high'].nlargest(3).index)
    peak_neighbors = set()
    for pi in top3_hi:
        for offset in [1, 2, 3]:
            nxt = pi + offset
            if nxt < n:
                peak_neighbors.add(nxt)

    # 20d price range for position period
    pos_20 = hold.iloc[max(entry_pos, n - 20):]
    range_high = pos_20['high'].max()
    range_low = pos_20['low'].min()

    for i in range(n - 1, entry_pos - 1, -1):
        row = hold.iloc[i]

        # MA bullish alignment
        if has_ma:
            if not (pd.notna(row['MA5']) and pd.notna(row['MA10']) and pd.notna(row['MA20'])):
                continue
            if not (row['MA5'] > row['MA10'] > row['MA20']):
                continue

        # True bearish only, exclude 一字板
        if not (row['close'] <= row['open'] and row['low'] < row['high']):
            continue

        # Heavy volume (OR of three sub-conditions)
        vol_ok = (pd.notna(row['vol_ratio_prev']) and row['vol_ratio_prev'] >= 1.5) or \
                 (pd.notna(row['vol_ratio_ma20']) and row['vol_ratio_ma20'] >= 2.0)
        if max_bull_vol > 0 and row['volume'] >= max_bull_vol * 0.9:
            vol_ok = True
        if not vol_ok:
            continue

        # Price position: upper 70% of 20d range or peak neighbor
        price_pos = (row['close'] - range_low) / (range_high - range_low) if range_high > range_low else 0
        if not (price_pos >= 0.70 or i in peak_neighbors):
            continue

        return True, str(row['date']), '放量巨阴'

    return False, None, None


def simulate(filtered_signals: dict, p: B1V3Params, initial_capital=None):
    """Backtest simulation with V2 exit engine."""
    if initial_capital is None:
        initial_capital = p.initial_capital
    cash = initial_capital
    positions = []
    daily_eq = []
    trades = []
    sorted_dates = sorted(filtered_signals.keys())

    # Load indicator cache for all relevant codes
    all_codes = set()
    for sigs in filtered_signals.values():
        for s in sigs:
            all_codes.add(s['code'])

    ind_cache = {}
    for code in all_codes:
        cp = INDICATORS_DIR / f"{code}.parquet"
        if cp.exists():
            df = pd.read_parquet(cp)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            ind_cache[code] = df

    active_codes = set()

    for date_val in sorted_dates:
        ts = pd.Timestamp(date_val)
        candidates = filtered_signals[date_val][:p.top_n_per_day]

        # ---- EXITS ----
        to_remove = []
        for pi, pos in enumerate(positions):
            code = pos['code']
            df = ind_cache.get(code)
            if df is None:
                continue

            m = df['date'] == ts
            if not m.any():
                if ts > df['date'].max() + pd.Timedelta(days=5):
                    row = df.iloc[-1]
                else:
                    continue
            else:
                row = df[m].iloc[0]

            cur_c = row['close']; cur_l = row['low']
            shares = pos['shares']; cost = pos['cost']
            unreal = shares * (cur_c - pos['entry_price'])
            hdays = (ts - pd.Timestamp(pos['entry_date'])).days

            launched = pos.get('launched', False)
            if not launched and unreal / cost > 0.05 and cur_c > row['white_line']:
                pos['launched'] = True; launched = True

            reason, xp = None, None

            if cur_l <= pos['stop_loss']:
                reason, xp = 'stop_loss', pos['stop_loss']
            elif hdays >= 3 and unreal / cost < p.t_plus_3_min_return:
                reason, xp = 'T_plus_3', cur_c
            elif p.exit_break_yellow and cur_c < row['yellow_line']:
                reason, xp = 'break_yellow', cur_c
            elif launched and p.exit_break_white and cur_c < row['white_line']:
                reason, xp = 'break_white', cur_c
            elif launched and p.exit_profit_25pct and unreal / cost > 0.25:
                reason, xp = 'profit_25pct', cur_c
            elif p.exit_s1_clear:
                df_desc = df[df['date'] <= ts].sort_values('date', ascending=False).reset_index(drop=True)
                has_s1, _, s1t = _detect_s1_exit(df_desc, pos['entry_date'])
                if has_s1:
                    reason, xp = f'S1_{s1t}', cur_c
            elif hdays >= p.max_hold_days:
                reason, xp = 'max_hold', cur_c

            if reason:
                profit = shares * xp - cost
                cash += shares * xp
                trades.append({
                    'code': code, 'buy_date': pos['entry_date'],
                    'buy_price': pos['entry_price'], 'sell_date': ts,
                    'sell_price': xp, 'hold_days': hdays,
                    'exit_reason': reason, 'profit_amount': profit,
                    'profit_pct': (xp / pos['entry_price'] - 1) * 100,
                })
                to_remove.append(pi)
                active_codes.discard(code)

        for idx in sorted(to_remove, reverse=True):
            del positions[idx]

        # ---- ENTRIES ----
        for cand in candidates:
            if len(positions) >= p.max_positions:
                break
            if cand['code'] in active_codes:
                continue

            pos_val = cash * p.position_pct
            shares = int(pos_val / cand['close'] / 100) * 100
            if shares < 100:
                continue
            cost = shares * cand['close']
            if cost > cash * 0.9:
                continue

            sl = cand['low'] - p.stop_loss_width
            if sl <= 0:
                continue

            cash -= cost
            positions.append({
                'code': cand['code'], 'entry_date': ts,
                'entry_price': cand['close'], 'shares': shares,
                'cost': cost, 'stop_loss': sl, 'launched': False,
                'surge_start_date': cand.get('surge_start_date'),
            })
            active_codes.add(cand['code'])

        # ---- Equity ----
        pos_val = 0
        for pos in positions:
            df = ind_cache.get(pos['code'])
            if df is not None:
                m = df['date'] == ts
                pos_val += pos['shares'] * (df[m].iloc[0]['close'] if m.any() else pos['entry_price'])
            else:
                pos_val += pos['shares'] * pos['entry_price']

        daily_eq.append({
            'date': ts, 'equity': cash + pos_val,
            'cash': cash, 'positions': len(positions),
        })

    return pd.DataFrame(daily_eq), pd.DataFrame(trades)


# ============================================================
# METRICS
# ============================================================

SENIOR = {
    'total_return': 102.95, 'win_rate': 44.4, 'max_drawdown': 7.50,
    'sharpe': 3.73, 'profit_factor': 2.34,
}

def calc_metrics(trades_df, eq_df, initial_capital=1_000_000):
    m = {}
    if len(trades_df) == 0:
        return {'total_return': 0, 'total_trades': 0, 'win_rate': 0, 'score': -1e9}

    wins = trades_df['profit_amount'] > 0
    m['total_trades'] = len(trades_df)
    m['winning_trades'] = int(wins.sum())
    m['losing_trades'] = int((~wins).sum())
    m['win_rate'] = wins.mean() * 100
    m['total_pnl'] = trades_df['profit_amount'].sum()
    m['avg_win'] = trades_df.loc[wins, 'profit_pct'].mean() if wins.any() else 0
    m['avg_loss'] = trades_df.loc[~wins, 'profit_pct'].mean() if (~wins).any() else 0
    m['wl_ratio'] = abs(m['avg_win'] / m['avg_loss']) if m['avg_loss'] != 0 else 99

    gp = trades_df.loc[wins, 'profit_amount'].sum()
    gl = abs(trades_df.loc[~wins, 'profit_amount'].sum())
    m['profit_factor'] = gp / gl if gl > 0 else 99

    m['avg_hold_win'] = trades_df.loc[wins, 'hold_days'].mean() if wins.any() else 0
    m['avg_hold_loss'] = trades_df.loc[~wins, 'hold_days'].mean() if (~wins).any() else 0

    if len(eq_df) > 0:
        fe = eq_df['equity'].iloc[-1]
        m['total_return'] = (fe / initial_capital - 1) * 100
        m['final_equity'] = fe
        eq_df = eq_df.copy()
        eq_df['peak'] = eq_df['equity'].cummax()
        eq_df['dd'] = (eq_df['equity'] - eq_df['peak']) / eq_df['peak'] * 100
        m['max_drawdown'] = eq_df['dd'].min()

        if len(eq_df) >= 20:
            eq_df['date'] = pd.to_datetime(eq_df['date'])
            monthly = eq_df.set_index('date')['equity'].resample('ME').last().dropna()
            if len(monthly) >= 3:
                mr = monthly.pct_change().dropna()
                m['sharpe'] = mr.mean() / mr.std() * np.sqrt(12) if mr.std() > 0 else 0
            else:
                m['sharpe'] = 0
        else:
            m['sharpe'] = 0
    else:
        m['total_return'] = m['sharpe'] = m['max_drawdown'] = 0

    if len(trades_df) > 0:
        m['best_code'] = trades_df.loc[trades_df['profit_pct'].idxmax(), 'code']
        m['best_pct'] = trades_df['profit_pct'].max()
        m['worst_code'] = trades_df.loc[trades_df['profit_pct'].idxmin(), 'code']
        m['worst_pct'] = trades_df['profit_pct'].min()

    if len(trades_df) >= 10:
        top10 = trades_df.nlargest(10, 'profit_amount')
        m['top10_pnl_pct'] = top10['profit_amount'].sum() / m['total_pnl'] * 100 if m['total_pnl'] != 0 else 0

    # Composite score
    s = 0
    if m['total_trades'] >= 30:
        s += m.get('total_return', 0) * 0.25
        s += m.get('sharpe', 0) * 60 * 0.18
        s += min(m.get('profit_factor', 0), 5) * 12 * 0.15
        s += (100 - abs(m.get('max_drawdown', 50))) * 0.08
        s += min(m.get('win_rate', 0), 55) * 0.15
        s += min(m.get('total_trades', 0), 300) * 0.07
        s += min(m.get('wl_ratio', 0), 5) * 4 * 0.07
        s += min(60 - abs(m.get('top10_pnl_pct', 100) - 90), 40) * 0.05
    m['score'] = s

    return m


def compare_senior(m):
    comp = {}
    for key in ['total_return', 'win_rate', 'sharpe', 'profit_factor']:
        if key in m and key in SENIOR:
            d = m[key] - SENIOR[key]
            comp[key] = {'current': m[key], 'target': SENIOR[key], 'delta': d,
                         'status': 'WIN' if d > 0 else 'LOSE'}
    if 'max_drawdown' in m:
        c, t = abs(m['max_drawdown']), SENIOR['max_drawdown']
        d = t - c
        comp['max_drawdown'] = {'current': c, 'target': t, 'delta': d,
                                'status': 'WIN' if d > 0 else 'LOSE'}
    better = sum(1 for v in comp.values() if v['status'] == 'WIN')
    comp['composite'] = {'better': better, 'total': len(comp),
                         'pct': better/len(comp)*100 if comp else 0,
                         'dominates': better >= len(comp)*0.8}
    return comp


# ============================================================
# SIGNAL CACHING
# ============================================================

def _extract_star(args):
    code, start_date, end_date, p = args
    return extract_signals_single(code, start_date, end_date, p)


def build_raw_cache(stock_codes, start_date, end_date, p: B1V3Params, n_workers=None):
    """Extract all raw candidates and cache to disk.

    The cache key binds parameters, universe, input data, and extraction code.
    A validated sidecar records the full identity and parameters for audit.
    """
    stock_codes = list(stock_codes)
    if n_workers is None:
        n_workers = max(1, mp.cpu_count() - 2)

    fp, params_dict = _param_fingerprint(p)
    universe_fp = _universe_fingerprint(stock_codes)
    data_fp = _data_snapshot_fingerprint(stock_codes)
    contract_fp = _feature_contract_fingerprint()
    identity = {
        "cache_version": CACHE_VERSION,
        "strategy": "B1_V3",
        "start": str(start_date),
        "end": str(end_date),
        "param_fingerprint": fp,
        "universe_fingerprint": universe_fp,
        "data_snapshot_fingerprint": data_fp,
        "feature_contract_fingerprint": contract_fp,
    }
    cache_key = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]
    cache_file = RAW_CACHE_DIR / f"b1v3_raw_{start_date}_{end_date}_{cache_key}.pkl"
    meta_file = cache_file.with_suffix(".meta.json")

    if cache_file.exists():
        cached = _load_raw_cache(cache_file, meta_file, identity)
        if cached is not None:
            print(f"  Loading raw cache: {cache_file.name}")
            return cached
        print(f"  Ignoring invalid raw cache: {cache_file.name}")

    print(f"\n  Extracting raw signals from {len(stock_codes)} stocks...")
    tasks = [(code, start_date, end_date, p) for code in stock_codes]

    all_signals = []
    with mp.Pool(n_workers) as pool:
        for i, result in enumerate(pool.imap_unordered(_extract_star, tasks, chunksize=50)):
            all_signals.extend(result)
            if i % 500 == 0:
                print(f"    Progress: {i}/{len(tasks)} ({len(all_signals)} signals)", end='\r')
        print(f"    Done: {len(tasks)} stocks, {len(all_signals)} raw signals")

    by_date = defaultdict(list)
    for sig in all_signals:
        by_date[sig['date']].append(sig)
    by_date = {d: v for d, v in sorted(by_date.items())}
    print(f"  {len(by_date)} trading days with signals")

    if (
        _data_snapshot_fingerprint(stock_codes) != data_fp
        or _feature_contract_fingerprint() != contract_fp
    ):
        raise RuntimeError(
            "B1 V3 raw-cache inputs changed during extraction; result was not published"
        )

    # Cache metadata for audit / parameter-invalidation triage.
    meta = {
        "schema_version": 1,
        "identity": identity,
        "cache_version": CACHE_VERSION,
        "strategy": "B1_V3",
        "start": str(start_date),
        "end": str(end_date),
        "param_fingerprint": fp,
        "universe_fingerprint": universe_fp,
        "data_snapshot_fingerprint": data_fp,
        "feature_contract_fingerprint": contract_fp,
        "params": params_dict,
        "n_stocks": len(stock_codes),
        "n_days": len(by_date),
        "n_raw_signals": len(all_signals),
        "cache_file": cache_file.name,
    }
    _atomic_write_json(meta_file, meta)
    _atomic_write_pickle(
        cache_file,
        {
            "schema_version": 1,
            "identity": identity,
            "by_date": by_date,
            "stock_codes": list(stock_codes),
        },
    )
    print(f"  Cached to {cache_file.name} (meta: {meta_file.name})")

    return by_date, stock_codes


# ============================================================
# MAIN ENTRY POINT (for testing)
# ============================================================

if __name__ == "__main__":
    from strategy.b1_v3_config import B1V3Params, build_fac_list, count_factors_by_group
    p = B1V3Params()
    fac = build_fac_list(p)
    print(f"B1 V3 loaded: {len(fac)} factors, {sum(1 for _,_,_,e,_ in fac if e)} enabled")
    print("Config OK")
