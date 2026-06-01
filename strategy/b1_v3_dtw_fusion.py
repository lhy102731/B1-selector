# -*- coding: utf-8 -*-
"""
DTW pattern matching fusion for B1 V3.
Computes similarity against 27 historical cases.
"""

import sys, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from strategy.pattern_feature_extractor import PatternFeatureExtractor
from strategy.pattern_matcher import PatternMatcher
from strategy.pattern_config import B1_PERFECT_CASES, SIMILARITY_WEIGHTS

# ============================================================
# Global: pre-computed case features
# ============================================================
CASE_FEATURES = None
CASE_LIST = None
EXTRACTOR = None
MATCHER = None


def _init_dtw():
    """Initialize DTW engine (lazy, called once)."""
    global CASE_FEATURES, CASE_LIST, EXTRACTOR, MATCHER
    if CASE_FEATURES is not None:
        return

    EXTRACTOR = PatternFeatureExtractor(lookback_days=25)
    MATCHER = PatternMatcher(weights=SIMILARITY_WEIGHTS)
    CASE_FEATURES = {}
    CASE_LIST = []

    INDICATORS_DIR = Path("data/indicators_cache")

    for case in B1_PERFECT_CASES:
        code = case['code']
        breakout_date = pd.Timestamp(case['breakout_date'])
        lookback = case.get('lookback_days', 25)

        parquet_path = INDICATORS_DIR / f"{code}.parquet"
        if not parquet_path.exists():
            continue

        try:
            df = pd.read_parquet(parquet_path)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date', ascending=False).reset_index(drop=True)

            # Get data up to breakout_date
            df_case = df[df['date'] <= breakout_date].copy()
            if len(df_case) < lookback + 10:
                continue

            # Extract features
            features = EXTRACTOR.extract(df_case, lookback_days=lookback)
            CASE_FEATURES[case['id']] = {
                'features': features,
                'case': case,
            }
            CASE_LIST.append(case['id'])

        except Exception as e:
            print(f"  DTW: failed to load case {case['id']} ({code}): {e}")
            continue

    print(f"DTW initialized: {len(CASE_FEATURES)}/{len(B1_PERFECT_CASES)} cases loaded")


def compute_similarity(df_desc, lookback_days=None):
    """
    Compute best DTW similarity score against all cases.
    df_desc: descending-order DataFrame (latest first).
    Returns: (best_score, best_case_id, all_scores_dict)
    """
    _init_dtw()

    if not CASE_FEATURES:
        return 0.0, None, {}

    try:
        lookback = lookback_days or 25
        cand_features = EXTRACTOR.extract(df_desc, lookback_days=lookback)
    except Exception:
        return 0.0, None, {}

    best_score = 0.0
    best_case = None
    all_scores = {}

    for case_id in CASE_LIST:
        case_data = CASE_FEATURES[case_id]
        try:
            result = MATCHER.match(cand_features, case_data['features'])
            score = result.get('total_score', 0.0)
            all_scores[f'case_sim_{case_id}'] = score
            if score > best_score:
                best_score = score
                best_case = case_id
        except Exception:
            all_scores[f'case_sim_{case_id}'] = 0.0
            continue

    return best_score, best_case, all_scores
