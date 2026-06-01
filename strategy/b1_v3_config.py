# -*- coding: utf-8 -*-
"""
B1 V3 unified parameter config.
All thresholds, weights, switches exposed as parameters.
No hardcoded magic numbers anywhere.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Any, Optional

# ============================================================
# B1V3Params: all parameters organized by category
# ============================================================

@dataclass
class B1V3Params:
    """Master parameter set for B1 V3 strategy."""

    # ---- A: Structural hard filters (logic always on, thresholds sweepable) ----
    require_white_gt_yellow: bool = True
    dif_min: float = 0.05
    require_no_double_60d: bool = True
    cap_min: float = 2_000_000_000
    # A5: position mode
    position_mode: str = "bowl"          # "bowl" / "vs_white" / "both"
    bowl_near_pct: float = 3.5           # V1 near tolerance %
    vs_white_low: float = -5.5           # V2-style lower bound %
    vs_white_high: float = 2.5           # V2-style upper bound %
    vs_yellow_min: float = 0             # minimum % above yellow

    # ---- B: Threshold filters (exceed => reject, within => score) ----
    j_max: float = 30.0
    j_min: float = -15.0
    # V1 volume measure
    vol_shrink_mode: str = "v1"          # "v1" / "v2" / "both" / "either"
    vol_vs_wave_peak_max: float = 0.9    # V1: vol / 20d-max-vol from wave start
    vol_peak_lookback: int = 20
    # V2 volume measure
    vol_ratio_ma5_max: float = 1.5       # V2: vol / 5d-avg-vol
    # Other B thresholds
    turnover_max: float = 6.0
    ret_5d_min: float = -12.0
    white_slope_min: float = -0.3
    pe_max: float = 80.0
    pb_max: float = 8.0
    cs_shadow_max: float = 70.0          # cross-sectional upper shadow pctile

    # ---- C: Optional hard filters (binary gates, independently toggleable) ----

    # C1-C6: from V1 wave lifecycle
    require_wave_qualified: bool = True       # gain<=60%, turnover<=90%, red/green>=1.2
    require_wave_healthy: bool = True         # surge_vol / accumulation_vol > 2.0
    require_no_wave_break: bool = True        # wave not broken (or new wave existed)
    require_max_vol_bullish: bool = True      # max-vol day must be bullish
    require_2nd_max_vol_bullish: bool = True  # 2nd max-vol day must be bullish
    require_max_high_vol_rank: bool = True    # max-high day vol rank <= 2
    require_no_s1: bool = False               # V1 comment: proven irrelevant

    # C8-C17: from V2 additional gates (default OFF)
    require_k_lt_d_days: int = 0              # 0=off, N=min days K<D
    require_no_dist_10d: bool = False
    require_dif_bull_div: bool = False
    require_ma_structure: bool = False
    require_j_bouncing: bool = False
    require_near_ma20: bool = False
    require_green: bool = False
    require_mom_improving: bool = False
    require_ma5_10_tight: bool = False
    require_vol_contracting: bool = False
    require_moderate_oversold: bool = False
    require_pb_last_green: bool = False
    require_vol_price_improving: bool = False
    require_anti_dist: bool = False
    require_net_up_positive: bool = False
    require_vwap_mean_revert: bool = False
    require_vol_dec_3: bool = False
    require_small_body: bool = False
    require_small_us: bool = False
    require_upvol_share_h: bool = False
    require_close_high: bool = False

    # ---- D: Scoring factor toggles (see FAC_LIST for details) ----
    # G0 original core (default ON)
    q_J: bool = True
    q_bowl: bool = True
    q_vol_sh: bool = True
    q_kd_dur: bool = True
    q_dif: bool = True
    q_slope: bool = True
    q_dif_dea: bool = True
    q_ret5: bool = True
    q_surge: bool = True
    q_retrace: bool = True
    q_nodist: bool = True
    # G1 vol structure (default ON)
    q_dif_bull: bool = True
    q_vol_rec: bool = True
    q_vol_ctr: bool = True
    q_vol_price: bool = True
    q_net_up: bool = True
    # G2 MA + form (default OFF)
    q_ma_struct: bool = False
    q_j_bounce: bool = False
    q_near_ma20: bool = False
    q_green: bool = False
    q_mom_imp: bool = False
    q_ma5_10: bool = False
    q_mod_over: bool = False
    q_pb_green: bool = False
    q_anti_dist: bool = False
    q_vwap: bool = False
    # G3 CS ranks (default OFF)
    q_cs_close: bool = False
    q_cs_shadow: bool = False
    q_cs_bowl: bool = False
    q_cs_vol: bool = False
    q_cs_dif: bool = False
    q_cs_J: bool = False
    q_cs_range: bool = False
    q_cs_bar_rev: bool = False
    q_cs_up_tight: bool = False
    q_cs_retrace: bool = False
    q_cs_trend: bool = False
    q_cs_kd_dur: bool = False
    q_cs_klow2: bool = False
    q_cs_kup2: bool = False
    q_cs_kmid2: bool = False
    q_cs_ksft2: bool = False
    q_cs_klen: bool = False
    # G4 Qlib (default OFF)
    q_rsi: bool = False
    q_atr: bool = False
    q_upvol: bool = False
    q_obv_div: bool = False
    # G5 new factors (default OFF)
    q_ret_vol_eff: bool = False
    q_mom_accel: bool = False
    q_pe: bool = False
    q_pb: bool = False
    q_upvol_share: bool = False
    q_vol_dec_days: bool = False
    q_close2low: bool = False
    q_vs_yellow: bool = False
    q_body_small: bool = False
    q_us_small: bool = False
    q_vs_60h: bool = False
    q_vol10: bool = False
    q_dif_mom: bool = False
    # G6 Tier1 mean-reversion (default OFF)
    q_wr: bool = False
    q_bias: bool = False
    q_bb_pct: bool = False
    q_vol_lowest: bool = False
    q_pb_green_ratio: bool = False
    q_max_dd_day: bool = False
    q_red_vol_dec: bool = False
    q_vol_dec_accel: bool = False
    q_yellow_slope: bool = False
    q_adx: bool = False
    # G7 Tier1 supplement (default OFF)
    q_rsi_turn: bool = False
    q_dist_low: bool = False
    # G8 Tier2 industry (default OFF)
    q_ind_rank: bool = False
    q_ind_dev: bool = False
    q_concept_cnt: bool = False
    # G9 Tier3 index (default OFF)
    q_rel_str: bool = False
    q_beta: bool = False
    q_alpha: bool = False
    q_mkt_state: bool = False
    # G10 Tier4 minute (default OFF)
    q_vwap_dev: bool = False
    q_tail_lift: bool = False
    q_open_weak: bool = False
    q_pm_vol: bool = False
    # V1 fusion (default OFF)
    q_pattern_sim: bool = False
    q_vol_resonance: bool = False
    q_limit_penalty: bool = False
    q_hist_bonus: bool = False

    # ---- D: Scoring factor weights ----
    w_J: float = 2.0
    w_bowl: float = 1.5
    w_vol_sh: float = 1.5
    w_kd_dur: float = 1.0
    w_dif: float = 0.8
    w_slope: float = 0.7
    w_dif_dea: float = 0.5
    w_ret5: float = 0.5
    w_surge: float = 3.0
    w_retrace: float = 2.0
    w_nodist: float = 1.0
    w_dif_bull: float = 2.0
    w_vol_rec: float = 1.0
    w_vol_ctr: float = 2.0
    w_vol_price: float = 1.5
    w_net_up: float = 0.8
    w_ma_struct: float = 1.5
    w_j_bounce: float = 1.0
    w_near_ma20: float = 0.8
    w_green: float = 1.0
    w_mom_imp: float = 1.5
    w_ma5_10: float = 0.5
    w_mod_over: float = 1.5
    w_pb_green: float = 1.0
    w_anti_dist: float = 1.5
    w_vwap: float = 1.0
    w_cs_close: float = 1.5
    w_cs_shadow: float = 1.0
    w_cs_bowl: float = 1.5
    w_cs_vol: float = 1.5
    w_cs_dif: float = 1.0
    w_cs_J: float = 2.0
    w_cs_range: float = 1.5
    w_cs_bar_rev: float = 1.5
    w_cs_up_tight: float = 1.0
    w_cs_retrace: float = 0.8
    w_cs_trend: float = 0.8
    w_cs_kd_dur: float = 0.8
    w_cs_klow2: float = 1.5
    w_cs_kup2: float = 1.0
    w_cs_kmid2: float = 1.0
    w_cs_ksft2: float = 1.2
    w_cs_klen: float = 0.8
    w_rsi: float = 1.0
    w_atr: float = 0.8
    w_upvol: float = 1.0
    w_obv_div: float = 1.5
    w_ret_vol_eff: float = 1.0
    w_mom_accel: float = 1.5
    w_pe: float = 0.8
    w_pb: float = 0.8
    w_upvol_share: float = 1.5
    w_vol_dec_days: float = 1.0
    w_close2low: float = 1.2
    w_vs_yellow: float = 1.0
    w_body_small: float = 1.0
    w_us_small: float = 0.8
    w_vs_60h: float = 0.8
    w_vol10: float = 0.8
    w_dif_mom: float = 0.8
    w_wr: float = 2.0
    w_bias: float = 1.5
    w_bb_pct: float = 1.5
    w_vol_lowest: float = 2.0
    w_pb_green_ratio: float = 1.5
    w_max_dd_day: float = 1.0
    w_red_vol_dec: float = 1.5
    w_vol_dec_accel: float = 1.5
    w_yellow_slope: float = 1.0
    w_adx: float = 1.0
    w_rsi_turn: float = 1.5
    w_dist_low: float = 1.0
    w_ind_rank: float = 2.0
    w_ind_dev: float = 1.5
    w_concept_cnt: float = 1.0
    w_rel_str: float = 1.5
    w_beta: float = 1.0
    w_alpha: float = 2.0
    w_mkt_state: float = 1.0
    w_vwap_dev: float = 1.5
    w_tail_lift: float = 1.0
    w_open_weak: float = 0.8
    w_pm_vol: float = 1.0
    w_pattern_sim: float = 3.0
    w_vol_resonance: float = 1.0
    w_limit_penalty: float = 0.5
    w_hist_bonus: float = 1.0

    # ---- Layer 2: filter & rank ----
    quality_score_min: float = 0.0
    top_n_per_day: int = 5

    # ---- E: Structural channels ----
    washout_enabled: bool = True
    super_b1_marker: bool = True        # label only, doesn't filter

    # ---- F: Exit rules ----
    stop_loss_width: float = 0.05
    t_plus_3_min_return: float = 0.02
    exit_break_yellow: bool = True
    exit_break_white: bool = True       # after launched
    exit_profit_25pct: bool = True
    max_hold_days: int = 35
    exit_ddt: bool = False
    exit_distribution: bool = False
    exit_s1_clear: bool = False
    s1_skip_types: frozenset = frozenset()
    s1_exit_mode: str = "new"
    s1_exit_skip: frozenset = frozenset()  # skip specific exit S1 types
    exit_s1_half: bool = False

    # ---- Position management ----
    max_positions: int = 8
    position_pct: float = 0.20
    initial_capital: float = 1_000_000

    # ============ MODULE 1: Surge day detection ============
    surge_min_gain_pct: float = 4.0
    surge_require_positive: bool = True
    surge_vol_vs_ma5: float = 1.0
    surge_ma5_lookback: int = 5

    # ============ MODULE 2: Group formation ============
    group_forward_gap_max: int = 4       # <= this means days between merged
    group_back_merge_gap_max: int = 15
    group_back_merge_vol_ratio: float = 0.7
    group_back_merge_price_retention: float = 0.90
    group_back_merge_check_wave_break: bool = True

    # ============ MODULE 3: Peak detection ============
    peak_cut_loss_pct: float = -5.0
    peak_cut_vol_ratio: float = 1.5
    peak_cut_enabled: bool = True

    # ============ MODULE 4: Upper shadow limit ============
    shadow_upper_ratio: float = 0.618
    shadow_max_per_n_days: int = 6
    shadow_min_allowance: int = 3
    shadow_limit_enabled: bool = True

    # ============ MODULE 5: Wave qualification ============
    wave_max_gain_pct: float = 60.0
    wave_max_turnover_sum: float = 90.0
    wave_red_green_vol_ratio: float = 1.2
    wave_qualification_enabled: bool = True

    # ============ MODULE 6: Wave health ============
    wave_health_accum_days: int = 10
    wave_health_surge_vol_ratio: float = 2.0
    wave_health_min_accum_bars: int = 3
    wave_health_enabled: bool = True

    # ============ MODULE 7: Volume-price ranking ============
    vol_rank_max_must_bullish: bool = True
    vol_rank_2nd_must_bullish: bool = True
    vol_rank_high_price_rank_max: int = 2
    vol_rank_high_price_must_bullish: bool = True
    vol_rank_enabled: bool = True

    # ============ MODULE 8: Wave break ============
    wave_break_stop_width: float = 0.05
    wave_break_new_surge_gap_max: int = 5
    wave_break_enabled: bool = True

    # ============ MODULE 9: Volume shrink (B-level params already above) ============

    # ============ MODULE 10: Washout ============
    washout_max_days_since_break: int = 5
    washout_vol_must_shrink: bool = True
    washout_stop_not_hit: bool = True

    # ============ MODULE 11: Super B1 ============
    super_b1_j_rebound_min: float = 20.0
    super_b1_j_current_max: float = 20.0
    super_b1_cost_distance_pct: float = 4.0
    super_b1_slope_flatten: bool = True

    # ============ Signal extraction prefilter ============
    prefilter_lookback_days: int = 60
    prefilter_j_max: float = 30.0         # loose J filter during extraction
    prefilter_k_lt_d: bool = True         # require K<D during extraction
    sur_min_surge_quality: float = 2.0
    sur_min_retrace_score: float = 1.0


# ============================================================
# FAC_LIST: all scoring factors
# Format: (name, feature_key, weight_attr, default_on_attr, group)
# Actual score value is computed dynamically in score_candidates()
# ============================================================

def build_fac_list(p: B1V3Params) -> List[Tuple[str, str, float, bool, str]]:
    """Build FAC list from params. Each entry: (name, feat_key, weight, enabled, group)."""
    fac = []

    # G0: Original core
    g0 = [
        ('q_J',            'J_score',          p.w_J,          p.q_J,          'G0_core'),
        ('q_bowl',         'bowl_score',       p.w_bowl,       p.q_bowl,       'G0_core'),
        ('q_vol_sh',       'vol_sh_score',     p.w_vol_sh,     p.q_vol_sh,     'G0_core'),
        ('q_kd_dur',       'kd_dur_score',     p.w_kd_dur,     p.q_kd_dur,     'G0_core'),
        ('q_dif',          'dif_score',        p.w_dif,        p.q_dif,        'G0_core'),
        ('q_slope',        'slope_score',      p.w_slope,      p.q_slope,      'G0_core'),
        ('q_dif_dea',      'dif_dea_score',    p.w_dif_dea,    p.q_dif_dea,    'G0_core'),
        ('q_ret5',         'ret5_score',       p.w_ret5,       p.q_ret5,       'G0_core'),
        ('q_surge',        'surge_score',      p.w_surge,      p.q_surge,      'G0_core'),
        ('q_retrace',      'retrace_score',    p.w_retrace,    p.q_retrace,    'G0_core'),
        ('q_nodist',       'nodist_score',     p.w_nodist,     p.q_nodist,     'G0_core'),
    ]
    fac.extend(g0)

    # G1: Vol structure
    g1 = [
        ('q_dif_bull',     'dif_bull_score',   p.w_dif_bull,   p.q_dif_bull,   'G1_vol'),
        ('q_vol_rec',      'vol_rec_score',    p.w_vol_rec,    p.q_vol_rec,    'G1_vol'),
        ('q_vol_ctr',      'vol_ctr_score',    p.w_vol_ctr,    p.q_vol_ctr,    'G1_vol'),
        ('q_vol_price',    'vol_price_score',  p.w_vol_price,  p.q_vol_price,  'G1_vol'),
        ('q_net_up',       'net_up_score',     p.w_net_up,     p.q_net_up,     'G1_vol'),
    ]
    fac.extend(g1)

    # G2: MA + form
    g2 = [
        ('q_ma_struct',    'ma_struct_score',  p.w_ma_struct,  p.q_ma_struct,  'G2_ma'),
        ('q_j_bounce',     'j_bounce_score',   p.w_j_bounce,   p.q_j_bounce,   'G2_ma'),
        ('q_near_ma20',    'near_ma20_score',  p.w_near_ma20,  p.q_near_ma20,  'G2_ma'),
        ('q_green',        'green_score',      p.w_green,      p.q_green,      'G2_ma'),
        ('q_mom_imp',      'mom_imp_score',    p.w_mom_imp,    p.q_mom_imp,    'G2_ma'),
        ('q_ma5_10',       'ma5_10_score',     p.w_ma5_10,     p.q_ma5_10,     'G2_ma'),
        ('q_mod_over',     'mod_over_score',   p.w_mod_over,   p.q_mod_over,   'G2_ma'),
        ('q_pb_green',     'pb_green_score',   p.w_pb_green,   p.q_pb_green,   'G2_ma'),
        ('q_anti_dist',    'anti_dist_score',  p.w_anti_dist,  p.q_anti_dist,  'G2_ma'),
        ('q_vwap',         'vwap_score',       p.w_vwap,       p.q_vwap,       'G2_ma'),
    ]
    fac.extend(g2)

    # G3: CS ranks
    g3 = [
        ('q_cs_close',     'cs_close_pos',     p.w_cs_close,   p.q_cs_close,   'G3_cs'),
        ('q_cs_shadow',    'cs_lower_shadow',  p.w_cs_shadow,  p.q_cs_shadow,  'G3_cs'),
        ('q_cs_bowl',      'cs_bowl',          p.w_cs_bowl,    p.q_cs_bowl,    'G3_cs'),
        ('q_cs_vol',       'cs_vol_shrink',    p.w_cs_vol,     p.q_cs_vol,     'G3_cs'),
        ('q_cs_dif',       'cs_dif_strong',    p.w_cs_dif,     p.q_cs_dif,     'G3_cs'),
        ('q_cs_J',         'cs_J_mid',         p.w_cs_J,       p.q_cs_J,       'G3_cs'),
        ('q_cs_range',     'cs_range_tight',   p.w_cs_range,   p.q_cs_range,   'G3_cs'),
        ('q_cs_bar_rev',   'cs_bar_reversal',  p.w_cs_bar_rev, p.q_cs_bar_rev, 'G3_cs'),
        ('q_cs_up_tight',  'cs_upper_tight',   p.w_cs_up_tight, p.q_cs_up_tight, 'G3_cs'),
        ('q_cs_retrace',   'cs_retrace_depth', p.w_cs_retrace, p.q_cs_retrace, 'G3_cs'),
        ('q_cs_trend',     'cs_trend_strong',  p.w_cs_trend,   p.q_cs_trend,   'G3_cs'),
        ('q_cs_kd_dur',    'cs_oversold_duration', p.w_cs_kd_dur, p.q_cs_kd_dur, 'G3_cs'),
        ('q_cs_klow2',     'cs_klow2',         p.w_cs_klow2,   p.q_cs_klow2,   'G3_cs'),
        ('q_cs_kup2',      'cs_kup2_small',    p.w_cs_kup2,    p.q_cs_kup2,    'G3_cs'),
        ('q_cs_kmid2',     'cs_kmid2',         p.w_cs_kmid2,   p.q_cs_kmid2,   'G3_cs'),
        ('q_cs_ksft2',     'cs_ksft2',         p.w_cs_ksft2,   p.q_cs_ksft2,   'G3_cs'),
        ('q_cs_klen',      'cs_klen_tight',    p.w_cs_klen,    p.q_cs_klen,    'G3_cs'),
    ]
    fac.extend(g3)

    # G4: Qlib
    g4 = [
        ('q_rsi',          'rsi_score',        p.w_rsi,        p.q_rsi,        'G4_qlib'),
        ('q_atr',          'atr_score',        p.w_atr,        p.q_atr,        'G4_qlib'),
        ('q_upvol',        'upvol_score',      p.w_upvol,      p.q_upvol,      'G4_qlib'),
        ('q_obv_div',      'obv_div_score',    p.w_obv_div,    p.q_obv_div,    'G4_qlib'),
    ]
    fac.extend(g4)

    # G5: New factors
    g5 = [
        ('q_ret_vol_eff',  'ret_vol_eff_score', p.w_ret_vol_eff, p.q_ret_vol_eff, 'G5_new'),
        ('q_mom_accel',    'mom_accel_score',  p.w_mom_accel,  p.q_mom_accel,  'G5_new'),
        ('q_pe',           'pe_score',         p.w_pe,         p.q_pe,         'G5_new'),
        ('q_pb',           'pb_score',         p.w_pb,         p.q_pb,         'G5_new'),
        ('q_upvol_share',  'upvol_share_score', p.w_upvol_share, p.q_upvol_share, 'G5_new'),
        ('q_vol_dec_days', 'vol_dec_days_score', p.w_vol_dec_days, p.q_vol_dec_days, 'G5_new'),
        ('q_close2low',    'close2low_score',  p.w_close2low,  p.q_close2low,  'G5_new'),
        ('q_vs_yellow',    'vs_yellow_score',  p.w_vs_yellow,  p.q_vs_yellow,  'G5_new'),
        ('q_body_small',   'body_small_score', p.w_body_small, p.q_body_small, 'G5_new'),
        ('q_us_small',     'us_small_score',   p.w_us_small,   p.q_us_small,   'G5_new'),
        ('q_vs_60h',       'vs_60h_score',     p.w_vs_60h,     p.q_vs_60h,     'G5_new'),
        ('q_vol10',        'vol10_score',      p.w_vol10,      p.q_vol10,      'G5_new'),
        ('q_dif_mom',      'dif_mom_score',    p.w_dif_mom,    p.q_dif_mom,    'G5_new'),
    ]
    fac.extend(g5)

    # G6: Tier1 mean-reversion (NEW)
    g6 = [
        ('q_wr',           'wr_score',         p.w_wr,         p.q_wr,         'G6_t1_mr'),
        ('q_bias',         'bias_score',       p.w_bias,       p.q_bias,       'G6_t1_mr'),
        ('q_bb_pct',       'bb_pct_score',     p.w_bb_pct,     p.q_bb_pct,     'G6_t1_mr'),
        ('q_vol_lowest',   'vol_lowest_score', p.w_vol_lowest, p.q_vol_lowest, 'G6_t1_mr'),
        ('q_pb_green_ratio', 'pb_green_ratio_score', p.w_pb_green_ratio, p.q_pb_green_ratio, 'G6_t1_mr'),
        ('q_max_dd_day',   'max_dd_day_score', p.w_max_dd_day, p.q_max_dd_day, 'G6_t1_mr'),
        ('q_red_vol_dec',  'red_vol_dec_score', p.w_red_vol_dec, p.q_red_vol_dec, 'G6_t1_mr'),
        ('q_vol_dec_accel','vol_dec_accel_score', p.w_vol_dec_accel, p.q_vol_dec_accel, 'G6_t1_mr'),
        ('q_yellow_slope', 'yellow_slope_score', p.w_yellow_slope, p.q_yellow_slope, 'G6_t1_mr'),
        ('q_adx',          'adx_score',        p.w_adx,        p.q_adx,        'G6_t1_mr'),
    ]
    fac.extend(g6)

    # G7: Tier1 supplement (NEW)
    g7 = [
        ('q_rsi_turn',     'rsi_turn_score',   p.w_rsi_turn,   p.q_rsi_turn,   'G7_t1_sup'),
        ('q_dist_low',     'dist_low_score',   p.w_dist_low,   p.q_dist_low,   'G7_t1_sup'),
    ]
    fac.extend(g7)

    # G8: Tier2 industry (NEW - Phase 2)
    g8 = [
        ('q_ind_rank',     'ind_rank_score',   p.w_ind_rank,   p.q_ind_rank,   'G8_t2_ind'),
        ('q_ind_dev',      'ind_dev_score',    p.w_ind_dev,    p.q_ind_dev,    'G8_t2_ind'),
        ('q_concept_cnt',  'concept_cnt_score', p.w_concept_cnt, p.q_concept_cnt, 'G8_t2_ind'),
    ]
    fac.extend(g8)

    # G9: Tier3 index (NEW - Phase 2)
    g9 = [
        ('q_rel_str',      'rel_str_score',    p.w_rel_str,    p.q_rel_str,    'G9_t3_idx'),
        ('q_beta',         'beta_score',       p.w_beta,       p.q_beta,       'G9_t3_idx'),
        ('q_alpha',        'alpha_score',      p.w_alpha,      p.q_alpha,      'G9_t3_idx'),
        ('q_mkt_state',    'mkt_state_score',  p.w_mkt_state,  p.q_mkt_state,  'G9_t3_idx'),
    ]
    fac.extend(g9)

    # G10: Tier4 minute (NEW - Phase 3)
    g10 = [
        ('q_vwap_dev',     'vwap_dev_score',   p.w_vwap_dev,   p.q_vwap_dev,   'G10_t4_min'),
        ('q_tail_lift',    'tail_lift_score',  p.w_tail_lift,  p.q_tail_lift,  'G10_t4_min'),
        ('q_open_weak',    'open_weak_score',  p.w_open_weak,  p.q_open_weak,  'G10_t4_min'),
        ('q_pm_vol',       'pm_vol_score',     p.w_pm_vol,     p.q_pm_vol,     'G10_t4_min'),
    ]
    fac.extend(g10)

    # V1 fusion
    v1 = [
        ('q_pattern_sim',  'pattern_sim_score', p.w_pattern_sim, p.q_pattern_sim, 'V1_fusion'),
        ('q_vol_resonance','vol_resonance_score', p.w_vol_resonance, p.q_vol_resonance, 'V1_fusion'),
        ('q_limit_penalty','limit_penalty_score', p.w_limit_penalty, p.q_limit_penalty, 'V1_fusion'),
        ('q_hist_bonus',   'hist_bonus_score',  p.w_hist_bonus,  p.q_hist_bonus,  'V1_fusion'),
    ]
    fac.extend(v1)

    return fac


# ============================================================
# SWEEP_PRESETS: pre-defined sweep spaces
# ============================================================

SWEEP_PRESETS = {
    # V1 wave module thresholds
    "wave_gain": {"wave_max_gain_pct": [30, 40, 50, 60, 80, 100]},
    "wave_turnover": {"wave_max_turnover_sum": [50, 70, 90, 120, 150]},
    "wave_red_green": {"wave_red_green_vol_ratio": [0.8, 1.0, 1.2, 1.5, 2.0]},
    "wave_health_ratio": {"wave_health_surge_vol_ratio": [1.2, 1.5, 2.0, 2.5, 3.0]},
    "wave_break_width": {"wave_break_stop_width": [0.02, 0.05, 0.08, 0.10]},
    "surge_gain": {"surge_min_gain_pct": [2.0, 3.0, 4.0, 5.0, 6.0]},
    "surge_vol": {"surge_vol_vs_ma5": [0.5, 0.8, 1.0, 1.2, 1.5]},
    "group_gap": {"group_forward_gap_max": [2, 3, 4, 5, 7]},
    "group_back_gap": {"group_back_merge_gap_max": [7, 10, 15, 20, 30]},
    "shadow_ratio": {"shadow_upper_ratio": [0.5, 0.618, 0.7, 0.8]},

    # Threshold filters
    "j_max": {"j_max": [20, 25, 30, 35, 40]},
    "j_min": {"j_min": [-20, -15, -10, -5, 0]},
    "vol_peak": {"vol_vs_wave_peak_max": [0.5, 0.7, 0.9, 1.0, 1.2]},
    "vol_ma5": {"vol_ratio_ma5_max": [0.8, 1.0, 1.2, 1.5, 2.0]},
    "turnover": {"turnover_max": [2, 4, 6, 8, 10]},
    "ret5d": {"ret_5d_min": [-20, -15, -12, -8, -5]},
    "white_slope": {"white_slope_min": [-0.5, -0.3, -0.1, 0.0, 0.1]},
    "pe": {"pe_max": [20, 30, 50, 80, 100, 200]},
    "pb": {"pb_max": [3, 5, 8, 10, 15, 20]},
    "cs_shadow": {"cs_shadow_max": [40, 50, 60, 70, 80, 100]},

    # Position
    "bowl_near": {"bowl_near_pct": [1.0, 2.0, 3.5, 5.0, 7.0]},
    "vs_white": {"vs_white_low": [-10, -8, -5.5, -4, -2], "vs_white_high": [1, 2.5, 4, 6, 10]},
    "vs_yellow": {"vs_yellow_min": [-5, -3, 0, 2, 5]},

    # Exit rules
    "stop_loss": {"stop_loss_width": [0.02, 0.05, 0.08, 0.10]},
    "t3_return": {"t_plus_3_min_return": [0.0, 0.02, 0.04, 0.06]},
    "max_hold": {"max_hold_days": [20, 25, 35, 45, 60]},
    "position_pct": {"position_pct": [0.10, 0.15, 0.20, 0.25, 0.30]},
    "max_pos": {"max_positions": [4, 6, 8, 10, 15]},
}

# ============================================================
# Factor count summary
# ============================================================

def count_factors_by_group(fac_list):
    """Count factors per group."""
    from collections import Counter
    return Counter(g for _, _, _, _, g in fac_list)

def count_enabled_factors(fac_list):
    """Count enabled factors."""
    return sum(1 for _, _, _, enabled, _ in fac_list if enabled)


if __name__ == "__main__":
    p = B1V3Params()
    fac = build_fac_list(p)
    print(f"Total factors: {len(fac)}")
    print(f"Enabled factors: {count_enabled_factors(fac)}")
    print("\nBy group:")
    for g, c in sorted(count_factors_by_group(fac).items()):
        print(f"  {g}: {c}")
    print(f"\nTotal params: {len([f for f in dir(B1V3Params) if not f.startswith('_')])}")
