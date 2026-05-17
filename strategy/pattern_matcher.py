"""
相似度计算引擎 - 支持多维度加权匹配
使用DTW进行形态相似度计算
"""
import numpy as np
from scipy.spatial.distance import euclidean


try:
    from fastdtw import fastdtw
    HAS_FASTDTW = True
except ImportError:
    HAS_FASTDTW = False
    print("fastdtw 未安装，将使用简化版DTW")


class PatternMatcher:
    """完美图形匹配器 - 支持从配置文件读取参数"""

    def __init__(self, weights=None, tolerances=None, exclude_limit_state=False):
        from strategy.pattern_config import SIMILARITY_WEIGHTS, MATCH_TOLERANCES
        self.weights = dict(weights or SIMILARITY_WEIGHTS)
        self.tolerances = tolerances or MATCH_TOLERANCES
        self.exclude_limit_state = exclude_limit_state
        if self.exclude_limit_state and 'limit_state' in self.weights:
            del self.weights['limit_state']
            # 将limit_state的权重按比例分配给其余维度
            total = sum(self.weights.values())
            for k in self.weights:
                self.weights[k] = round(self.weights[k] / total, 4)

    def match(self, candidate_features: dict, case_features: dict) -> dict:
        """新评分体系：volume/price_shape/move_power/trend/divergence/[limit_state]"""
        if not candidate_features or not case_features:
            return {"total_score": 0.0, "breakdown": {}}

        scores = {}

        # 1. volume — Effort：量是根基
        v_cand = candidate_features.get("volume_pattern") or {}
        v_case = case_features.get("volume_pattern") or {}
        scores["volume"] = self._calc_volume_similarity(v_cand, v_case)

        # 2. price_shape — Structure：DTW曲线匹配
        p_cand = candidate_features.get("price_shape") or {}
        p_case = case_features.get("price_shape") or {}
        scores["price_shape"] = self._calc_shape_similarity(p_cand, p_case)

        # 3. move_power — Cause→Effect：攻击力(吸收原move+build_gain)
        m_cand = candidate_features.get("move_strength") or {}
        m_case = case_features.get("move_strength") or {}
        b_cand = candidate_features.get("build_health") or {}
        b_case = case_features.get("build_health") or {}
        scores["move_power"] = self._calc_move_power(m_cand, m_case, b_cand, b_case)

        # 4. trend — Context：趋势环境(吸收ma_score)
        t_cand = candidate_features.get("trend_structure") or {}
        t_case = case_features.get("trend_structure") or {}
        scores["trend"] = self._calc_trend_similarity(t_cand, t_case, b_cand, b_case)

        # 5. divergence — Hidden：背离信号(原kdj精简)
        k_cand = candidate_features.get("kdj_state") or {}
        k_case = case_features.get("kdj_state") or {}
        scores["divergence"] = self._calc_divergence_similarity(k_cand, k_case)

        # 6. limit_state — Confirmation：涨停状态(可选)
        if not self.exclude_limit_state:
            scores["limit_state"] = self._calc_limit_similarity(b_cand, b_case)

        total_score = sum(scores[k] * self.weights.get(k, 0.1) for k in scores)
        return {
            "total_score": round(total_score * 100, 2),
            "breakdown": {k: round(v * 100, 2) for k, v in scores.items()},
        }

    def _calc_move_power(self, move_cand: dict, move_case: dict,
                          build_cand: dict, build_case: dict) -> float:
        """攻击力：异动涨幅(原move_strength) + 建仓涨幅(原build_gain)"""
        sims = []

        # 3.1 total_gain — 异动总涨幅 (容差10%, >60%衰减)
        tg = move_cand.get("move_total_gain", 0)
        tc = move_case.get("move_total_gain", 0)
        # 超过60%的涨幅惩罚
        t_eff = 1.0 if tg <= 60 else 0.5
        diff = abs(tg - tc) * t_eff
        sims.append(max(0, 1 - diff / 20))

        # 3.2 gain_quality — 涨幅/换手效率(吸收build_gain, 新增turnover效率)
        bg_cand = build_cand.get("build_gain", 0)
        bg_case = build_case.get("build_gain", 0)
        st_cand = build_cand.get("surge_turnover_sum", 0.01)
        st_case = build_case.get("surge_turnover_sum", 0.01)
        eff_cand = bg_cand / max(st_cand, 0.01)
        eff_case = bg_case / max(st_case, 0.01)
        eff_diff = abs(eff_cand - eff_case) / 5
        sims.append(max(0, 1 - eff_diff))

        # 3.3 move_rhythm — 节奏稳定性
        if "move_first_last_ratio" in move_cand and "move_first_last_ratio" in move_case:
            r_diff = abs(move_cand["move_first_last_ratio"] - move_case["move_first_last_ratio"])
            sims.append(max(0, 1 - r_diff / 1.5))
        if "move_days" in move_cand and "move_days" in move_case:
            d_diff = abs(move_cand["move_days"] - move_case["move_days"])
            sims.append(max(0, 1 - d_diff / 5))

        # 3.4 avg_gain
        if "move_avg_gain" in move_cand and "move_avg_gain" in move_case:
            a_diff = abs(move_cand["move_avg_gain"] - move_case["move_avg_gain"])
            sims.append(max(0, 1 - a_diff / 8))

        return np.mean(sims) if sims else 0.5

    def _calc_divergence_similarity(self, kdj_cand: dict, kdj_case: dict) -> float:
        """背离信号：J底背离 + 量价背离 + 金叉"""
        sims = []

        # 5.1 j_divergence (50%)
        if "j_divergence" in kdj_cand and "j_divergence" in kdj_case:
            sims.append(1.0 if kdj_cand["j_divergence"] == kdj_case["j_divergence"] else 0.5)

        # 5.2 j_value 匹配 (30% — 从原kdj继承，权重降低)
        if "j_value" in kdj_cand and "j_value" in kdj_case:
            j_diff = abs(kdj_cand["j_value"] - kdj_case["j_value"])
            sims.append(max(0, 1 - j_diff / 30))

        # 5.3 kdj_golden_cross (20%)
        if "k_cross_d" in kdj_cand and "k_cross_d" in kdj_case:
            sims.append(1.0 if kdj_cand["k_cross_d"] == kdj_case["k_cross_d"] else 0.6)

        return np.mean(sims) if sims else 0.5

    def _calc_limit_similarity(self, build_cand: dict, build_case: dict) -> float:
        """涨停状态匹配（从原build_health独立）"""
        sims = []

        # 6.1 has_limit_up (60%)
        if "has_limit_up" in build_cand and "has_limit_up" in build_case:
            sims.append(1.0 if build_cand["has_limit_up"] == build_case["has_limit_up"] else 0.3)

        # 6.2 has_one_word_limit (40%)
        if "has_one_word_limit" in build_cand and "has_one_word_limit" in build_case:
            sims.append(1.0 if build_cand["has_one_word_limit"] == build_case["has_one_word_limit"] else 0.2)

        return np.mean(sims) if sims else 0.5

    def _calc_trend_similarity(self, cand: dict, case: dict,
                                build_cand: dict = None, build_case: dict = None) -> float:
        """趋势环境4维：斜率方向+价格位置+均线共振(吸收ma_score)+趋势发散"""
        sims = []

        # 4.1 slope_direction (35%) — 方向一致性开关
        if "short_slope" in cand and "short_slope" in case:
            same_dir = (cand["short_slope"] > 0) == (case["short_slope"] > 0)
            if same_dir:
                slope_diff = abs(cand["short_slope"] - case["short_slope"])
                sims.append(max(0.7, 1 - slope_diff / 10))
            else:
                sims.append(max(0, 0.3 - abs(cand["short_slope"] - case["short_slope"]) / 20))

        # 4.2 price_position (30%) — 价格偏离趋势线幅度
        cand_bias = cand.get("price_vs_short_pct", cand.get("price_vs_short", 0) * 100 - 100)
        case_bias = case.get("price_vs_short_pct", case.get("price_vs_short", 0) * 100 - 100)
        bias_diff = abs(cand_bias - case_bias)
        sims.append(max(0, 1 - bias_diff / 10))

        # 4.3 ma_resonance (20%) — 均线多头共振（从build_health迁入）
        if build_cand and build_case:
            if "ma_score" in build_cand and "ma_score" in build_case:
                ma_diff = abs(build_cand["ma_score"] - build_case["ma_score"])
                sims.append(max(0, 1 - ma_diff / 30))

        # 4.4 trend_spread (15%) — 双线发散度
        cand_spread = cand.get("trend_spread_pct", cand.get("trend_spread", 0))
        case_spread = case.get("trend_spread_pct", case.get("trend_spread", 0))
        spread_diff = abs(cand_spread - case_spread)
        sims.append(max(0, 1 - spread_diff / 10))

        return np.mean(sims) if sims else 0.5
    
    def _calc_volume_similarity(self, cand: dict, case: dict) -> float:
        """量能4维：缩量质量(40%) + 放量确认(25%) + Spring检测(20%) + 换手健康(15%)"""
        sims = []

        # 1.1 shrink_quality (40%) — vol_ratio越小越缩量，黄线附近增强
        vr_c = cand.get("volume_compress", 0.5)
        vr_e = case.get("volume_compress", 0.5)
        shrink_c = 1 - min(vr_c, 1.5) / 1.5
        shrink_e = 1 - min(vr_e, 1.5) / 1.5
        shrink_diff = abs(shrink_c - shrink_e)
        sims.append(max(0, 1 - shrink_diff))

        # 1.2 expand_confirm (25%) — 缩量后是否放量回升
        if "shrink_then_expand" in cand and "shrink_then_expand" in case:
            sims.append(1.0 if cand["shrink_then_expand"] == case["shrink_then_expand"] else 0.3)

        # 1.3 spring_detect (20%)
        spring_c = cand.get("spring_detect", False)
        spring_e = case.get("spring_detect", False)
        sims.append(1.0 if spring_c == spring_e else 0.5)

        # 1.4 turnover_health (15%) — 大阳换手在2%~15%满分
        if "big_gain_turnover_avg" in cand and "big_gain_turnover_avg" in case:
            t_c = cand["big_gain_turnover_avg"]
            t_e = case["big_gain_turnover_avg"]
            # 样板合理区间得分
            t_ok_c = 1.0 if 2 <= t_c <= 15 else max(0, 1 - abs(t_c - 8.5) / 10)
            t_ok_e = 1.0 if 2 <= t_e <= 15 else max(0, 1 - abs(t_e - 8.5) / 10)
            sims.append(1 - abs(t_ok_c - t_ok_e))

        return np.mean(sims) if sims else 0.5

    def _calc_shape_similarity(self, cand: dict, case: dict) -> float:
        """价格形态相似度 - 使用平均距离 DTW，且序列已统一长度"""
        similarities = []

        # 从配置读取回撤容差
        drawdown_tol = self.tolerances.get("drawdown", 15)

        dtw_appended = False
        # 使用平均距离 DTW
        if "normalized_curve" in cand and "normalized_curve" in case:
            cand_curve = np.array(cand["normalized_curve"])
            case_curve = np.array(case["normalized_curve"])

            if len(cand_curve) > 0 and len(case_curve) > 0:
                if HAS_FASTDTW:
                    try:
                        distance, path = fastdtw(cand_curve, case_curve, dist=euclidean, radius=2)
                        path_length = len(path)
                        avg_distance = distance / path_length if path_length > 0 else 0
                        # 最大平均距离经验值 0.15（可根据实际调整）
                        curve_sim = max(0, 1 - avg_distance / 0.15)
                    except Exception:
                        curve_sim = self._simple_dtw(cand_curve, case_curve)
                else:
                    curve_sim = self._simple_dtw(cand_curve, case_curve)
                similarities.append(curve_sim)
                dtw_appended = True

        # 回撤幅度相似（使用配置容差）
        if "max_drawdown" in cand and "max_drawdown" in case:
            drawdown_diff = abs(cand["max_drawdown"] - case["max_drawdown"])
            sim = max(0, 1 - drawdown_diff / drawdown_tol)
            similarities.append(sim)

        # 突破力度相似
        if "breakout_strength" in cand and "breakout_strength" in case:
            breakout_diff = abs(cand["breakout_strength"] - case["breakout_strength"])
            sim = max(0, 1 - breakout_diff / 5)
            similarities.append(sim)

        # 整体趋势方向一致性
        if "overall_trend" in cand and "overall_trend" in case:
            if cand["overall_trend"] == case["overall_trend"]:
                similarities.append(1.0)
            else:
                similarities.append(0.5)

        # 盘整天数接近度
        if "consolidation_days" in cand and "consolidation_days" in case:
            days_diff = abs(cand["consolidation_days"] - case["consolidation_days"])
            sim = max(0, 1 - days_diff / 10)
            similarities.append(sim)

        # 价格是否回踩短期趋势线
        if "near_short_trend" in cand and "near_short_trend" in case:
            if cand["near_short_trend"] and case["near_short_trend"]:
                similarities.append(1.0)
            else:
                similarities.append(0.3)

        # 波动率收窄相似度
        if "volatility_shrink" in cand and "volatility_shrink" in case:
            shrink_diff = abs(cand["volatility_shrink"] - case["volatility_shrink"])
            sim = max(0, 1 - shrink_diff / 0.5)
            similarities.append(sim)

        # DTW权重3x, 其余各1x
        if not similarities:
            return 0.5
        if dtw_appended and len(similarities) > 1:
            weights = [3.0] + [1.0] * (len(similarities) - 1)
        else:
            weights = [1.0] * len(similarities)
        return float(np.average(similarities, weights=weights))
    
    def _simple_dtw(self, seq1: np.ndarray, seq2: np.ndarray) -> float:
        """简化版DTW（当fastdtw不可用时使用）"""
        n, m = len(seq1), len(seq2)
        if n == 0 or m == 0:
            return 0.0
        
        # 如果长度不同，进行线性插值到相同长度
        if n != m:
            target_len = max(n, m)
            if n < target_len:
                seq1 = np.interp(
                    np.linspace(0, n-1, target_len),
                    np.arange(n),
                    seq1
                )
            if m < target_len:
                seq2 = np.interp(
                    np.linspace(0, m-1, target_len),
                    np.arange(m),
                    seq2
                )
        
        # 计算欧氏距离
        distance = np.sqrt(np.sum((seq1 - seq2) ** 2))
        max_dist = np.sqrt(len(seq1))
        
        similarity = max(0, 1 - distance / max_dist) if max_dist > 0 else 0
        return similarity
