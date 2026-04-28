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
    
    def __init__(self, weights=None, tolerances=None):
        from strategy.pattern_config import SIMILARITY_WEIGHTS, MATCH_TOLERANCES
        self.weights = weights or SIMILARITY_WEIGHTS
        self.tolerances = tolerances or MATCH_TOLERANCES
    
    def match(self, candidate_features: dict, case_features: dict) -> dict:
        """
        计算候选股与案例的相似度
        返回0-1之间的分数
        """
        if not candidate_features or not case_features:
            return {"total_score": 0.0, "breakdown": {}}
        
        scores = {}
        
        # 1. 知行趋势线结构相似度
        if candidate_features.get("trend_structure") and case_features.get("trend_structure"):
            scores["trend_structure"] = self._calc_trend_similarity(
                candidate_features["trend_structure"],
                case_features["trend_structure"]
            )
        else:
            scores["trend_structure"] = 0.5
        
        # 2. KDJ状态相似度
        if candidate_features.get("kdj_state") and case_features.get("kdj_state"):
            scores["kdj_state"] = self._calc_kdj_similarity(
                candidate_features["kdj_state"],
                case_features["kdj_state"]
            )
        else:
            scores["kdj_state"] = 0.5
        
        # 3. 量能模式相似度
        if candidate_features.get("volume_pattern") and case_features.get("volume_pattern"):
            scores["volume_pattern"] = self._calc_volume_similarity(
                candidate_features["volume_pattern"],
                case_features["volume_pattern"]
            )
        else:
            scores["volume_pattern"] = 0.5
        
        # 4. 价格形态相似度（DTW）
        if candidate_features.get("price_shape") and case_features.get("price_shape"):
            scores["price_shape"] = self._calc_shape_similarity(
                candidate_features["price_shape"],
                case_features["price_shape"]
            )
        else:
            scores["price_shape"] = 0.5

        # 5. 异动期涨幅特征（新增）
        if candidate_features.get("move_strength") and case_features.get("move_strength"):
            scores["move_strength"] = self._calc_move_similarity(
                candidate_features["move_strength"],
                case_features["move_strength"]
            )
        else:
            scores["move_strength"] = 0.5

        # 6. 建仓健康度相似度
        if candidate_features.get("build_health") and case_features.get("build_health"):
            scores["build_health"] = self._calc_build_health_similarity(
                candidate_features["build_health"],
                case_features["build_health"]
            )
        else:
            scores["build_health"] = 0.5

        # 加权总分
        total_score = sum(
            scores[k] * self.weights.get(k, 0.2) for k in scores
        )
        
        return {
            "total_score": round(total_score * 100, 2),  # 转换为百分制
            "breakdown": {k: round(v * 100, 2) for k, v in scores.items()},
        }

    def _calc_move_similarity(self, cand: dict, case: dict) -> float:
        """异动期涨幅相似度"""
        similarities = []

        # 从配置读取容差
        avg_gain_tol = self.tolerances.get("move_avg_gain", 5)
        total_gain_tol = self.tolerances.get("move_total_gain", 10)

        # 平均异动涨幅相似
        if "move_avg_gain" in cand and "move_avg_gain" in case:
            gain_diff = abs(cand["move_avg_gain"] - case["move_avg_gain"])
            sim = max(0, 1 - gain_diff / avg_gain_tol)
            similarities.append(sim)

        # 最大异动涨幅相似
        if "move_max_gain" in cand and "move_max_gain" in case:
            max_diff = abs(cand["move_max_gain"] - case["move_max_gain"])
            sim = max(0, 1 - max_diff / avg_gain_tol)  # 可使用同一容差
            similarities.append(sim)

        # 异动总涨幅相似
        if "move_total_gain" in cand and "move_total_gain" in case:
            total_diff = abs(cand["move_total_gain"] - case["move_total_gain"])
            sim = max(0, 1 - total_diff / total_gain_tol)
            similarities.append(sim)

        # 异动天数相似
        if "move_days" in cand and "move_days" in case:
            days_diff = abs(cand["move_days"] - case["move_days"])
            sim = max(0, 1 - days_diff / 3)  # 容差3天
            similarities.append(sim)

        # 首次/末次涨幅比相似
        if "move_first_last_ratio" in cand and "move_first_last_ratio" in case:
            ratio_diff = abs(cand["move_first_last_ratio"] - case["move_first_last_ratio"])
            sim = max(0, 1 - ratio_diff / 1)  # 容差1（即100%差异）
            similarities.append(sim)

        return np.mean(similarities) if similarities else 0.5

    def _calc_build_health_similarity(self, cand: dict, case: dict) -> float:
        """建仓波健康度相似度"""
        similarities = []

        # 综合健康分相似
        if "build_health_score" in cand and "build_health_score" in case:
            score_diff = abs(cand["build_health_score"] - case["build_health_score"])
            sim = max(0, 1 - score_diff / 40)  # 容差40分
            similarities.append(sim)

        # 建仓涨幅相似（容差15%）
        if "build_gain" in cand and "build_gain" in case:
            gain_diff = abs(cand["build_gain"] - case["build_gain"])
            sim = max(0, 1 - gain_diff / 15)
            similarities.append(sim)

        # 异动换手累加相似（容差20%）
        if "surge_turnover_sum" in cand and "surge_turnover_sum" in case:
            turnover_diff = abs(cand["surge_turnover_sum"] - case["surge_turnover_sum"])
            sim = max(0, 1 - turnover_diff / 20)
            similarities.append(sim)

        # 均线多头评分相似（容差30分）
        if "ma_score" in cand and "ma_score" in case:
            ma_diff = abs(cand["ma_score"] - case["ma_score"])
            sim = max(0, 1 - ma_diff / 30)
            similarities.append(sim)

        # 涨停状态（有涨停扣分，与案例匹配给高分）
        if "has_limit_up" in cand and "has_limit_up" in case:
            if cand["has_limit_up"] == case["has_limit_up"]:
                similarities.append(1.0)
            else:
                similarities.append(0.3)  # 案例有涨停但候选没有，或不一致，大幅扣分

        # 一字涨停（更严格的扣分）
        if "has_one_word_limit" in cand and "has_one_word_limit" in case:
            if cand["has_one_word_limit"] == case["has_one_word_limit"]:
                similarities.append(1.0)
            else:
                similarities.append(0.2)

        return np.mean(similarities) if similarities else 0.5

    def _calc_trend_similarity(self, cand: dict, case: dict) -> float:
        """知行趋势线相似度 - 基于相对百分比偏离"""
        similarities = []
        
        # 从配置读取容差参数
        trend_ratio_tol = self.tolerances.get("trend_ratio", 0.10)
        price_bias_tol = self.tolerances.get("price_bias", 10)
        trend_spread_tol = self.tolerances.get("trend_spread", 10)
        
        # 1. short_vs_bullbear 比值相似
        if "short_vs_bullbear" in cand and "short_vs_bullbear" in case:
            ratio_diff = abs(cand["short_vs_bullbear"] - case["short_vs_bullbear"])
            sim = max(0, 1 - ratio_diff / trend_ratio_tol)
            similarities.append(sim)
        
        # 2. 斜率方向一致性（最重要）
        if "short_slope" in cand and "short_slope" in case:
            short_slope_same = (cand["short_slope"] > 0) == (case["short_slope"] > 0)
            if short_slope_same:
                slope_diff = abs(cand["short_slope"] - case["short_slope"])
                sim = max(0.7, 1 - slope_diff / 10)
            else:
                sim = max(0, 0.3 - abs(cand["short_slope"] - case["short_slope"]) / 20)
            similarities.append(sim)
        
        # 3. 是否在碗中（形态位置）
        if "is_in_bowl" in cand and "is_in_bowl" in case:
            if cand["is_in_bowl"] == case["is_in_bowl"]:
                similarities.append(1.0)
            else:
                similarities.append(0.2)
        
        # 4. 价格相对于短期趋势的偏离（百分比）
        cand_price_bias = cand.get("price_vs_short_pct", cand.get("price_vs_short", 0) * 100 - 100)
        case_price_bias = case.get("price_vs_short_pct", case.get("price_vs_short", 0) * 100 - 100)
        price_bias_diff = abs(cand_price_bias - case_price_bias)
        sim = max(0, 1 - price_bias_diff / price_bias_tol)
        similarities.append(sim)
        
        # 5. 趋势发散程度相似（百分比）
        cand_spread = cand.get("trend_spread_pct", cand.get("trend_spread", 0))
        case_spread = case.get("trend_spread_pct", case.get("trend_spread", 0))
        spread_diff = abs(cand_spread - case_spread)
        sim = max(0, 1 - spread_diff / trend_spread_tol)
        similarities.append(sim)
        
        # 6. 双线乖离率相似
        if "price_bias_pct" in cand and "price_bias_pct" in case:
            bias_diff = abs(cand["price_bias_pct"] - case["price_bias_pct"])
            sim = max(0, 1 - bias_diff / price_bias_tol)
            similarities.append(sim)
        
        return np.mean(similarities) if similarities else 0.5
    
    def _calc_kdj_similarity(self, cand: dict, case: dict) -> float:
        """KDJ状态相似度"""
        similarities = []
        
        # 从配置读取J值容差
        j_value_tol = self.tolerances.get("j_value", 30)
        
        # J值具体数值相似（使用配置的容差）
        if "j_value" in cand and "j_value" in case:
            j_diff = abs(cand["j_value"] - case["j_value"])
            sim = max(0, 1 - j_diff / j_value_tol)
            similarities.append(sim)
        
        # 金叉状态一致性
        if "k_cross_d" in cand and "k_cross_d" in case:
            if cand["k_cross_d"] == case["k_cross_d"]:
                similarities.append(1.0)
            else:
                similarities.append(0.6)
        
        # J值趋势方向
        if "j_rebound" in cand and "j_rebound" in case:
            if cand["j_rebound"] == case["j_rebound"]:
                similarities.append(1.0)
            else:
                similarities.append(0.7)

        # 新增：J值绝对低位（<10）给予额外奖励
        if cand.get("j_value", 100) < -5 and case.get("j_value", 100) < -5:
            similarities.append(1.3)  # 奖励20%
        elif cand.get("j_value", 100) < 13 and case.get("j_value", 100) < 13:
            similarities.append(1.1)
        else:
            j_position_map = {"低位": 0.9, "中位": 0.5, "高位": 0.1}
            cand_score = j_position_map.get(cand.get("j_position", ""), 0.3)
            case_score = j_position_map.get(case.get("j_position", ""), 0.3)
            sim = 1 - abs(cand_score - case_score)
            similarities.append(sim)

        # 新增：底背离特征（价格新低但J值未新低）
        if "j_divergence" in cand and "j_divergence" in case:
            if cand["j_divergence"] == case["j_divergence"]:
                similarities.append(1.0)
            else:
                similarities.append(0.5)

        return np.mean(similarities) if similarities else 0.5

    def _calc_volume_similarity(self, cand: dict, case: dict) -> float:
        """
        量能模式相似度计算
        包含：成交量特征 + 换手率特征
        """
        similarities = []
        weights = []  # 用于加权平均

        # ========== 成交量相关特征（原有） ==========
        # 均量比
        if "avg_volume_ratio" in cand and "avg_volume_ratio" in case:
            ratio_diff = abs(cand["avg_volume_ratio"] - case["avg_volume_ratio"])
            sim = max(0, 1 - ratio_diff / 1.5)
            similarities.append(sim)
            weights.append(0.15)  # 权重15%

        # 缩量后放量模式
        if "shrink_then_expand" in cand and "shrink_then_expand" in case:
            if cand["shrink_then_expand"] == case["shrink_then_expand"]:
                sim = 1.0
            else:
                sim = 0.5
            similarities.append(sim)
            weights.append(0.15)

        # 关键K线数量
        if "key_candles_count" in cand and "key_candles_count" in case:
            count_diff = abs(cand["key_candles_count"] - case["key_candles_count"])
            sim = max(0, 1 - count_diff / 3)
            similarities.append(sim)
            weights.append(0.15)

        # 量能趋势分类
        if "volume_trend" in cand and "volume_trend" in case:
            if cand["volume_trend"] == case["volume_trend"]:
                sim = 1.0
            else:
                sim = 0.6
            similarities.append(sim)
            weights.append(0.15)

        # 最大量比
        if "max_volume_ratio" in cand and "max_volume_ratio" in case:
            max_vol_diff = abs(cand["max_volume_ratio"] - case["max_volume_ratio"])
            sim = max(0, 1 - max_vol_diff / 3)
            similarities.append(sim)
            weights.append(0.15)

        # 量能压缩度
        if "volume_compress" in cand and "volume_compress" in case:
            compress_diff = abs(cand["volume_compress"] - case["volume_compress"])
            sim = max(0, 1 - compress_diff / 0.3)
            similarities.append(sim)
            weights.append(0.15)

        # ========== 新增：换手率相关特征 ==========
        # 换手率比值
        if "turnover_ratio" in cand and "turnover_ratio" in case:
            turn_ratio_diff = abs(cand["turnover_ratio"] - case["turnover_ratio"])
            # 容差设为 0.5（可配置）
            sim = max(0, 1 - turn_ratio_diff / 0.5)
            similarities.append(sim)
            weights.append(0.10)  # 权重15%

        # 最大换手率
        if "max_turnover" in cand and "max_turnover" in case:
            max_turn_diff = abs(cand["max_turnover"] - case["max_turnover"])
            sim = max(0, 1 - max_turn_diff / 5.0)  # 容差5%
            similarities.append(sim)
            weights.append(0.02)

        # 换手率趋势斜率
        if "turnover_slope" in cand and "turnover_slope" in case:
            slope_diff = abs(cand["turnover_slope"] - case["turnover_slope"])
            sim = max(0, 1 - slope_diff / 2.0)  # 容差2
            similarities.append(sim)
            weights.append(0.03)

        # ★ 重点关注：涨幅≥4%日的平均换手率
        if "big_gain_turnover_avg" in cand and "big_gain_turnover_avg" in case:
            big_gain_diff = abs(cand["big_gain_turnover_avg"] - case["big_gain_turnover_avg"])
            # 容差建议设为 2.0（即±2%换手率），可根据实际调整
            sim = max(0, 1 - big_gain_diff / 2.0)
            similarities.append(sim)
            weights.append(0.20)  # 给予最高权重20%

        if not similarities:
            return 0.5

        # 加权平均
        total_weight = sum(weights)
        if total_weight == 0:
            return 0.5
        weighted_sum = sum(s * w for s, w in zip(similarities, weights))
        return weighted_sum / total_weight

    def _calc_shape_similarity(self, cand: dict, case: dict) -> float:
        """价格形态相似度 - 使用平均距离 DTW，且序列已统一长度"""
        similarities = []

        # 从配置读取回撤容差
        drawdown_tol = self.tolerances.get("drawdown", 15)

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

        return np.mean(similarities) if similarities else 0.5
    
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
