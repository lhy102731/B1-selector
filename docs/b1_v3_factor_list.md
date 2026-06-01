# B1 V3 因子清单（84个）

## 分类说明

| 标签 | 含义 |
|------|------|
| ✅ 已有 | V2 FAC列表中已实现且正常工作的因子 |
| 🔧 待修复 | V2已有但存在bug或数据依赖问题的因子 |
| 🆕 T1 | 新增，零成本日线可算 |
| 🆕 T2 | 新增，需加载concept.json |
| 🆕 T3 | 新增，需下载3只指数日线 |
| 🆕 T4 | 新增，需分钟线（候选股按需获取） |

---

## 一、结构硬过滤（A类，5个）

非因子，是层级过滤条件：

| # | 条件 | 默认值 | 来源 |
|---|------|--------|------|
| A1 | white > yellow | True | V1/V2 |
| A2 | DIF > dif_min | 0.05 | V1/V2 |
| A3 | 60日未翻倍 | True | V1/V2 |
| A4 | 市值 >= cap_min | 20亿 | V1/V2 |
| A5 | 碗区位置（或洗盘） | near_pct=3.5% | V1 |

---

## 二、阈值过滤（B类，9个）

超阈值=硬排除，界内=评分因子：

| # | 条件 | 默认值 | 界内评分因子 | 来源 |
|---|------|--------|------------|------|
| B1 | J值范围 | j_max=30, j_min=-15 | q_J | V1/V2 |
| B2 | 缩量(V1方式) | vol_vs_wave_peak_max=0.9 | — | V1 |
| B3 | 缩量(V2方式) | vol_ratio_ma5_max=1.5 | q_vol_sh | V2 |
| B4 | 换手率上限 | turnover_max=6% | — | V2 |
| B5 | 5日回报下限 | ret_5d_min=-12% | q_ret5 | V2 |
| B6 | 白线斜率下限 | white_slope_min=-0.3 | q_slope | V2 |
| B7 | PE上限 | pe_max=80 | q_pe 🔧 | V2 |
| B8 | PB上限 | pb_max=8 | q_pb 🔧 | V2 |
| B9 | CS上影线百分位 | cs_shadow_max=70 | q_cs_shadow | V2 |

---

## 三、评分因子（D类，81个）

### G0：原始核心（11个，默认ON）✅

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D01 | q_J | max(0, 15-abs(J)) / 15 | 2.0 | ✅ |
| D02 | q_bowl | max(0, 4-abs(vs_white)) / 4 | 1.5 | ✅ |
| D03 | q_vol_sh | max(0, 1.5-vol_ratio_ma5) / 1.5 | 1.5 | ✅ |
| D04 | q_kd_dur | min(k_lt_d_days, 10) / 10 | 1.0 | ✅ |
| D05 | q_dif | min(max(DIF, 0), 3) / 3 | 0.8 | ✅ |
| D06 | q_slope | max(0, min(white_slope_5d, 2)) / 2 | 0.7 | ✅ |
| D07 | q_dif_dea | dif_gt_dea (0 or 1) | 0.5 | ✅ |
| D08 | q_ret5 | ret_5d处理 | 0.5 | ✅ |
| D09 | q_surge | surge_quality / 10 | 3.0 | ✅ |
| D10 | q_retrace | retrace_score / 3 | 2.0 | ✅ |
| D11 | q_nodist | no_dist_10d (0 or 1) | 1.0 | ✅ |

### G1：量能结构（5个，默认ON）✅

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D12 | q_dif_bull | dif_bull_div (0 or 1) | 2.0 | ✅ |
| D13 | q_vol_rec | vol_shrinking_recent (0 or 1) | 1.0 | ✅ |
| D14 | q_vol_ctr | vol_contracting (0 or 1) | 2.0 | ✅ |
| D15 | q_vol_price | vol_price_improving (0 or 1) | 1.5 | ✅ |
| D16 | q_net_up | net_up_positive (0 or 1) | 0.8 | ✅ |

### G2：MA+形态（10个，默认OFF）✅

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D17 | q_ma_struct | ma_structure_bull (0 or 1) | 1.5 | ✅ |
| D18 | q_j_bounce | j_bouncing (0 or 1) | 1.0 | ✅ |
| D19 | q_near_ma20 | near_ma20 (0 or 1) | 0.8 | ✅ |
| D20 | q_green | is_green (0 or 1) | 1.0 | ✅ |
| D21 | q_mom_imp | mom_improving (0 or 1) | 1.5 | ✅ |
| D22 | q_ma5_10 | ma5_10_tight (0 or 1) | 0.5 | ✅ |
| D23 | q_mod_over | moderate_oversold (0 or 1) | 1.5 | ✅ |
| D24 | q_pb_green | pb_last_green (0 or 1) | 1.0 | ✅ |
| D25 | q_anti_dist | anti_dist (0 or 1) | 1.5 | ✅ |
| D26 | q_vwap | vwap_mean_revert (0-1) | 1.0 | ✅ 🔧 |

### G3：横截面排名（19个，默认OFF）✅

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D27 | q_cs_close | cs_close_pos / 100 | 1.5 | ✅ |
| D28 | q_cs_shadow | cs_lower_shadow / 100 | 1.0 | ✅ |
| D29 | q_cs_bowl | cs_bowl / 100 | 1.5 | ✅ |
| D30 | q_cs_vol | cs_vol_shrink / 100 | 1.5 | ✅ |
| D31 | q_cs_dif | cs_dif_strong / 100 | 1.0 | ✅ |
| D32 | q_cs_J | cs_J_mid / 100 | 2.0 | ✅ |
| D33 | q_cs_range | cs_range_tight / 100 | 1.5 | ✅ |
| D34 | q_cs_bar_rev | cs_bar_reversal / 100 | 1.5 | ✅ |
| D35 | q_cs_up_tight | cs_upper_tight / 100 | 1.0 | ✅ |
| D36 | q_cs_retrace | cs_retrace_depth / 100 | 0.8 | ✅ |
| D37 | q_cs_trend | cs_trend_strong / 100 | 0.8 | 🔧 |
| D38 | q_cs_kd_dur | cs_oversold_duration / 100 | 0.8 | ✅ |
| D39 | q_cs_klow2 | cs_klow2 / 100 | 1.5 | ✅ |
| D40 | q_cs_kup2 | cs_kup2_small / 100 | 1.0 | ✅ |
| D41 | q_cs_kmid2 | cs_kmid2 / 100 | 1.0 | ✅ |
| D42 | q_cs_ksft2 | cs_ksft2 / 100 | 1.2 | ✅ |
| D43 | q_cs_klen | cs_klen_tight / 100 | 0.8 | ✅ |

### G4：Qlib（4个，默认OFF）✅

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D44 | q_rsi | max(0, 40-rsi_14) / 40 | 1.0 | ✅ |
| D45 | q_atr | max(0, 3-atr_14) / 3 | 0.8 | ✅ |
| D46 | q_upvol | up_vol_ratio (0-1) | 1.0 | ✅ |
| D47 | q_obv_div | obv_div (0 or 1) | 1.5 | ✅ |

### G5：新因子（13个，默认OFF）✅

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D48 | q_ret_vol_eff | max(0, 5-ret_vol_eff) / 5 | 1.0 | ✅ |
| D49 | q_mom_accel | max(0, mom_accel) / 3 | 1.5 | ✅ |
| D50 | q_pe | max(0, 80-pe)/80 (pe>0) | 0.8 | 🔧 |
| D51 | q_pb | max(0, 8-pb)/8 (pb>0) | 0.8 | 🔧 |
| D52 | q_upvol_share | up_day_vol_share | 1.5 | ✅ |
| D53 | q_vol_dec_days | min(vol_dec_days, 5) / 5 | 1.0 | ✅ |
| D54 | q_close2low | close_to_low | 1.2 | ✅ |
| D55 | q_vs_yellow | max(0, 10-abs(vs_yellow))/10 | 1.0 | ✅ |
| D56 | q_body_small | max(0, 3-abs(candle_body))/3 | 1.0 | ✅ |
| D57 | q_us_small | max(0, 2-upper_shadow)/2 | 0.8 | ✅ |
| D58 | q_vs_60h | max(0, abs(vs_60d_high)-5)/10 | 0.8 | ✅ |
| D59 | q_vol10 | max(0, 5-volatility_10d)/5 | 0.8 | ✅ |
| D60 | q_dif_mom | max(0, -dif_momentum)/3 | 0.8 | ✅ |

### G6：Tier 1 超卖与均值回归（10个，默认OFF）🆕

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D61 | q_wr | (100-WR)/100, WR越低越好 | 2.0 | 🆕 T1 |
| D62 | q_bias | max(0, 15-abs(BIAS20))/15, 距MA20乖离 | 1.5 | 🆕 T1 |
| D63 | q_bb_pct | 1-abs(BB_pct-0.5)*2, 布林带位置适中最好 | 1.5 | 🆕 T1 |
| D64 | q_vol_lowest | vol为N日最低=1, 地量 | 2.0 | 🆕 T1 |
| D65 | q_pb_green_ratio | 回撤中阳线占比(0-1) | 1.5 | 🆕 T1 |
| D66 | q_max_dd_day | max(0, 8+max_dd_day)/8, 最大单日跌幅 | 1.0 | 🆕 T1 |
| D67 | q_red_vol_dec | 阴线量递减趋势(0-1) | 1.5 | 🆕 T1 |
| D68 | q_vol_dec_accel | 缩量加速度, >0=加速缩 | 1.5 | 🆕 T1 |
| D69 | q_yellow_slope | max(0, min(yellow_slope,3))/3 | 1.0 | 🆕 T1 |
| D70 | q_adx | min(ADX14, 40)/40, 趋势强度 | 1.0 | 🆕 T1 |

### G7：Tier 1 补充改进（2个，默认OFF）🆕

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D71 | q_rsi_turn | RSI_5d_change>0=1, 低位拐头 | 1.5 | 🆕 T1 |
| D72 | q_dist_low | 距60日前低距离/止损宽度, 风险收益比 | 1.0 | 🆕 T1 |

### G8：Tier 2 行业/概念（3个，默认OFF）🆕

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D73 | q_ind_rank | 行业内quality_score百分位/100 | 2.0 | 🆕 T2 |
| D74 | q_ind_dev | 个股质量分偏离行业均值(std单位) | 1.5 | 🆕 T2 |
| D75 | q_concept_cnt | min(所属概念数, 10)/10 | 1.0 | 🆕 T2 |

### G9：Tier 3 指数相关（4个，默认OFF）🆕

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D76 | q_rel_str | 个股20日涨幅/指数20日涨幅, >1=跑赢 | 1.5 | 🆕 T3 |
| D77 | q_beta | 1-abs(beta_60d-0.5), 中低Beta最优 | 1.0 | 🆕 T3 |
| D78 | q_alpha | max(0, alpha_60d)/5, 超额收益 | 2.0 | 🆕 T3 |
| D79 | q_mkt_state | 指数>MA20=1, 市场适合做B1 | 1.0 | 🆕 T3 |

### G10：Tier 4 分钟线（4个，默认OFF）🆕

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D80 | q_vwap_dev | 1-abs(close/VWAP-1)*10, 越小越好 | 1.5 | 🆕 T4 |
| D81 | q_tail_lift | 尾盘30min涨幅/全天涨幅, 尾盘拉升 | 1.0 | 🆕 T4 |
| D82 | q_open_weak | 低开幅度<2%=1, 低开回踩更健康 | 0.8 | 🆕 T4 |
| D83 | q_pm_vol | 下午量/全天量, 缩量在下午=衰竭 | 1.0 | 🆕 T4 |

### V1独有（3个，默认OFF）

| # | 因子 | 计算 | 权重 | 标签 |
|---|------|------|------|------|
| D84 | q_pattern_sim | V1形态相似度% / 100 | 3.0 | 🆕 V1融合 |
| D85 | q_vol_resonance | 最高价日量排名=1 → 1.0, =2 → 0 | 1.0 | 🆕 V1融合 |
| D86 | q_limit_penalty | 一字涨停=-2, 缩量涨停=-1 | 0.5 | 🆕 V1融合 |

---

## 四、Bug修复清单（3个）

| # | Bug | 现状 | 修复 |
|---|-----|------|------|
| 🔧1 | q_cs_trend 恒为0 | white_yellow_gap从未计算 | 补上计算: `(white-yellow)/yellow*100` |
| 🔧2 | q_pe 恒为0 | pe=-1时max(0,80-(-1))/80=1.01，实际上是错的 | 无PE缓存时跳过该因子(tw不加) |
| 🔧3 | q_pb 恒为0 | 同上 | 同上 |
| 🔧4 | q_vwap 恒为0 | parquet中amount列可能不存在 | 无amount时跳过 |

---

## 五、统计汇总

| 分组 | 数量 | 默认ON | 状态 |
|------|------|--------|------|
| G0 原始核心 | 11 | 11 | ✅ |
| G1 量能结构 | 5 | 5 | ✅ |
| G2 MA+形态 | 10 | 0 | ✅ |
| G3 横截面排名 | 19 | 0 | ✅ (1个🔧) |
| G4 Qlib | 4 | 0 | ✅ |
| G5 新因子 | 13 | 0 | ✅ (2个🔧) |
| G6 Tier1超卖回归 | 10 | 0 | 🆕 |
| G7 Tier1补充 | 2 | 0 | 🆕 |
| G8 Tier2行业 | 3 | 0 | 🆕 |
| G9 Tier3指数 | 4 | 0 | 🆕 |
| G10 Tier4分钟 | 4 | 0 | 🆕 |
| V1融合 | 3 | 0 | 🆕 |
| **合计** | **88** | **16** | |

> 注：加上A类5个硬过滤 + B类9个阈值过滤 = 共约102个可调节参数

---

## 六、数据依赖总览

| 数据 | 大小估计 | 用途 | 因子数 |
|------|---------|------|--------|
| indicators_cache/*.parquet | 已有(~5000文件) | 全部日线因子 | 78 |
| concept.json | 已有(~3MB) | 行业/概念因子 | 3 |
| baostock 指数日线 | ~50KB/年/指数 | 相对强度/Beta/Alpha | 4 |
| baostock PE/PB缓存 | 已有(412MB) | q_pe, q_pb | 2 |
| baostock 分钟线 | 待定 | VWAP/尾盘/开盘/盘中 | 4 |
