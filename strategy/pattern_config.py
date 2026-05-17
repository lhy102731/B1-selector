"""
B1完美图形配置管理
新增案例只需在这里添加配置，无需改代码
为后续B2、B3等扩展预留空间

配置优先级：
1. 首先读取 config/strategy_params.yaml 中的 B1PatternMatch 配置
2. 如果YAML中未配置，使用本文件中的默认值
"""

import os
import yaml
from pathlib import Path


def _load_yaml_config():
    """从YAML配置文件加载B1PatternMatch配置"""
    config_path = Path(__file__).parent.parent / 'config' / 'strategy_params.yaml'
    
    if not config_path.exists():
        return {}
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config.get('B1PatternMatch', {})
    except Exception as e:
        print(f"加载B1PatternMatch配置失败: {e}，使用默认值")
        return {}


# 加载YAML配置
_yaml_config = _load_yaml_config()

# B1完美图形案例配置（10个历史成功案例）
# 注：日期为"选股系统选出的买入日期"，不是突破日
B1_PERFECT_CASES = [
    {
        "id": "case_001",
        "name": "华纳药厂",
        "code": "688799",
        "breakout_date": "2025-05-12",
        "lookback_days": 15,
        "tags": ["科创板", "医药"],
        "description": "杯型整理+缩量+J值低位"
    },
    {
        "id": "case_002",
        "name": "宁波韵升",
        "code": "600366",
        "breakout_date": "2025-08-06",
        "lookback_days": 28,
        "tags": ["主板", "稀土永磁"],
        "description": "回落短期趋势线+量能平稳+J值中位"
    },
    {
        "id": "case_003",
        "name": "微芯生物",
        "code": "688321",
        "breakout_date": "2025-06-20",
        "lookback_days": 17,
        "tags": ["科创板", "医药"],
        "description": "平台整理+缩量后放量+J值低位"
    },
    {
        "id": "case_004",
        "name": "方正科技",
        "code": "600601",
        "breakout_date": "2025-07-23",
        "lookback_days": 35,
        "tags": ["主板", "科技"],
        "description": "靠近多空线+量能平稳+J值中位"
    },
    {
        "id": "case_005",
        "name": "圣阳股份",
        "code": "002580",
        "breakout_date": "2026-04-08",
        "lookback_days": 14,
        "tags": ["创业板", "科技"],
        "description": "靠近多空线+价格震荡+J值低位"
    },
    {
        "id": "case_006",
        "name": "国轩高科",
        "code": "002074",
        "breakout_date": "2025-08-04",
        "lookback_days": 12,
        "tags": ["中小板", "新能源"],
        "description": "靠近短期趋势线+量能平稳+J值低位"
    },
    {
        "id": "case_007",
        "name": "野马电池",
        "code": "605378",
        "breakout_date": "2025-08-01",
        "lookback_days": 29,
        "tags": ["主板", "电池"],
        "description": "持续缩量+J值深度低位+趋势下行"
    },
    {
        "id": "case_008",
        "name": "光电股份",
        "code": "600184",
        "breakout_date": "2025-07-10",
        "lookback_days": 17,
        "tags": ["主板", "军工"],
        "description": "缩量后放量+J值低位+趋势上行"
    },
    {
        "id": "case_009",
        "name": "新瀚新材",
        "code": "301076",
        "breakout_date": "2025-08-01",
        "lookback_days": 29,
        "tags": ["创业板", "化工"],
        "description": "缩量后放量+价格接近短期趋势线+J值中位"
    },
    {
        "id": "case_010",
        "name": "航天发展",
        "code": "000547",
        "breakout_date": "2025-11-12",
        "lookback_days": 12,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_011",
        "name": "欧林生物",
        "code": "688319",
        "breakout_date": "2025-08-08",
        "lookback_days": 27,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_012",
        "name": "东吴证券",
        "code": "601555",
        "breakout_date": "2025-08-08",
        "lookback_days": 12,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_013",
        "name": "中富电路",
        "code": "300814",
        "breakout_date": "2025-08-08",
        "lookback_days": 40,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_014",
        "name": "众辰科技",
        "code": "603275",
        "breakout_date": "2025-07-11",
        "lookback_days": 37,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_015",
        "name": "国城矿业",
        "code": "000688",
        "breakout_date": "2025-07-10",
        "lookback_days": 38,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_016",
        "name": "桐昆股份",
        "code": "601233",
        "breakout_date": "2025-08-05",
        "lookback_days": 11,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_017",
        "name": "康平科技",
        "code": "300907",
        "breakout_date": "2025-08-12",
        "lookback_days": 12,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_018",
        "name": "国盛证券",
        "code": "002670",
        "breakout_date": "2025-07-18",
        "lookback_days": 19,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_019",
        "name": "中科金财",
        "code": "002657",
        "breakout_date": "2025-08-05",
        "lookback_days": 37,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_020",
        "name": "信达证券",
        "code": "601059",
        "breakout_date": "2025-08-21",
        "lookback_days": 13,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_021",
        "name": "中国稀土",
        "code": "000831",
        "breakout_date": "2025-08-06",
        "lookback_days": 20,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_022",
        "name": "阿莱德",
        "code": "301419",
        "breakout_date": "2025-07-24",
        "lookback_days": 12,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_023",
        "name": "新力金融",
        "code": "600318",
        "breakout_date": "2025-08-04",
        "lookback_days": 29,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_024",
        "name": "中信证券",
        "code": "600030",
        "breakout_date": "2024-09-13",
        "lookback_days": 40,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_025",
        "name": "润建股份",
        "code": "002929",
        "breakout_date": "2026-04-29",
        "lookback_days": 16,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_026",
        "name": "云南锗业",
        "code": "002428",
        "breakout_date": "2026-04-30",
        "lookback_days": 27,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
    {
        "id": "case_027",
        "name": "大业股份",
        "code": "603278",
        "breakout_date": "2026-05-06",
        "lookback_days": 11,
        "tags": ["主板", "军工"],
        "description": "航天军工+量能异动+趋势突破"
    },
]

# 相似度权重配置（优先从YAML读取，否则使用默认值）
_default_weights = {
    "trend_structure": 0.08,    # 知行趋势线结构（降低）
    "kdj_state": 0.08,          # KDJ动能状态（降低）
    "volume_pattern": 0.20,     # 量能特征（保持）
    "price_shape": 0.28,        # 价格形态（提高）
    "move_strength": 0.18,      # 异动期涨幅（提高）
    "build_health": 0.18,       # 建仓健康度（涨幅+换手+均线）（提高）
}
SIMILARITY_WEIGHTS = _yaml_config.get('weights', _default_weights)

# 匹配阈值（低于此值不显示）
MIN_SIMILARITY_SCORE = _yaml_config.get('min_similarity', 60.0)

# 回看天数（默认25天）
DEFAULT_LOOKBACK_DAYS = _yaml_config.get('lookback_days', 25)

# Top N 结果展示（优先从YAML读取）
TOP_N_RESULTS = _yaml_config.get('top_n_results', 15)

# 匹配容差参数（优先从YAML读取）
_default_tolerances = {
    "trend_ratio": 0.10,    # 趋势比值容差（±10%）
    "price_bias": 10,       # 价格偏离容差（±10%）
    "trend_spread": 10,     # 趋势发散容差（±10%）
    "j_value": 30,          # J值差异容差（±30）
    "drawdown": 15,         # 回撤幅度容差（±15%）
    "move_avg_gain": 5,  # 平均异动涨幅容差（±5%）
    "move_total_gain": 10,  # 总涨幅容差（±10%）
}
MATCH_TOLERANCES = _yaml_config.get('tolerances', _default_tolerances)
