# A股量化选股系统 (B1-Selector)

基于 Python + akshare 的 A 股量化选股系统，实现**统一 B1 碗口反弹策略**与**多维相似度图形匹配**，支持钉钉自动通知、Web 管理界面和完整回测引擎。

## 核心功能

- **统一B1策略** — 双线趋势判断（知行白线/黄线）+ KDJ 超卖检测 + 成交量萎缩识别 + 建仓波质量评估 + S1 出货信号过滤
- **B1完美图形匹配** — 基于 24 个历史成功案例，六维相似度引擎（趋势结构/KDJ状态/量能形态/价格形态/攻击力度/建仓健康度），含 DTW 弹性对齐算法
- **超级B1检测** — 识别 20 日内曾出现 B1 信号、J 值反弹后再次回落的二次买点
- **B2 信号检测** — 识别跳空半阳、涨幅 4%+、倍量的接力信号
- **击穿对手盘识别** — 检测缩量击穿黄线后快速修复的洗盘模式
- **智能数据更新** — 三数据源冗余（baostock → 腾讯 → akshare），多进程并行抓取，自动判断是否需要增量更新
- **回测引擎** — 多进程回测，三批次建仓，多条件止盈止损，含市场择时状态机
- **钉钉通知** — 选股结果 + K 线图自动推送，内置限流保护与指数退避重试
- **Web 管理** — Flask 单页应用，可视化查看股票数据、选股结果、策略配置

## 快速开始

```bash
# 1. 克隆项目
git clone https://github.com/lhy102731/B1-selector.git
cd B1-selector

# 2. 安装依赖
pip3 install -r requirements.txt

# 3. 配置钉钉通知（可选）
cp config/config.yaml.template config/config.yaml
# 编辑 config/config.yaml，填写 webhook_url 和 secret

# 4. 首次全量抓取数据（6年历史数据，耗时较长）
python3 main.py init

# 5. 执行选股（更新数据 → 选股 → B1匹配 → 发送钉钉）
python3 main.py run --b1-match

# 6. 快速测试（只处理前 500 只股票）
python3 main.py run --b1-match --max-stocks 500

# 7. 启动 Web 管理界面
python3 main.py web
```

## 策略说明

### 统一 B1 碗口反弹策略 (UnifiedB1Strategy)

#### 选股条件（全部满足）

1. **上升趋势** — 白线（短期趋势线）> 黄线（多空线）
2. **KDJ 超卖** — J 值 < 阈值（默认 30），处于低位
3. **成交量萎缩** — 当日成交量 < 20 日内最大成交量 × 0.618
4. **MACD 多头** — DIF > 0
5. **市值门槛** — 流通市值 ≥ 40 亿
6. **无暴涨** — 60 日内无翻倍行情
7. **建仓波质量** — 攻击波涨幅 ≤ 60%，换手 ≤ 90%，红量 > 绿量 × 1.2，无缩量涨停
8. **无 S1 信号** — 无出货分布信号（放量巨阴/顶部大风车/次高点放量）
9. **位置合格** — 价格在碗口区域内，或满足击穿对手盘例外条件

#### 技术指标定义

**知行短期趋势线（白线）**
```
EMA(EMA(CLOSE, 10), 10)
```

**知行多空线（黄线）**
```
(MA14 + MA28 + MA57 + MA114) / 4
```

### B1 完美图形匹配（六维相似度引擎）

基于 24 个历史成功案例，对选股结果进行六维相似度排序。

📖 详见 [B1_PATTERN_MATCH.md](B1_PATTERN_MATCH.md)

#### 匹配维度

| 维度 | 权重 | 说明 |
|------|------|------|
| **价格形态** | 28% | DTW 弹性对齐归一化价格曲线，含最大回撤、波动收缩 |
| **量能形态** | 20% | 成交量趋势分类（放量/缩量/缩量后放量/平稳），换手率分析 |
| **攻击力度** | 18% | 攻击波平均/最大/总涨幅，攻击日数量 |
| **建仓健康度** | 18% | 建仓质量评分，含均线评分、涨停惩罚 |
| **趋势结构** | 8% | 双线比值、斜率方向、碗口位置、价格偏离度 |
| **KDJ 状态** | 8% | J 值匹配、金叉状态、J 值反弹、背离检测 |

#### 案例库（24 个历史成功案例）

案例覆盖不同板块与形态类型，包括：华纳药厂(688799)、宁波韵升(600366)、微芯生物(688321)、方正科技(600601)、国轩高科(002074)、野马电池(605378)、光电股份(600184)、新瀚新材(301076)、昂利康(002940)、航天发展(000547)、科蓝软件(300663)、全志科技(300458)、浪潮信息(000977)、中远海科(002401)、远大智能(002689)、海兰信(300065)、江南化工(002226)、三维通信(002115)、华钰矿业(601020)、华钰矿业(601020)_b、万丰奥威(002085)、天海防务(300008)、宗申动力(001696)、瑞丰光电(300241)。

## 技术栈

| 技术 | 用途 |
|------|------|
| **Python 3.8+** | 核心语言 |
| **akshare** | A 股实时/历史数据 API |
| **baostock** | 历史前复权数据（含换手率） |
| **pandas / numpy** | 数据处理与技术指标计算 |
| **pandas_ta** | KDJ 等技术指标 |
| **matplotlib** | K 线图生成 |
| **Flask** | Web 管理界面 |
| **Chart.js** | Web 前端图表 |
| **FastDTW / scipy** | 动态时间规整（价格曲线匹配） |
| **requests** | HTTP 数据获取与钉钉 Webhook |
| **PyYAML** | 配置文件解析 |
| **Pillow** | 图片处理 |
| **schedule** | 定时任务调度 |
| **tqdm** | 进度条 |

## 项目结构

```
├── main.py                       # 主程序入口（CLI）
├── web_server.py                 # 独立 Flask Web 服务器
├── backtest_optimized.py         # 完整回测引擎（多进程）
├── test_all_cases.py             # 历史案例回测验证
├── build_indicators_cache.py     # 预计算指标缓存（Parquet）
├── fetch_market_data.py          # 大盘择时数据抓取
├── requirements.txt              # Python 依赖
├── B1_PATTERN_MATCH.md           # B1 图形匹配详细文档
│
├── config/
│   ├── config.yaml               # 主配置（钉钉 Webhook 等）
│   ├── config.yaml.template      # 配置模板
│   ├── strategy_params.yaml      # 策略参数
│   └── crontab.txt               # 定时任务示例
│
├── strategy/                     # 策略模块
│   ├── __init__.py
│   ├── base_strategy.py          # 策略抽象基类
│   ├── unified_b1_strategy.py    # 统一 B1 策略核心
│   ├── strategy_registry.py      # 策略动态加载/注册器
│   ├── pattern_config.py         # B1 案例库配置（24 案例）
│   ├── pattern_library.py        # 图形库管理器（含缓存）
│   ├── pattern_matcher.py        # 六维相似度计算引擎
│   └── pattern_feature_extractor.py  # 特征提取
│
├── utils/                        # 工具模块
│   ├── akshare_fetcher.py        # 数据获取（三源冗余）
│   ├── csv_manager.py            # CSV 数据读写管理
│   ├── technical.py              # 技术指标（KDJ/EMA/MA 等）
│   ├── dingtalk_notifier.py      # 钉钉通知（含限流）
│   ├── stock_scorer.py           # B1 相似度评分封装
│   ├── market_timing.py          # 市场择时状态机
│   ├── washout_detector.py       # 击穿对手盘检测
│   ├── s1_filter.py              # S1 出货信号过滤
│   ├── kline_chart.py            # K线图生成（标准版）
│   └── kline_chart_fast.py       # K线图生成（快速版）
│
├── web/                          # Web 前端
│   ├── templates/index.html      # 单页应用
│   └── static/
│       ├── css/style.css         # 样式
│       └── js/app.js             # 前端逻辑（Chart.js）
│
└── data/                         # 股票数据（自动生成，gitignore）
    ├── 00/ 30/ 60/ 68/           # 按交易所前缀分目录
    ├── indicators_cache/         # 预计算指标缓存
    └── market/                   # 大盘数据
```

## 命令说明

### 基础命令

| 命令 | 说明 |
|------|------|
| `python3 main.py init` | 首次全量抓取 6 年历史数据（多进程） |
| `python3 main.py update` | 每日增量更新（收盘后执行） |
| `python3 main.py run` | 完整流程：更新数据 → 选股 → 发送钉钉 |
| `python3 main.py run --max-stocks 500` | 快速测试模式 |
| `python3 main.py web` | 启动 Web 界面（默认端口 5000） |
| `python3 main.py --version` | 显示版本信息 |

### B1 完美图形匹配命令

| 命令 | 说明 |
|------|------|
| `python3 main.py run --b1-match` | 启用 B1 图形匹配排序 |
| `python3 main.py run --b1-match --lookback-days 30` | 指定回看 30 天 |
| `python3 main.py run --b1-match --min-similarity 70` | 相似度阈值提高到 70% |
| `python3 main.py run --b1-match --max-stocks 100` | 快速测试前 100 只 |

### 智能更新逻辑

`python3 main.py run` 自动判断更新策略：
1. **15:00 前** — 不更新，使用本地已有数据（盘中）
2. **15:00 后** — 检查每只股票是否有当天数据
3. **100% 已有当日数据** — 跳过更新
4. **否则** — 执行多进程增量更新（3 次重试）

## 策略配置

编辑 `config/strategy_params.yaml`：

```yaml
# 统一 B1 策略
UnifiedB1Strategy:
  M1: 14                # 多空线 MA 周期 1
  M2: 28                # 多空线 MA 周期 2
  M3: 57                # 多空线 MA 周期 3
  M4: 114               # 多空线 MA 周期 4
  J_threshold: 30       # KDJ J 值超卖阈值
  cap_threshold: 4000000000  # 市值门槛（40亿）
  volume_shrink_ratio: 0.618 # 缩量比例
  max_gain: 60          # 建仓波最大涨幅(%)
  max_turnover: 90      # 建仓波最大换手(%)
  near_pct: 3           # 靠近黄线判定百分比

# B1 完美图形匹配
B1PatternMatch:
  min_similarity: 60    # 最小相似度阈值
  lookback_days: 25     # 回看天数
  top_n_results: 15     # 展示 Top N
  weights:              # 六维权重
    trend_structure: 0.08
    kdj_state: 0.08
    volume_pattern: 0.20
    price_shape: 0.28
    move_strength: 0.18
    build_health: 0.18
```

## 钉钉通知

### 限流保护

| 限制项 | 默认值 | 说明 |
|--------|--------|------|
| 每分钟最大消息数 | 20 条 | 达到后自动等待 |
| 最小发送间隔 | 2 秒 | 每条消息间隔 |
| 重试次数 | 3 次 | 指数退避（1s → 4s → 8s） |

### 通知内容

包含：选股结果（按 B1 相似度排序）、相似度百分比、匹配历史案例、六维分项得分、策略分类、K 线图（含白线/黄线/成交量）。

## Web 界面

访问 `http://localhost:5000`：

- **系统概览** — 股票数量、最新数据日期
- **股票列表** — 分页加载、搜索过滤
- **选股结果** — 在线执行选股，查看信号详情
- **策略配置** — 在线查看/修改策略参数
- **K 线详情** — Chart.js 交互式图表，含 KDJ 指标叠加

## 定时任务

添加到 crontab 实现每日自动选股：

```bash
# 每个工作日 15:05 执行
5 15 * * 1-5 cd /path/to/B1-selector && /usr/bin/python3 main.py run --b1-match >> /var/log/b1-selector.log 2>&1
```

## 回测

```bash
# 运行回测
python3 backtest_optimized.py

# 预构建指标缓存（加速回测）
python3 build_indicators_cache.py

# 验证 24 个历史案例是否通过策略筛选
python3 test_all_cases.py
```

回测引擎支持：三批次建仓、多条件止盈止损、S1 减半仓、滴滴信号、白线破位、市场择时过滤。

## 扩展新策略

1. 在 `strategy/` 目录创建新文件，继承 `BaseStrategy`
2. 实现 `calculate_indicators()` 和 `select_stocks()` 方法
3. 在 `config/strategy_params.yaml` 添加参数配置
4. 系统通过 `StrategyRegistry` 自动发现并加载

```python
from strategy.base_strategy import BaseStrategy

class MyStrategy(BaseStrategy):
    def __init__(self, params=None):
        super().__init__("我的策略", params)

    def calculate_indicators(self, df):
        return df

    def select_stocks(self, df, stock_name=''):
        return signals
```

## 免责声明

本项目仅供学习和研究使用，不构成任何投资建议。股市有风险，投资需谨慎。

## License

MIT License
