"""
A股量化选股系统 - 主程序（简化版，仅统一B1策略 + B1相似度排序）

使用方法:
    python main.py init                              # 首次全量抓取
    python main.py update                            # 每日增量更新
    python main.py run --b1-match                    # 执行统一B1选股
    python main.py run --b1-match --max-stocks 100   # 快速测试
    python main.py run --b1-match --min-similarity 70 --lookback-days 40
"""
import sys
import os
import argparse
import platform
from pathlib import Path
from datetime import datetime, time as dt_time
import time

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

__version__ = "1.0.0"

from utils.akshare_fetcher import AKShareFetcher
from utils.csv_manager import CSVManager
from utils.dingtalk_notifier import DingTalkNotifier
from strategy.strategy_registry import get_registry
import yaml


class QuantSystem:
    """量化系统主类"""

    def __init__(self, config_file="config/config.yaml"):
        self.config = self._load_config(config_file)
        self.data_dir = self.config.get('data_dir', 'data')
        self.csv_manager = CSVManager(self.data_dir)
        self.fetcher = AKShareFetcher(self.data_dir)
        self.notifier = self._init_notifier()
        self.registry = get_registry("config/strategy_params.yaml")

    def _load_config(self, config_file):
        config_path = Path(config_file)
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        return {}

    def _init_notifier(self):
        webhook = self.config.get('dingtalk', {}).get('webhook_url')
        secret = self.config.get('dingtalk', {}).get('secret')
        return DingTalkNotifier(webhook, secret)

    def _load_stock_names(self, stock_data):
        names_file = Path(self.data_dir) / 'stock_names.json'
        try:
            stock_names = self.fetcher.get_all_stock_codes()
            if stock_names:
                import json
                with open(names_file, 'w', encoding='utf-8') as f:
                    json.dump(stock_names, f, ensure_ascii=False)
                return stock_names
        except:
            pass
        if names_file.exists():
            import json
            with open(names_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {code: f"股票{code}" for code in stock_data.keys()}

    def init_data(self, max_stocks=None, force_full=False):
        print("=" * 60)
        print("🚀 首次全量数据抓取")
        print("=" * 60)
        self.fetcher.init_full_data(max_stocks=max_stocks, force_full=force_full)
        print("\n✓ 数据初始化完成")

    def _smart_update(self, max_stocks=None):
        print("\n🔄 执行数据更新...")
        fetcher = AKShareFetcher()
        fetcher.daily_update(max_stocks=max_stocks)
        print("\n✓ 数据更新完成")

    def update_data(self, max_stocks=None):
        print("=" * 60)
        print("🔄 每日增量更新")
        print("=" * 60)
        if __name__ == '__main__':
            fetcher = AKShareFetcher()
            fetcher.daily_update(max_stocks=max_stocks)
        print("\n✓ 数据更新完成")

    def run_simple_b1(self, max_stocks=None, min_similarity=None, lookback_days=None):
        """
        简化版B1选股流程：统一B1策略 + B1相似度排序
        """
        from strategy.pattern_config import MIN_SIMILARITY_SCORE, DEFAULT_LOOKBACK_DAYS
        if min_similarity is None:
            min_similarity = MIN_SIMILARITY_SCORE
        if lookback_days is None:
            lookback_days = DEFAULT_LOOKBACK_DAYS

        print("=" * 60)
        print("🚀 执行统一B1选股流程")
        if max_stocks:
            print(f"   快速测试模式：只处理前 {max_stocks} 只股票")
        print(f"   相似度阈值: {min_similarity}%")
        print(f"   回看天数: {lookback_days}天")
        print("=" * 60)

        self._smart_update(max_stocks=max_stocks)

        self.registry.auto_register_from_directory("strategy")
        strategy = self.registry.get_strategy('UnifiedB1Strategy')
        if not strategy:
            from strategy.unified_b1_strategy import UnifiedB1Strategy
            strategy = self.registry.register(UnifiedB1Strategy, name='UnifiedB1Strategy')

        stock_codes = self.csv_manager.list_all_stocks()
        if max_stocks:
            stock_codes = stock_codes[:max_stocks]
        stock_names = self._load_stock_names({})

        selected = []
        print(f"\n执行统一B1策略，共 {len(stock_codes)} 只股票...")

        from utils.stock_scorer import StockScorer
        scorer = StockScorer(self.csv_manager, self.registry)

        for i, code in enumerate(stock_codes, 1):
            df = self.csv_manager.read_stock(code)
            name = stock_names.get(code, '未知')

            if df.empty or len(df) < 60:
                continue
            if any(kw in name for kw in ['退', 'ST', '*ST']):
                continue

            df_indicators = strategy.calculate_indicators(df)
            signals = strategy.select_stocks(df_indicators, name)

            if signals:
                score_info = scorer.score_stock(code, df_indicators, lookback_days)
                if score_info['b1_score'] >= min_similarity:
                    signal = signals[0]
                    selected.append({
                        'code': code,
                        'name': name,
                        'b1_score': score_info['b1_score'],
                        'matched_case': score_info['matched_case'],
                        'matched_date': score_info['matched_date'],
                        'breakdown': score_info['breakdown'],
                        'is_washout': signal.get('is_washout', False),
                        'build_gain': signal.get('build_gain', 0),
                        'surge_turnover': signal.get('surge_turnover', 0),
                        'close': signal['close'],
                        'J': signal['J'],
                        'reasons': signal['reasons'],
                    })

            if i % 100 == 0:
                print(f"  进度: {i}/{len(stock_codes)}，已选出 {len(selected)} 只...")

        selected.sort(key=lambda x: x['b1_score'], reverse=True)

        print(f"\n✓ 最终选出 {len(selected)} 只股票")

        if selected:
            self.notifier.send_simple_b1_results(selected, min_similarity)

        return selected

    def run_full(self, category='all', max_stocks=None):
        """保留原接口兼容，实际调用简化版"""
        return self.run_simple_b1(max_stocks=max_stocks)


def print_version():
    import akshare
    import pandas
    print(f"A-Share Quant v{__version__}")
    print(f"Python: {sys.version.split()[0]}")
    print(f"akshare: {akshare.__version__}")
    print(f"pandas: {pandas.__version__}")
    print(f"System: {platform.system()}")


def main():
    parser = argparse.ArgumentParser(
        description='A股量化选股系统（统一B1策略版）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py init                              # 首次抓取历史数据
  python main.py update                            # 每日增量更新
  python main.py run --b1-match                    # 执行统一B1选股
  python main.py run --b1-match --max-stocks 100   # 快速测试
  python main.py run --b1-match --min-similarity 70 --lookback-days 40
        """
    )

    parser.add_argument('--version', action='store_true', help='显示版本信息并退出')
    parser.add_argument('command', choices=['init', 'update', 'run'], nargs='?', help='要执行的命令')
    parser.add_argument('--max-stocks', type=int, default=None, help='限制处理的股票数量（用于快速测试）')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--b1-match', action='store_true', help='启用B1完美图形匹配排序')
    parser.add_argument('--min-similarity', type=float, default=None, help='B1匹配最小相似度阈值')
    parser.add_argument('--lookback-days', type=int, default=None, help='B1匹配回看天数')
    parser.add_argument('--force-full', action='store_true', help='强制全量重新抓取，忽略失败列表')

    args = parser.parse_args()

    if args.version:
        print_version()
        sys.exit(0)

    if not args.command:
        parser.print_help()
        sys.exit(1)

    os.chdir(project_root)
    quant = QuantSystem(args.config)

    if args.command == 'init':
        quant.init_data(max_stocks=args.max_stocks, force_full=args.force_full)
    elif args.command == 'update':
        quant.update_data(max_stocks=args.max_stocks)
    elif args.command == 'run':
        from strategy.pattern_config import MIN_SIMILARITY_SCORE, DEFAULT_LOOKBACK_DAYS
        min_sim = args.min_similarity if args.min_similarity is not None else MIN_SIMILARITY_SCORE
        lookback = args.lookback_days if args.lookback_days is not None else DEFAULT_LOOKBACK_DAYS
        quant.run_simple_b1(
            max_stocks=args.max_stocks,
            min_similarity=min_sim,
            lookback_days=lookback
        )


if __name__ == '__main__':
    main()