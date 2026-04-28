"""
缓存 vs 实时计算 结果一致性对比脚本（修复版）
用法: python compare_cache.py --start 2026-04-01 --end 2026-04-10 --workers 16 --sample 300
"""
import sys
import argparse
import pandas as pd
import shutil
from pathlib import Path
from backtest_optimized import OptimizedBacktester


def run_backtest_and_save(use_cache, args, tag):
    """回测并保存结果文件为备份，返回权益和交易DataFrame"""
    bt = OptimizedBacktester(data_dir='data', use_cache=use_cache)
    bt.use_indicators_cache = use_cache
    bt.max_stocks_per_day = args.max_stocks
    bt.min_similarity = args.min_similarity

    bt.run(
        start_date=args.start,
        end_date=args.end,
        sample_size=args.sample,
        n_workers=args.workers
    )

    # 立即备份结果文件，防止被下一轮覆盖
    equity_dst = f'backtest_equity_{tag}.csv'
    trades_dst = f'backtest_trades_{tag}.csv'
    shutil.copy('backtest_equity.csv', equity_dst)
    try:
        shutil.copy('backtest_trades.csv', trades_dst)
    except FileNotFoundError:
        pass

    # 读取备份
    try:
        equity = pd.read_csv(equity_dst)
    except FileNotFoundError:
        equity = pd.DataFrame()

    try:
        trades = pd.read_csv(trades_dst, dtype={
            'code': str, 'buy_date': str, 'sell_date': str
        })
    except FileNotFoundError:
        trades = pd.DataFrame(columns=['code', 'buy_date', 'sell_date',
                                       'buy_price', 'sell_price', 'shares',
                                       'pnl', 'pnl_pct', 'reason'])

    return equity, trades


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default='2026-04-01')
    parser.add_argument('--end', default='2026-04-10')
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--sample', type=int, default=300)
    parser.add_argument('--max-stocks', type=int, default=10)
    parser.add_argument('--min-similarity', type=float, default=60)
    args = parser.parse_args()

    print("=" * 60)
    print("🚀 第一轮：使用缓存回测")
    print("=" * 60)
    eq_cache, tr_cache = run_backtest_and_save(use_cache=True, args=args, tag='cache')

    print("\n" + "=" * 60)
    print("🚀 第二轮：禁用缓存，实时计算回测")
    print("=" * 60)
    eq_realtime, tr_realtime = run_backtest_and_save(use_cache=False, args=args, tag='realtime')

    # --- 资产曲线对比 ---
    if not eq_cache.empty and not eq_realtime.empty:
        eq_cache = eq_cache.rename(columns={'total': 'total_cache'})
        eq_realtime = eq_realtime.rename(columns={'total': 'total_realtime'})
        eq_compare = eq_cache[['date']].copy()
        eq_compare['total_cache'] = eq_cache['total_cache']
        eq_compare['total_realtime'] = eq_realtime['total_realtime']
        eq_compare['diff'] = eq_compare['total_cache'] - eq_compare['total_realtime']

        print("\n" + "=" * 60)
        print("📊 资产曲线差异分析")
        print("=" * 60)
        max_diff = eq_compare['diff'].abs().max()
        if max_diff == 0:
            print("✅ 资产曲线完全一致，缓存安全！")
        else:
            print(f"❌ 资产曲线存在差异，最大偏差: {max_diff:.2f} 元")
            eq_compare.to_csv('equity_diff.csv', index=False)
            print("→ 差异明细已保存至 equity_diff.csv")
    else:
        print("⚠️ 无权益数据对比")

    # --- 交易记录对比 ---
    print("\n📈 交易记录对比")
    print("-" * 60)
    print(f"缓存模式交易数: {len(tr_cache)}")
    print(f"实时模式交易数: {len(tr_realtime)}")

    if tr_cache.empty and tr_realtime.empty:
        print("✅ 两种模式均无交易")
    elif not tr_cache.empty and not tr_realtime.empty:
        # 创建匹配键
        tr_cache['_key'] = tr_cache['code'] + '_' + tr_cache['buy_date']
        tr_realtime['_key'] = tr_realtime['code'] + '_' + tr_realtime['buy_date']

        merged = tr_cache.merge(tr_realtime, on='_key', how='outer', indicator=True)
        only_cache = merged[merged['_merge'] == 'left_only']
        only_realtime = merged[merged['_merge'] == 'right_only']
        common = merged[merged['_merge'] == 'both']

        print(f"  · 缓存独有: {len(only_cache)} 笔")
        print(f"  · 实时独有: {len(only_realtime)} 笔")
        print(f"  · 共同交易: {len(common)} 笔")

        if len(only_cache) > 0 or len(only_realtime) > 0:
            print("⚠️ 交易记录存在差异，缓存存在问题！")
            if len(only_cache) > 0:
                only_cache.to_csv('trades_only_cache.csv', index=False)
            if len(only_realtime) > 0:
                only_realtime.to_csv('trades_only_realtime.csv', index=False)
        else:
            print("✅ 交易记录完全一致。")
            # 对比盈亏
            if 'pnl_x' in common.columns and 'pnl_y' in common.columns:
                pnl_diff = (common['pnl_x'] - common['pnl_y']).abs()
                max_pnl_diff = pnl_diff.max()
                if max_pnl_diff == 0:
                    print("✅ 所有共同交易盈亏无偏差。")
                else:
                    print(f"❌ 共同交易盈亏存在偏差，最大偏差: {max_pnl_diff:.2f} 元")
            else:
                # 可能发生了其他列名，这里不强行对比
                pass
    else:
        print("❌ 差异严重：一种模式有交易，另一种无交易。缓存存在问题！")
        if not tr_cache.empty:
            tr_cache.to_csv('trades_cache_only.csv', index=False)
        if not tr_realtime.empty:
            tr_realtime.to_csv('trades_realtime_only.csv', index=False)


if __name__ == '__main__':
    main()