# -*- coding: utf-8 -*-
"""
Daily one-click: B1 V3 + B3 + Brick scan
Usage: python daily_run.py [--date YYYY-MM-DD]
"""

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from utils.process_lock import ProcessConcurrencyError, process_lock


DAILY_RUN_LOCK_PATH = Path(
    os.environ.get(
        "DAILY_RUN_LOCK_PATH",
        Path(tempfile.gettempdir()) / "a_share_quant_selector_daily.lock",
    )
)


def run(cmd: list[str], desc: str) -> bool:
    print('\n' + '=' * 60)
    print('  ' + desc)
    print('=' * 60)
    r = subprocess.run(cmd, shell=False, cwd=Path(__file__).parent)
    if r.returncode != 0:
        print('[WARN] {} returned code {}'.format(desc, r.returncode))
    return r.returncode == 0


def _run_pipeline() -> int:
    ap = argparse.ArgumentParser(description='Daily selection runner')
    ap.add_argument('--date', type=str, default=None, help='Date YYYY-MM-DD')
    ap.add_argument('--max-stocks', type=int, default=None, help='Limit stocks (test only)')
    ap.add_argument('--skip-b1', action='store_true', help='Skip B1 V3 + B3')
    ap.add_argument('--skip-brick', action='store_true', help='Skip brick scan')
    ap.add_argument('--skip-etf', action='store_true', help='Skip research-only ETF candidate scan')
    ap.add_argument('--skip-update', action='store_true', help='Skip data update')
    args = ap.parse_args()

    # 0. Data update
    if not args.skip_update:
        if not run([sys.executable, 'main.py', 'update'], 'Data update (CSV)'):
            print('[ERROR] Daily pipeline stopped at: Data update (CSV)')
            return 1

    from run_b1_v3 import _effective_select_date

    today = _effective_select_date(args.date).strftime('%Y-%m-%d')
    print('Daily Run - ' + today)

    if not args.skip_update:
        update_steps = (
            (
                [
                    sys.executable,
                    'tools/update_ths_market_assets.py',
                    '--asset-types',
                    'etf',
                    '--end',
                    today,
                ],
                'Update THS ETF data',
            ),
            (
                [
                    sys.executable,
                    'tools/backfill_daily_pcf_baostock.py',
                    '--date',
                    today,
                    '--apply',
                ],
                'Fill daily pcfNcfTTM fallback',
            ),
            (
                [sys.executable, 'build_indicators_cache.py'],
                'Rebuild indicators cache (parquet)',
            ),
            (
                [sys.executable, 'build_daily_ret_cache.py'],
                'Update daily return cache',
            ),
        )
        for command, description in update_steps:
            if not run(command, description):
                print(f'[ERROR] Daily pipeline stopped at: {description}')
                return 1

    overall_ok = True

    # 1. B1 V3
    if not args.skip_b1:
        cmd = [
            sys.executable,
            'run_b1_v3.py',
            'select',
            '--date',
            today,
        ]
        if args.max_stocks:
            cmd.extend(['--max-stocks', str(args.max_stocks)])
        overall_ok = run(cmd, 'B1 V3') and overall_ok


    # 3. B3 (main.py)
    if not args.skip_b1:
        cmd = [sys.executable, 'run_b3.py']
        if args.max_stocks:
            cmd.extend(['--max-stocks', str(args.max_stocks)])
        overall_ok = run(cmd, 'B3 standalone scan') and overall_ok

    if not args.skip_etf:
        overall_ok = run(
            [
                sys.executable,
                'tools/select_etf_candidates.py',
                '--date',
                today,
                '--output',
                'artifacts/daily/etf/signals_today.csv',
            ],
            'ETF technical candidate pool (research-only)',
        ) and overall_ok

    # 2. Brick scan
    if not args.skip_brick:
        brick_signal_path = 'artifacts/daily/brick/signals_today.csv'
        cmd = [
            sys.executable,
            'backtest_brick_v2.py',
            '--save-raw',
            brick_signal_path,
            '--start',
            today,
            '--end',
            today,
            '--no-timing',
        ]
        brick_ok = run(
            cmd,
            'Brick scan -> artifacts/daily/brick/signals_today.csv',
        )
        overall_ok = brick_ok and overall_ok

        if brick_ok:
            # V2: filter executive reduction on signal day (d=-0.087, validated)
            overall_ok = run(
                [
                    sys.executable,
                    'filter_exec_reduce.py',
                    '--signals',
                    brick_signal_path,
                    '--date',
                    today,
                ],
                'Filter exec reduction -> artifacts/daily/brick/signals_today.csv',
            ) and overall_ok
        else:
            # Never let a failed scan leave yesterday's candidates looking
            # current, and do not run today's reduction filter over stale rows.
            from backtest_brick_v2 import save_raw_signals

            save_raw_signals([], brick_signal_path)
            print('[ERROR] Brick scan failed; replaced stale signal artifact with a header-only file')

    print('\n' + '=' * 60)
    print('  Daily run complete')
    print('=' * 60)
    print('B1 V3 results: DingTalk notification sent')
    print('Brick signals: artifacts/daily/brick/signals_today.csv (exec filtered)')
    print('ETF candidates: artifacts/daily/etf/signals_today.csv (research-only, not stock-ranked)')
    print()
    print('Next: T+1 09:25 -> python daily_select.py')
    return 0 if overall_ok else 1


def main() -> int:
    try:
        with process_lock(DAILY_RUN_LOCK_PATH, "daily pipeline"):
            return _run_pipeline()
    except ProcessConcurrencyError as error:
        print(f"[ERROR] {error}")
        return 3


if __name__ == '__main__':
    raise SystemExit(main())
