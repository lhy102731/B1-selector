
# -*- coding: utf-8 -*-
"""
从指南针 Z_SK/day.vdat 提取活跃市值数据，输出到 data/market/active_cap.csv

用法:
  python fetch_active_cap.py                    # 更新 CSV（增量追加）
  python fetch_active_cap.py --full             # 全量重新提取
  python fetch_active_cap.py --last 10          # 只显示最近10条
"""
import sys
import struct
from pathlib import Path
from datetime import datetime
import pandas as pd

VDAT_PATH = Path(r'D:\Compass\WavMain\ANALYSE\Data\ChinaStk\Z_SK\day.vdat')
CSV_PATH = Path(__file__).parent / 'data' / 'market' / 'active_cap.csv'
CSV_PATH.parent.mkdir(parents=True, exist_ok=True)


def extract_all_records(filepath):
    """从 day.vdat 提取所有 date + OHLC 记录（28字节一组，逐字节扫描）"""
    with open(filepath, 'rb') as f:
        data = f.read()

    records = []
    # 记录格式: date(uint32) + open(float) + high(float) + low(float) + close(float) + 2×float = 28 bytes
    REC_SIZE = 28

    for offset in range(0, len(data) - REC_SIZE):
        date_int = struct.unpack('<I', data[offset:offset+4])[0]
        if not (19900101 <= date_int <= 20351231):
            continue
        y, m, d = date_int // 10000, (date_int // 100) % 100, date_int % 100
        if not (1 <= m <= 12 and 1 <= d <= 31):
            continue

        o = struct.unpack('<f', data[offset+4:offset+8])[0]
        h = struct.unpack('<f', data[offset+8:offset+12])[0]
        l = struct.unpack('<f', data[offset+12:offset+16])[0]
        c = struct.unpack('<f', data[offset+16:offset+20])[0]

        if not (100 < c < 500000 and 100 < o < 500000):
            continue
        if h < l:
            continue

        records.append({
            'date': datetime(y, m, d),
            'active_cap': round(c, 4),
            'open': round(o, 2),
            'high': round(h, 2),
            'low': round(l, 2),
        })

    return records


def update_csv(records):
    """增量更新 CSV 文件"""
    if CSV_PATH.exists():
        existing = pd.read_csv(CSV_PATH)
        existing['date'] = pd.to_datetime(existing['date'])
        existing_dates = set(existing['date'])
    else:
        existing = pd.DataFrame(columns=['date', 'active_cap'])
        existing_dates = set()

    new_records = [r for r in records if r['date'] not in existing_dates]
    if not new_records:
        print('没有新数据需要追加')
        return

    new_df = pd.DataFrame(new_records)
    new_df = new_df[['date', 'active_cap']]  # 只保留 date 和 close 值
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.sort_values('date').drop_duplicates('date').reset_index(drop=True)
    combined.to_csv(CSV_PATH, index=False)

    # 显示最新数据
    print(f'已追加 {len(new_records)} 条记录')
    print(f'\n最新 5 条:')
    for _, row in combined.tail(5).iterrows():
        print(f'  {row["date"].strftime("%Y-%m-%d")}: {row["active_cap"]:.2f}')

    # 计算涨跌幅
    if len(combined) >= 2:
        last = combined.iloc[-1]['active_cap']
        prev = combined.iloc[-2]['active_cap']
        pct = (last - prev) / prev * 100
        print(f'\n最新涨跌幅: {pct:+.2f}% ({combined.iloc[-1]["date"].strftime("%Y-%m-%d")})')


if __name__ == '__main__':
    if not VDAT_PATH.exists():
        print(f'错误: 找不到 {VDAT_PATH}')
        print('请确保指南针已安装且数据已更新')
        sys.exit(1)

    print(f'读取: {VDAT_PATH}')
    records = extract_all_records(VDAT_PATH)
    print(f'提取到 {len(records)} 条活跃市值记录')
    if records:
        print(f'日期范围: {records[0]["date"].strftime("%Y-%m-%d")} ~ {records[-1]["date"].strftime("%Y-%m-%d")}')

    if '--last' in sys.argv:
        n = int(sys.argv[sys.argv.index('--last') + 1]) if len(sys.argv) > sys.argv.index('--last') + 1 else 10
        print(f'\n最近 {n} 条:')
        for r in records[-n:]:
            print(f'  {r["date"].strftime("%Y-%m-%d")}: C={r["active_cap"]:.2f} (O={r["open"]:.2f} H={r["high"]:.2f} L={r["low"]:.2f})')
    elif '--full' in sys.argv:
        if CSV_PATH.exists():
            CSV_PATH.unlink()
        update_csv(records)
    else:
        update_csv(records)
