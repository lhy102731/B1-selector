"""
三层资金流数据采集器
- 概念资金流 (concept_fund_flow)
- 个股资金流 (stock_fund_flow)
- 大单明细 (big_deal)
数据源: 同花顺 (10jqka)
"""
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import py_mini_racer
import akshare as ak

sys.path.insert(0, str(Path(__file__).parent.parent))


class FundFlowCollector:
    """每日资金流数据采集"""

    def __init__(self, data_dir=None):
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'data' / 'block'
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------
    @staticmethod
    def _get_headers(referer='http://data.10jqka.com.cn/funds/gnzjl/'):
        from akshare.datasets import get_ths_js
        js_code = py_mini_racer.MiniRacer()
        js_content = open(get_ths_js('ths.js'), encoding='utf-8').read()
        js_code.eval(js_content)
        v_code = js_code.call('v')
        return {
            'Accept': 'text/html, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'hexin-v': v_code,
            'Host': 'data.10jqka.com.cn',
            'Pragma': 'no-cache',
            'Referer': referer,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'X-Requested-With': 'XMLHttpRequest',
        }

    # ------------------------------------------------------------------
    # HTML table parser (avoids pd.read_html encoding issues)
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_html_table(html_text):
        soup = BeautifulSoup(html_text, features='lxml')
        table = soup.find('table', class_='m-table')
        if not table:
            return pd.DataFrame()
        rows = table.find_all('tr')
        data = []
        for tr in rows:
            cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
            if cells:
                data.append(cells)
        if len(data) < 2:
            return pd.DataFrame()
        return pd.DataFrame(data[1:], columns=data[0])

    @staticmethod
    def _fetch_paginated(url_template, total_pages, referer):
        all_data = []
        for page in range(1, total_pages + 1):
            headers = FundFlowCollector._get_headers(referer)
            r = requests.get(url_template.format(page), headers=headers, timeout=30)
            r.encoding = 'gbk'
            df = FundFlowCollector._parse_html_table(r.text)
            if df.empty:
                continue
            all_data.append(df)
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    @staticmethod
    def _fix_stock_code(code_val):
        s = str(int(float(code_val))) if not isinstance(code_val, str) else code_val.strip()
        return s.zfill(6)

    @staticmethod
    def _parse_amount(val):
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        if '亿' in s:
            return float(s.replace('亿', ''))
        if '万' in s:
            return float(s.replace('万', '')) / 10000.0
        try:
            return float(s)
        except:
            return np.nan

    # ------------------------------------------------------------------
    # 1. Concept fund flow
    # ------------------------------------------------------------------
    BASE_CONCEPT = 'http://data.10jqka.com.cn/funds/gnzjl/field/zdf/order/desc/page/{}/ajax/1/free/1/'

    CONCEPT_COLS = ['rank', 'concept', 'index_val', 'change_pct', 'inflow',
                    'outflow', 'net_flow', 'company_count', 'lead_stock',
                    'lead_change_pct', 'lead_price']

    def collect_concept_fund_flow(self, date_str=None):
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')

        referer = 'http://data.10jqka.com.cn/funds/gnzjl/'
        headers = self._get_headers(referer)
        r = requests.get(self.BASE_CONCEPT.format(1), headers=headers, timeout=30)
        r.encoding = 'gbk'
        soup = BeautifulSoup(r.text, features='lxml')
        page_span = soup.find(name='span', attrs={'class': 'page_info'})
        if page_span is None:
            print('[ERR] concept fund flow - cannot parse page info')
            return pd.DataFrame()
        total_pages = int(page_span.text.split('/')[1])
        print(f'  concept flow: {total_pages} pages')

        result = self._fetch_paginated(self.BASE_CONCEPT, total_pages, referer)
        if result.empty:
            return result
        result.columns = self.CONCEPT_COLS
        for c in ['inflow', 'outflow', 'net_flow']:
            result[c] = result[c].apply(self._parse_amount)

        out_dir = self.data_dir / 'concept_flow'
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / f'{date_str}.csv'
        result.to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f'[OK] concept flow: {len(result)} rows -> {out_path}')
        return result

    # ------------------------------------------------------------------
    # 2. Individual stock fund flow
    # ------------------------------------------------------------------
    BASE_STOCK = 'http://data.10jqka.com.cn/funds/ggzjl/field/zdf/order/desc/page/{}/ajax/1/free/1/'

    STOCK_COLS = ['rank', 'code', 'name', 'price', 'change_pct', 'turnover',
                  'inflow', 'outflow', 'net_flow', 'amount']

    def collect_stock_fund_flow(self, date_str=None):
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')

        referer = 'http://data.10jqka.com.cn/funds/ggzjl/'
        headers = self._get_headers(referer)
        r = requests.get(self.BASE_STOCK.format(1), headers=headers, timeout=30)
        r.encoding = 'gbk'
        soup = BeautifulSoup(r.text, features='lxml')
        page_span = soup.find(name='span', attrs={'class': 'page_info'})
        if page_span is None:
            print('[ERR] stock fund flow - cannot parse page info')
            return pd.DataFrame()
        total_pages = int(page_span.text.split('/')[1])
        print(f'  stock flow: {total_pages} pages')

        result = self._fetch_paginated(self.BASE_STOCK, total_pages, referer)
        if result.empty:
            return result
        result.columns = self.STOCK_COLS
        result['code'] = result['code'].apply(self._fix_stock_code)
        for c in ['inflow', 'outflow', 'net_flow', 'amount']:
            result[c] = result[c].apply(self._parse_amount)

        out_dir = self.data_dir / 'stock_flow'
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / f'{date_str}.csv'
        result.to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f'[OK] stock flow: {len(result)} rows -> {out_path}')
        return result

    # ------------------------------------------------------------------
    # 3. Big deal detail
    # ------------------------------------------------------------------
    BIG_DEAL_COLS = ['time', 'code', 'name', 'price', 'volume', 'amount',
                     'direction', 'change_pct', 'change_amount']

    @classmethod
    def _normalize_big_deal(cls, df):
        if len(df.columns) == len(cls.BIG_DEAL_COLS):
            df.columns = cls.BIG_DEAL_COLS
        dir_col = df.columns[6]
        df['direction'] = df[dir_col].apply(
            lambda x: 'buy' if ord(str(x)[0]) == 20080 else 'sell')
        return df

    def collect_big_deal(self, date_str=None):
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')

        print('  fetching big deal (akshare)...')
        df = ak.stock_fund_flow_big_deal()
        if df is None or df.empty:
            print('[ERR] big deal - no data')
            return pd.DataFrame()

        df = self._normalize_big_deal(df)

        out_dir = self.data_dir / 'big_deal'
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / f'{date_str}.csv'
        df.to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f'[OK] big deal: {len(df)} rows -> {out_path}')
        return df

    # ------------------------------------------------------------------
    # 4. Concept index (daily closing prices)
    # ------------------------------------------------------------------
    @staticmethod
    def _load_flow_index_map(flow_dir, date_str):
        """从概念资金流CSV中提取 概念名 -> index_val 映射，作为fallback数据源"""
        flow_file = flow_dir / f'{date_str}.csv'
        if not flow_file.exists():
            return {}
        try:
            df = pd.read_csv(flow_file, encoding='utf-8-sig')
            if 'concept' not in df.columns or 'index_val' not in df.columns:
                return {}
            df['index_val'] = pd.to_numeric(df['index_val'], errors='coerce')
            return dict(zip(df['concept'], df['index_val']))
        except Exception:
            return {}

    def _get_member_synthesis(self, name, last_close, date_str):
        """第二层fallback: 从概念成分股等权合成当日指数"""
        concept_file = self.data_dir / 'concept.json'
        if not concept_file.exists():
            return None

        try:
            with open(concept_file, 'r', encoding='utf-8') as f:
                concept_data = json.load(f)
        except Exception:
            return None

        stock_to_blocks = concept_data.get('stock_to_blocks', {})
        # 文件名用_替代/ (如DRG_DIP -> DRG/DIP)
        lookup_names = {name}
        if '_' in name:
            lookup_names.add(name.replace('_', '/'))
        members = [sc for sc, blocks in stock_to_blocks.items() if lookup_names & set(blocks)]
        if len(members) < 3:
            return None

        from utils.csv_manager import CSVManager
        csv_mgr = CSVManager(str(self.data_dir.parent))

        returns = []
        for code in members:
            try:
                stock_path = csv_mgr.get_stock_path(code)
                if not stock_path.exists():
                    continue
                # 数据倒序存储，nrows=1即最新日
                row = pd.read_csv(stock_path, nrows=1, encoding='gbk')
                if row.empty or 'close' not in row.columns:
                    continue
                row_date = str(row['date'].iloc[0])[:10]
                latest_close = float(row['close'].iloc[0])
                if latest_close <= 0:
                    continue
                if row_date == date_str:
                    prev_path = stock_path
                    df_tmp = pd.read_csv(prev_path, nrows=2, encoding='gbk')
                    prev_close = float(df_tmp['close'].iloc[1]) if len(df_tmp) > 1 and df_tmp['close'].iloc[1] > 0 else latest_close
                    ret = (latest_close / prev_close - 1) if prev_close > 0 else 0
                else:
                    ret = 0  # 该股今日无数据，不贡献涨跌
                returns.append(ret)
            except Exception:
                continue

        if len(returns) < 3:
            return None

        avg_ret = np.mean(returns)
        new_close = round(last_close * (1 + avg_ret), 4)
        return pd.DataFrame([{'date': date_str, 'close': new_close}])

    def _collect_legacy_synthetic_concept_index(self, date_str=None):
        """Deprecated mixed-provider implementation retained only for audit history."""
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')

        concept_dir = self.data_dir / 'concept_index'
        if not concept_dir.is_dir():
            print('  [SKIP] concept_index dir not found')
            return

        concept_files = sorted(concept_dir.glob('*.csv'))
        if not concept_files:
            print('  [SKIP] no concept index files')
            return

        print(f'  concept index: {len(concept_files)} concepts')

        # 预加载概念资金流作为fallback (Layer 1已写入)
        flow_index_map = self._load_flow_index_map(
            self.data_dir / 'concept_flow', date_str)

        updated = 0
        skipped = 0
        failed = 0
        fallback_used = 0
        synth_used = 0

        for i, fp in enumerate(concept_files):
            name = fp.stem
            try:
                existing = pd.read_csv(fp, dtype={'date': str}, encoding='utf-8-sig')
                if existing.empty:
                    skipped += 1
                    continue

                last_date = existing['date'].max()
                if last_date >= date_str:
                    skipped += 1
                    continue

                # Tier 1: THS指数API
                df_new = None
                try:
                    df_raw = ak.stock_board_concept_index_ths(
                        symbol=name, start_date=last_date, end_date=date_str)
                    if df_raw is not None and not df_raw.empty:
                        df_new = df_raw.rename(columns={'日期': 'date', '收盘价': 'close'})
                        df_new['date'] = pd.to_datetime(df_new['date']).dt.strftime('%Y-%m-%d')
                        df_new = df_new[['date', 'close']]
                except Exception:
                    pass

                # Tier 2: 从概念资金流取index_val
                if df_new is None and name in flow_index_map:
                    index_val = flow_index_map[name]
                    if pd.notna(index_val) and index_val > 0:
                        df_new = pd.DataFrame([{'date': date_str, 'close': index_val}])
                        fallback_used += 1

                # Tier 3: 从成分股等权合成
                if df_new is None:
                    last_close = float(existing['close'].iloc[-1])
                    df_new = self._get_member_synthesis(name, last_close, date_str)
                    if df_new is not None:
                        synth_used += 1

                if df_new is None or df_new.empty:
                    skipped += 1
                    continue

                combined = pd.concat([existing, df_new], ignore_index=True)
                combined = combined.drop_duplicates(subset=['date'], keep='last')
                combined = combined.sort_values('date', ascending=True)
                combined.to_csv(fp, index=False, encoding='utf-8-sig')
                updated += 1
            except Exception as e:
                failed += 1
                if failed <= 5:
                    print(f'  [WARN] {name}: {e}')

            if (i + 1) % 50 == 0:
                print(f'    progress: {i + 1}/{len(concept_files)} '
                      f'(updated={updated}, fallback={fallback_used}, synth={synth_used})')

        print(f'[OK] concept index: updated={updated}, skipped={skipped}, '
              f'failed={failed}, fallback={fallback_used}, synth={synth_used}')

    def collect_concept_index(self, date_str=None):
        """Update native THS concept indices; never synthesize from members."""
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        from tools.update_ths_market_assets import run

        result = run(
            data_dir=self.data_dir.parent,
            asset_types=("concept",),
            end=date_str,
        )
        if result != 0:
            raise RuntimeError(f"THS concept index update failed with exit code {result}")
        return result

    def collect_native_indices(self, date_str=None):
        """Update independently stored THS industry and concept indices."""
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        from tools.update_ths_market_assets import run

        result = run(
            data_dir=self.data_dir.parent,
            asset_types=("industry", "concept"),
            end=date_str,
        )
        if result != 0:
            raise RuntimeError(f"THS native index update failed with exit code {result}")
        return result

    # ------------------------------------------------------------------
    # 5. Collect all (with caching to avoid redundant runs)
    # ------------------------------------------------------------------
    @staticmethod
    def _get_last_trading_day(date):
        """简易交易日回退，akshare不可用时fallback"""
        try:
            import akshare as ak
            trade_cal = ak.tool_trade_date_hist_sina()
            trade_dates = pd.to_datetime(trade_cal['trade_date']).dt.date
            past = [d for d in trade_dates if d < date]
            if past:
                return past[-1]
        except Exception:
            pass
        from datetime import timedelta
        w = date.weekday()
        if w == 0:
            return date - timedelta(days=3)
        if w == 6:
            return date - timedelta(days=2)
        return date - timedelta(days=1)

    @staticmethod
    def _is_trading_day(date):
        try:
            import akshare as ak
            trade_cal = ak.tool_trade_date_hist_sina()
            trade_dates = pd.to_datetime(trade_cal['trade_date']).dt.date
            return date in set(trade_dates)
        except Exception:
            return date.weekday() < 5

    def _check_block_cache(self, today, current_time, is_today_trading):
        """检查block数据是否已覆盖当前周期，返回True跳过"""
        cache_file = self.data_dir / '.update_cache.json'
        if not cache_file.exists():
            return False

        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache = json.load(f)
        except Exception:
            return False

        cache_date = cache.get('last_update_date')
        if not cache_date:
            return False
        try:
            cache_date_obj = datetime.strptime(cache_date, '%Y-%m-%d').date()
        except Exception:
            return False

        # 计算本周期需要覆盖的交易日
        last_trading_day = self._get_last_trading_day(today)
        after_close = current_time >= datetime.strptime("15:00", "%H:%M").time()
        if is_today_trading and after_close:
            need_date = today
        else:
            need_date = last_trading_day

        return cache_date_obj >= need_date

    def _write_block_cache(self, today, is_today_trading, current_time):
        """写入block数据更新缓存，记录本次覆盖的交易日"""
        last_trading_day_str = self._get_last_trading_day(today).strftime('%Y-%m-%d')
        after_close = current_time >= datetime.strptime("15:00", "%H:%M").time()
        if is_today_trading and after_close:
            covered = today.strftime('%Y-%m-%d')
        else:
            covered = last_trading_day_str

        cache_file = self.data_dir / '.update_cache.json'
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump({'last_update_date': covered}, f)
        except Exception:
            pass

    def collect_all(self, date_str=None):
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')

        now = datetime.now()
        today = now.date()
        current_time = now.time()
        is_today_trading = self._is_trading_day(today)

        # 盘中拦截
        trading_start = datetime.strptime("09:00", "%H:%M").time()
        trading_end = datetime.strptime("15:00", "%H:%M").time()
        if trading_start <= current_time <= trading_end and is_today_trading:
            print(f'[SKIP] 交易时段内，跳过block数据更新')
            return

        # 缓存检查
        if self._check_block_cache(today, current_time, is_today_trading):
            print(f'[OK] block数据已是最新，无需重复更新')
            return

        print('=' * 60)
        print(f'[FundFlow] collecting {date_str}')
        print('=' * 60)
        failures = []

        print('\n--- Layer 1: Concept flow ---')
        try:
            self.collect_concept_fund_flow(date_str)
        except Exception as e:
            failures.append("concept_flow")
            print(f'[ERR] concept flow failed: {e}')
            import traceback
            traceback.print_exc()

        print('\n--- Layer 2: Stock flow ---')
        try:
            self.collect_stock_fund_flow(date_str)
        except Exception as e:
            failures.append("stock_flow")
            print(f'[ERR] stock flow failed: {e}')
            import traceback
            traceback.print_exc()

        print('\n--- Layer 3: Big deal ---')
        try:
            self.collect_big_deal(date_str)
        except Exception as e:
            failures.append("big_deal")
            print(f'[ERR] big deal failed: {e}')
            import traceback
            traceback.print_exc()

        print('\n--- Layer 4: Native THS industry/concept indices ---')
        try:
            self.collect_native_indices(date_str)
        except Exception as e:
            failures.append("native_indices")
            print(f'[ERR] native THS index update failed: {e}')
            import traceback
            traceback.print_exc()

        if failures:
            print(f'\n[FundFlow] incomplete; failed layers: {", ".join(failures)}')
            return 2
        self._write_block_cache(today, is_today_trading, current_time)
        print('\n[FundFlow] done')
        return 0


if __name__ == '__main__':
    collector = FundFlowCollector()
    collector.collect_all()
