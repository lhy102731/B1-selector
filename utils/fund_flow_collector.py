"""
三层资金流数据采集器
- 概念资金流 (concept_fund_flow)
- 个股资金流 (stock_fund_flow)
- 大单明细 (big_deal)
数据源: 同花顺 (10jqka)
"""
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
    # 4. Collect all
    # ------------------------------------------------------------------
    def collect_all(self, date_str=None):
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')

        print('=' * 60)
        print(f'[FundFlow] collecting {date_str}')
        print('=' * 60)

        print('\n--- Layer 1: Concept flow ---')
        try:
            self.collect_concept_fund_flow(date_str)
        except Exception as e:
            print(f'[ERR] concept flow failed: {e}')
            import traceback
            traceback.print_exc()

        print('\n--- Layer 2: Stock flow ---')
        try:
            self.collect_stock_fund_flow(date_str)
        except Exception as e:
            print(f'[ERR] stock flow failed: {e}')
            import traceback
            traceback.print_exc()

        print('\n--- Layer 3: Big deal ---')
        try:
            self.collect_big_deal(date_str)
        except Exception as e:
            print(f'[ERR] big deal failed: {e}')
            import traceback
            traceback.print_exc()

        print('\n[FundFlow] done')


if __name__ == '__main__':
    collector = FundFlowCollector()
    collector.collect_all()
