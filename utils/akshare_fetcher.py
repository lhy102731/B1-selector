"""
A股数据抓取模块 - 后复权 K线 + 历史流通市值
优先级: baostock (后复权+PE/PB) > 腾讯K线 (后复权) > mootdx (前复权+因子转后复权)
"""
import pandas as pd
from datetime import datetime, timedelta
import time
import sys
from pathlib import Path
import json
import requests
import random
from http.client import RemoteDisconnected
import os
import contextlib
from tqdm import tqdm

# mootdx TCP行情（通达信，不封IP）
try:
    from mootdx.quotes import Quotes
    MOOTDX = Quotes.factory(market='std')
except Exception:
    MOOTDX = None

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.csv_manager import CSVManager

# 设置请求会话
session = requests.Session()
session.trust_env = False
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Referer': 'https://quote.eastmoney.com/',
    'Connection': 'keep-alive',
})
REQUEST_TIMEOUT = 30

# 备选A股股票列表（当网络获取失败时使用）
DEFAULT_STOCK_LIST = {
    "600519": "贵州茅台", "600036": "招商银行", "601398": "工商银行",
    "600900": "长江电力", "601288": "农业银行", "601088": "中国神华",
    "601857": "中国石油", "600030": "中信证券", "601628": "中国人寿",
    "600276": "恒瑞医药", "601318": "中国平安", "600309": "万华化学",
    "600887": "伊利股份", "601166": "兴业银行", "600028": "中国石化",
    "601888": "中国中免", "600031": "三一重工", "601012": "隆基绿能",
    "603288": "海天味业", "600009": "上海机场", "600436": "片仔癀",
    "603259": "药明康德", "601668": "中国建筑", "600048": "保利发展",
    "600585": "海螺水泥", "601601": "中国太保", "603501": "韦尔股份",
    "600690": "海尔智家", "601818": "光大银行", "600893": "航发动力",
    "601688": "华泰证券", "601211": "国泰君安", "600837": "海通证券",
    "601669": "中国电建", "600406": "国电南瑞", "601989": "中国重工",
    "601186": "中国铁建", "601390": "中国中铁", "601800": "中国交建",
    "601618": "中国中冶", "601117": "中国化学",
    "000001": "平安银行", "000002": "万科A", "000333": "美的集团",
    "000858": "五粮液", "002594": "比亚迪", "000568": "泸州老窖",
    "000538": "云南白药", "002415": "海康威视", "000725": "京东方A",
    "000063": "中兴通讯", "002142": "宁波银行", "000651": "格力电器",
    "000895": "双汇发展", "002304": "洋河股份", "000776": "广发证券",
    "002271": "东方雨虹", "000938": "中芯国际", "002230": "科大讯飞",
    "000100": "TCL科技", "002460": "赣锋锂业", "002024": "苏宁易购",
    "000625": "长安汽车", "002007": "华兰生物", "000768": "中航西飞",
    "002049": "紫光国微", "000166": "申万宏源", "000069": "华侨城A",
    "000338": "潍柴动力", "000983": "山西焦煤",
    "000921": "海信家电", "000999": "华润三九", "000750": "国海证券",
    "300750": "宁德时代", "300059": "东方财富", "300760": "迈瑞医疗",
    "300124": "汇川技术", "300015": "爱尔眼科", "300014": "亿纬锂能",
    "300433": "蓝思科技", "300003": "乐普医疗", "300122": "智飞生物",
    "300142": "沃森生物", "300408": "三环集团", "300413": "芒果超媒",
    "300001": "特锐德", "300033": "同花顺", "300496": "中科创达",
    "300136": "信维通信", "300383": "光环新网", "300316": "晶盛机电",
    "300454": "深信服", "300661": "圣邦股份", "300285": "国瓷材料",
    "300751": "迈为股份", "300618": "寒锐钴业", "300677": "英科医疗",
    "300776": "帝尔激光", "300073": "当升科技", "300724": "捷佳伟创",
    "300274": "阳光电源", "300763": "锦浪科技", "300012": "华测检测",
    "300223": "北京君正", "300373": "扬杰科技",
    "300207": "欣旺达", "300118": "东方日升", "300450": "先导智能",
    "300604": "长川科技", "300395": "菲利华",
    "300529": "健帆生物", "300601": "康泰生物", "300676": "华大基因",
    "300595": "欧普康视", "300357": "我武生物", "300832": "新产业",
    "300009": "安科生物", "300463": "迈克生物", "300026": "红日药业",
    "300244": "迪安诊断", "300298": "三诺生物",
    "300347": "泰格医药", "300558": "贝达药业", "300630": "普利制药",
    "300841": "康华生物", "300896": "爱美客", "300999": "金龙鱼",
    "300888": "稳健医疗", "300866": "安克创新",
}


# ==================== 多进程任务函数（模块顶层） ====================
def _update_one_stock_mp(code, days_to_fetch, quote_data, data_dir, skip_baostock=False):
    import time
    import random
    time.sleep(random.uniform(0.1, 0.5))
    try:
        from utils.akshare_fetcher import AKShareFetcher
        fetcher = AKShareFetcher(data_dir)
        existing_df = fetcher.csv_manager.read_stock(code)
        old_count = len(existing_df)
        df = fetcher.fetch_stock_update(code, days=days_to_fetch, skip_baostock_login=skip_baostock, verbose=False)
        if df is not None and not df.empty:
            is_tencent_kline = skip_baostock or df.attrs.get('source') == 'tencent'
            # 只保留新日期，防止腾讯K线(缺字段)覆盖baostock完整数据
            if not existing_df.empty:
                existing_dates = set(pd.to_datetime(existing_df['date']).dt.strftime('%Y-%m-%d'))
                df['date_str'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df = df[~df['date_str'].isin(existing_dates)]
                df = df.drop(columns=['date_str'])
            if df.empty:
                return (code, True, 0, None)  # 无新数据

            # Tencent K-line rows do not carry the same complete fundamentals as
            # baostock, and their hfq baseline can differ from existing baostock
            # history. In fast mode, only persist the latest row after quote
            # enrichment, then reject rows that create an obvious price-level
            # discontinuity against the local history.
            if is_tencent_kline and len(df) > 1:
                df = df.sort_values('date', ascending=False).head(1).copy()

            # 从腾讯行情数据补全腾讯K线缺失字段
            stock_q = quote_data.get(code, {})
            # 补 market_cap (整列)
            has_mc = 'market_cap' in df.columns and not df['market_cap'].isna().all() and (df['market_cap'] > 0).any()
            if not has_mc:
                df['market_cap'] = stock_q.get('market_cap', 0)
            # 补最新一行字段（腾讯K线缺失 turnover/PE/PB/amount/change_pct）
            if stock_q:
                latest_idx = df.index[0]  # 降序排列，第0行=最新
                for col, key in [
                    ('turnover', 'turnover'),
                    ('pe_dynamic', 'pe_dynamic'),
                    ('pb', 'pb'),
                    ('change_pct', 'change_pct'),
                    ('amount', 'amount'),
                ]:
                    val = stock_q.get(key)
                    if col in df.columns and val is not None and val != 0:
                        if pd.isna(df.at[latest_idx, col]) or df.at[latest_idx, col] == 0:
                            df.at[latest_idx, col] = val

            if is_tencent_kline and not existing_df.empty and not df.empty:
                existing_sorted = existing_df.copy()
                existing_sorted['date'] = pd.to_datetime(existing_sorted['date'])
                existing_sorted = existing_sorted.sort_values('date', ascending=False)
                anchor = existing_sorted.iloc[0]
                anchor_close = pd.to_numeric(pd.Series([anchor.get('close')]), errors='coerce').iloc[0]

                kept_rows = []
                prev_close = anchor_close
                for _, row in df.sort_values('date').iterrows():
                    row_close = pd.to_numeric(pd.Series([row.get('close')]), errors='coerce').iloc[0]
                    if pd.notna(prev_close) and prev_close > 0 and pd.notna(row_close) and row_close > 0:
                        gap = abs(row_close / prev_close - 1)
                        if gap > 0.35:
                            continue
                    kept_rows.append(row)
                    prev_close = row_close
                if not kept_rows:
                    return (code, True, 0, None)
                df = pd.DataFrame(kept_rows).sort_values('date', ascending=False)

            fetcher.csv_manager.update_stock(code, df)
            new_df = fetcher.csv_manager.read_stock(code)
            new_count = len(new_df)
            added = new_count - old_count
            return (code, True, added, None)
        else:
            return (code, False, 0, "获取数据失败")
    except Exception as e:
        return (code, False, 0, str(e))


def _init_one_stock_mp(code, market_cap_map, data_dir):
    import time
    import random
    time.sleep(random.uniform(0.2, 1.0))

    try:
        from utils.akshare_fetcher import AKShareFetcher
        fetcher = AKShareFetcher(data_dir)
        df = fetcher.fetch_stock_history(code, years=30, market_cap=market_cap_map.get(code))
        if df is not None and not df.empty:
            if len(df) < 10 or df['close'].mean() <= 0:
                return (code, False, "数据无效")
            fetcher.csv_manager.write_stock(code, df)
            return (code, True, None)
        else:
            return (code, False, "获取数据失败")
    except Exception as e:
        return (code, False, str(e))


from utils.baostock_lock import serialized_baostock


class AKShareFetcher:
    def __init__(self, data_dir="data"):
        self.csv_manager = CSVManager(data_dir)
        self.full_data_dir = Path(data_dir)
        self.stock_names_file = Path(data_dir) / 'stock_names.json'
        self.data_dir = data_dir

    def _load_local_stock_names(self):
        if self.stock_names_file.exists():
            try:
                with open(self.stock_names_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {}

    def _save_stock_names(self, stock_dict):
        try:
            with open(self.stock_names_file, 'w', encoding='utf-8') as f:
                json.dump(stock_dict, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"  保存股票名称失败: {e}")

    @serialized_baostock
    def _fetch_stock_list_baostock(self):
        """使用baostock获取全量A股列表（含退市股），30天磁盘缓存"""
        import baostock as bs
        from contextlib import redirect_stdout
        import os
        import re

        cache_file = self.full_data_dir / 'all_stocks_baostock.json'
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached = json.load(f)
                cache_time = cached.get('_cache_time', 0)
                if time.time() - cache_time < 30 * 24 * 3600:
                    stocks = {k: v for k, v in cached.items() if not k.startswith('_')}
                    if len(stocks) > 4000:
                        return stocks
            except:
                pass

        stocks = {}
        try:
            with redirect_stdout(open(os.devnull, 'w')):
                lg = bs.login()
            if lg.error_code != '0':
                print(f"  baostock登录失败: {lg.error_msg}")
                return {}
        except Exception as e:
            print(f"  baostock登录异常: {e}")
            return {}

        try:
            rs = bs.query_stock_basic()
            if rs.error_code != '0':
                print(f"  baostock query_stock_basic失败: {rs.error_msg}")
                return {}

            exclude_keywords = ['债', '基', 'ETF', 'LOF', '基金', '理财', '信托',
                              'B股', '指数', '国债', '企债', '转债', '回购', 'R-', 'GC']
            code_pattern = re.compile(r'^(00|30|60|68)\d{4}$')

            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                # fields: code, code_name, ipoDate, outDate, type, status
                bs_code = row[0]
                name = row[1]
                stock_type = row[4]  # 1=股票

                if stock_type != '1':
                    continue

                # 提取纯数字代码 (sh.600519 -> 600519)
                if '.' in bs_code:
                    pure_code = bs_code.split('.')[1]
                else:
                    continue

                if not code_pattern.match(pure_code):
                    continue
                if any(kw in name for kw in exclude_keywords):
                    continue

                stocks[pure_code] = name

            # 缓存30天
            if stocks:
                stocks['_cache_time'] = time.time()
                try:
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        json.dump(stocks, f, ensure_ascii=False, indent=2)
                except:
                    pass

        except Exception as e:
            print(f"  baostock股票列表查询异常: {e}")
        finally:
            try:
                with redirect_stdout(open(os.devnull, 'w')):
                    bs.logout()
            except:
                pass

        # 返回不含元数据的纯股票字典
        return {k: v for k, v in stocks.items() if not k.startswith('_')}

    def _fetch_delisted_stocks_akshare(self):
        """备选方案：使用akshare退市股API获取退市A股列表，7天磁盘缓存"""
        cache_file = self.full_data_dir / 'delisted_stocks_akshare.json'
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached = json.load(f)
                cache_time = cached.get('_cache_time', 0)
                if time.time() - cache_time < 7 * 24 * 3600:
                    stocks = {k: v for k, v in cached.items() if not k.startswith('_')}
                    if len(stocks) > 100:
                        return stocks
            except:
                pass

        stocks = {}
        try:
            import akshare as ak
            # 上交所退市股
            try:
                sh = ak.stock_info_sh_delist()
                if not sh.empty:
                    for _, row in sh.iterrows():
                        code = str(row.iloc[0]).strip()
                        name = str(row.iloc[1]).strip()
                        if not code.isdigit() or len(code) != 6:
                            continue
                        if not code.startswith(('60', '68')):
                            continue
                        stocks[code] = name
            except Exception as e:
                print(f"  akshare上交所退市股获取失败: {e}")

            # 深交所退市股
            try:
                sz = ak.stock_info_sz_delist()
                if not sz.empty:
                    for _, row in sz.iterrows():
                        code = str(row.iloc[0]).strip()
                        name = str(row.iloc[1]).strip()
                        if not code.isdigit() or len(code) != 6:
                            continue
                        if not code.startswith(('00', '30')):
                            continue
                        stocks[code] = name
            except Exception as e:
                print(f"  akshare深交所退市股获取失败: {e}")

            # 为不含退/ST的退市股名称加退后缀，确保被现有过滤器识别
            if stocks:
                for code in list(stocks.keys()):
                    if code.startswith('_'):
                        continue
                    name = stocks[code]
                    if '退' not in name and 'ST' not in name:
                        stocks[code] = name + '退'

                stocks['_cache_time'] = time.time()
                try:
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        json.dump(stocks, f, ensure_ascii=False, indent=2)
                except:
                    pass

        except Exception as e:
            print(f"  akshare退市股列表获取异常: {e}")

        return {k: v for k, v in stocks.items() if not k.startswith('_')}

    def _fetch_supplemental_stocks(self):
        """获取补充股票列表（退市/历史股票），优先baostock全量，备选akshare退市API"""
        # 优先：baostock全量列表（含退市股+当前股，缓存30天）
        stocks = self._fetch_stock_list_baostock()
        if stocks:
            print(f"  baostock全量列表: {len(stocks)} 只")
            return stocks

        # 备选：akshare退市股API
        stocks = self._fetch_delisted_stocks_akshare()
        if stocks:
            print(f"  akshare退市股列表: {len(stocks)} 只")
        return stocks

    def _fetch_quote_batch_tencent(self, stock_codes):
        """批量获取腾讯实时行情数据（流通市值+换手率+PE/PB+成交额+涨跌幅）

        返回: {code: {market_cap, turnover, pe_dynamic, pb, change_pct, amount, volume_ratio, ...}}
        腾讯 qt.gtimg.cn 字段映射:
          [32] change_pct(%)  [37] amount(万)  [38] turnover(%)
          [39] PE(TTM)  [44] 流通市值(亿)  [45] 总市值(亿)  [46] PB
          [49] 量比  [33] high  [34] low  [43] 振幅(%)
        """
        quote_data = {}
        batch_size = 80
        total = len(stock_codes)
        try:
            for i in range(0, total, batch_size):
                batch = stock_codes[i:i + batch_size]
                query_codes = []
                for code in batch:
                    if code.startswith('6') or code.startswith('8'):
                        query_codes.append(f"sh{code}")
                    else:
                        query_codes.append(f"sz{code}")
                url = f"https://qt.gtimg.cn/q={','.join(query_codes)}"
                resp = session.get(url, timeout=REQUEST_TIMEOUT, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                lines = resp.text.strip().split(';')
                for line in lines:
                    if 'v_' in line and '~' in line:
                        try:
                            code_match = line.split('v_')[1].split('=')[0] if 'v_' in line else ''
                            if not code_match or len(code_match) < 8:
                                continue
                            code = code_match[2:]
                            parts = line.split('~')
                            if len(parts) < 50:
                                continue

                            def _f(idx):
                                """安全取float字段"""
                                try:
                                    v = parts[idx].strip()
                                    return float(v) if v else 0.0
                                except:
                                    return 0.0

                            market_cap_raw = _f(45)  # 总市值(亿)
                            circ_cap_raw = _f(44)    # 流通市值(亿)

                            quote_data[code] = {
                                'market_cap': int(market_cap_raw * 1e8) if market_cap_raw > 0 else 0,
                                'circ_market_cap': int(circ_cap_raw * 1e8) if circ_cap_raw > 0 else 0,
                                'turnover': _f(38),         # 换手率(%)
                                'pe_dynamic': _f(39) if _f(39) > 0 else None,   # PE(TTM)
                                'pb': _f(46) if _f(46) > 0 else None,           # PB
                                'change_pct': _f(32),       # 涨跌幅(%)
                                'amount': _f(37) * 10000,   # 成交额(万元→元)
                                'volume_ratio': _f(49),     # 量比
                                'amplitude': _f(43),        # 振幅(%)
                            }
                        except:
                            continue
                if (i + batch_size) % 500 == 0 and i > 0:
                    print(f"  已获取 {min(i + batch_size, total)}/{total} 只行情数据...")
                    time.sleep(0.1)
        except Exception as e:
            print(f"  腾讯行情接口获取失败: {e}")
        return quote_data

    def _fetch_market_cap_tencent(self, stock_codes):
        """兼容旧接口：批量获取流通市值（从行情接口提取）"""
        quote_data = self._fetch_quote_batch_tencent(stock_codes)
        return {code: info['market_cap'] for code, info in quote_data.items()
                if info.get('market_cap', 0) > 0}

    def get_all_stock_codes(self, max_retries=3):
        print("正在获取A股股票列表...")
        # 优先腾讯 (速度快), akshare 备用
        for attempt in range(max_retries):
            try:
                print(f"  尝试腾讯接口 (第{attempt+1}/{max_retries}次)...")
                stocks = self._fetch_stock_list_http()
                if stocks:
                    filtered = {}
                    code_pattern = r'^(00|30|60|68|88)\d{4}$'
                    exclude_keywords = ['债', '基', 'ETF', 'LOF', '基金', '理财', '信托', 'B股', '指数', '国债', '企债', '转债', '回购', 'R-', 'GC']
                    for code, name in stocks.items():
                        if not pd.Series([code]).str.match(code_pattern).iloc[0]:
                            continue
                        if any(kw in name for kw in exclude_keywords):
                            continue
                        filtered[code] = name
                    if filtered:
                        # 补充退市/历史股票（用于回测，避免幸存者偏差）
                        supplemental = self._fetch_supplemental_stocks()
                        if supplemental:
                            added = 0
                            for code, name in supplemental.items():
                                if code not in filtered:
                                    filtered[code] = name
                                    added += 1
                            if added > 0:
                                print(f"  补充退市/历史股票: {added} 只 (总计 {len(filtered)} 只)")
                        print(f"[OK] 腾讯获取成功: {len(filtered)} 只A股股票")
                        self._save_stock_names(filtered)
                        return filtered
            except Exception as e:
                print(f"  腾讯失败: {e}")
                time.sleep(1)
        for attempt in range(max_retries):
            try:
                print(f"  尝试akshare (第{attempt+1}/{max_retries}次)...")
                sh_df = ak.stock_sh_a_spot_em()
                sz_df = ak.stock_sz_a_spot_em()
                all_stocks = pd.concat([sh_df[['代码', '名称']], sz_df[['代码', '名称']]])
                all_stocks = all_stocks.drop_duplicates(subset=['代码'])
                code_pattern = r'^(00|30|60|68|88)\d{4}$'
                all_stocks = all_stocks[all_stocks['代码'].str.match(code_pattern)]
                exclude_keywords = ['债', '基', 'ETF', 'LOF', '基金', '理财', '信托', 'B股', '指数', '国债', '企债', '转债', '回购', 'R-', 'GC']
                for keyword in exclude_keywords:
                    all_stocks = all_stocks[~all_stocks['名称'].str.contains(keyword, na=False)]
                stock_dict = dict(zip(all_stocks['代码'], all_stocks['名称']))
                # 补充退市/历史股票
                supplemental = self._fetch_supplemental_stocks()
                if supplemental:
                    added = 0
                    for code, name in supplemental.items():
                        if code not in stock_dict:
                            stock_dict[code] = name
                            added += 1
                    if added > 0:
                        print(f"  补充退市/历史股票: {added} 只 (总计 {len(stock_dict)} 只)")
                print(f"[OK] akshare获取成功: {len(stock_dict)} 只A股股票")
                self._save_stock_names(stock_dict)
                return stock_dict
            except Exception as e:
                print(f"  akshare失败: {e}")
                time.sleep(2 ** attempt)
        print("\n网络连接失败，尝试加载本地缓存...")
        local_stocks = self._load_local_stock_names()
        if local_stocks:
            print(f"[OK] 从本地缓存加载: {len(local_stocks)} 只股票")
            return local_stocks
        print("\n使用内置默认股票列表...")
        print(f"[OK] 加载默认列表: {len(DEFAULT_STOCK_LIST)} 只股票")
        return DEFAULT_STOCK_LIST.copy()

    def _fetch_stock_list_http(self):
        try:
            stocks = {}
            sh_ranges = []
            for prefix in range(600, 610):
                sh_ranges.append((f'{prefix}000', f'{prefix}999'))
            sh_ranges.extend([
                ('601000', '601999'),
                ('603000', '603999'),
                ('605000', '605999'),
                ('688000', '689999'),
            ])
            sz_ranges = [
                ('000001', '009999'),
                ('001000', '001999'),
                ('002000', '002999'),
                ('003000', '003999'),
                ('300000', '309999'),
            ]
            cached_stocks = self._load_local_stock_names()
            if len(cached_stocks) >= 5000:
                print(f"  从本地缓存加载 {len(cached_stocks)} 只股票")
                return cached_stocks
            print(f"\n  正在通过腾讯接口获取股票列表...")
            batch_size = 100
            all_codes = []
            step = 1
            for start, end in sh_ranges:
                for code_num in range(int(start), int(end) + 1, step):
                    all_codes.append(str(code_num).zfill(6))
            for start, end in sz_ranges:
                for code_num in range(int(start), int(end) + 1, step):
                    all_codes.append(str(code_num).zfill(6))
            total_batches = (len(all_codes) + batch_size - 1) // batch_size
            for i in range(0, len(all_codes), batch_size):
                batch = all_codes[i:i + batch_size]
                batch_num = i // batch_size + 1
                query_codes_list = []
                for c in batch:
                    if c.startswith('6') or c.startswith('8'):
                        query_codes_list.append(f"sh{c}")
                    elif c.startswith('0') or c.startswith('3'):
                        query_codes_list.append(f"sz{c}")
                if not query_codes_list:
                    continue
                query_codes = ','.join(query_codes_list)
                url = f"https://qt.gtimg.cn/q={query_codes}"
                try:
                    resp = session.get(url, timeout=REQUEST_TIMEOUT, headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    })
                    lines = resp.text.strip().split(';')
                    for line in lines:
                        if 'v_' in line and '~' in line:
                            parts = line.split('~')
                            if len(parts) >= 45:
                                code_match = line.split('v_')[1].split('=')[0] if 'v_' in line else ''
                                if code_match:
                                    code = code_match[2:]
                                    name = parts[1] if len(parts) > 1 else ''
                                    exclude_keywords = ['债', '基', 'ETF', 'LOF', '理财', '信托', 'B股', '指数']
                                    if not name or name == '""' or any(x in name for x in exclude_keywords):
                                        continue
                                    if '退' in name:
                                        continue
                                    try:
                                        if float(parts[3]) <= 0:
                                            continue
                                    except:
                                        continue
                                    stocks[code] = name
                    if batch_num % 50 == 0:
                        print(f"    进度: {batch_num}/{total_batches} 批次, 已获取 {len(stocks)} 只...")
                    time.sleep(0.05)
                except Exception as e:
                    continue
            if stocks:
                print(f"  [OK]通过腾讯接口获取: {len(stocks)} 只股票")
                return stocks
            return DEFAULT_STOCK_LIST.copy()
        except Exception as e:
            print(f"  HTTP获取失败: {e}")
            return DEFAULT_STOCK_LIST.copy()

    # CSV输出列（baostock提供，不含振幅/涨跌额/量比/静态PE）
    _OUTPUT_COLUMNS = [
        'date', 'open', 'high', 'low', 'close', 'close_raw', 'volume', 'amount',
        'turnover', 'change_pct', 'pe_dynamic', 'pb', 'ps', 'pcf', 'market_cap',
    ]
    @serialized_baostock
    def _fetch_stock_history_baostock(self, stock_code, years=30):
        """baostock: 后复权K线 + 换手率 + PE/PB/PS/PCF (不含市值)"""
        import baostock as bs
        from contextlib import redirect_stdout
        import os
        def sf(val):
            try: return float(val) if val not in ('', None) else None
            except: return None
        def si(val):
            try: return int(float(val)) if val not in ('', None) else 0
            except: return 0
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * years)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        if stock_code.startswith('6'):
            bs_code = f'sh.{stock_code}'
        else:
            bs_code = f'sz.{stock_code}'

        # baostock 字段: date,code,open,high,low,close,preclose,volume,amount,
        #               adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST
        fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM"

        for attempt in range(2):
            try:
                with redirect_stdout(open(os.devnull, 'w')):
                    lg = bs.login()
                if lg.error_code != '0':
                    if attempt < 1: time.sleep(2); continue
                    return None
                rs = bs.query_history_k_data_plus(
                    bs_code, fields,
                    start_date=start_str, end_date=end_str,
                    frequency="d", adjustflag="1"
                )
                if rs.error_code != '0':
                    if attempt < 1:
                        with redirect_stdout(open(os.devnull, 'w')): bs.logout()
                        time.sleep(2); continue
                    return None
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    row = rs.get_row_data()
                    # 0:date  1:code  2:open  3:high  4:low  5:close
                    # 6:volume  7:amount  8:turn  9:pctChg
                    # 10:peTTM  11:pbMRQ  12:psTTM  13:pcfNcfTTM
                    data_list.append({
                        'date': row[0],
                        'open': sf(row[2]) or 0.0,
                        'high': sf(row[3]) or 0.0,
                        'low': sf(row[4]) or 0.0,
                        'close': sf(row[5]) or 0.0,
                        'volume': si(row[6]),
                        'amount': sf(row[7]) or 0.0,
                        'turnover': sf(row[8]) or 0.0,
                        'change_pct': sf(row[9]) or 0.0,
                        'pe_dynamic': sf(row[10]),       # peTTM
                        'pb': sf(row[11]),                # pbMRQ
                        'ps': sf(row[12]),                # psTTM
                        'pcf': sf(row[13]),               # pcfNcfTTM
                        # 以下 baostock 不提供, 填占位
                        'amplitude': 0.0,
                        'change': 0.0,
                        'volume_ratio': None,
                        'pe_static': None,
                        'total_cap': 0,
                        'circ_cap': 0,
                    })
                if data_list:
                    df = pd.DataFrame(data_list)
                    df['date'] = pd.to_datetime(df['date'])
                    df['market_cap'] = 0  # baostock 无市值
                    df = df.sort_values('date', ascending=False)
                    return df
                return None
            except Exception:
                if attempt < 1: time.sleep(2); continue
                return None
            finally:
                with redirect_stdout(open(os.devnull, 'w')):
                    bs.logout()
        return None

    def fetch_stock_history(self, stock_code, years=30, market_cap=None):
        """获取股票历史K线: baostock(后复权) → 腾讯(后复权备选)"""
        # 主力: baostock 后复权
        df = self._fetch_stock_history_baostock(stock_code, years)
        if df is not None and not df.empty:
            if market_cap is not None:
                df['market_cap'] = market_cap
            print(f"[OK] baostock {len(df)}条")
            return df
        # baostock失败，尝试腾讯API备选
        df = self._fetch_stock_history_tencent(stock_code, years)
        if df is not None and not df.empty:
            if market_cap is not None:
                df['market_cap'] = market_cap
            print(f"[OK] 腾讯备选 {len(df)}条")
            return df
        print(f"  [ERR] {stock_code} 历史数据失败 (baostock+腾讯均不可用)")
        return None


    def _fetch_stock_update_eastmoney(self, stock_code, days=10):
        """Eastmoney public daily K-line update with local adjusted-price anchoring."""
        try:
            from utils.eastmoney_fetcher import EastmoneyFetcher
            em = EastmoneyFetcher()
            df = em.fetch_recent_update(stock_code, days=days + 10, adjust="hfq")
            if df is None or df.empty:
                return None

            existing = self.csv_manager.read_stock(stock_code)
            if existing is not None and not existing.empty and 'date' in existing.columns and 'close' in existing.columns:
                existing = existing.copy()
                existing['date'] = pd.to_datetime(existing['date'])
                existing = existing.sort_values('date', ascending=False)
                anchor = existing.iloc[0]
                anchor_date = pd.to_datetime(anchor['date']).strftime('%Y-%m-%d')
                try:
                    anchor_close = float(anchor['close'])
                except Exception:
                    anchor_close = 0.0
                if anchor_close > 0:
                    df, factor = em.rescale_ohlc_to_anchor(df, anchor_date, anchor_close)
                    if factor is None:
                        return None

            for col in ['pe_dynamic', 'pb', 'ps', 'pcf']:
                if col not in df.columns:
                    df[col] = None
            if 'market_cap' not in df.columns:
                df['market_cap'] = 0
            df = df.sort_values('date', ascending=False).head(days)
            df.attrs['source'] = 'eastmoney'
            return df
        except Exception:
            return None

    def fetch_stock_update(self, stock_code, days=10, skip_baostock_login=False, verbose=False):
        """Incremental update with every BaoStock-capable path serialized."""
        df = self._fetch_stock_update_eastmoney(stock_code, days)
        if df is not None and not df.empty:
            if verbose:
                print(f"[OK](eastmoney update {len(df)} rows)")
            return df
        if skip_baostock_login:
            df = self._fetch_stock_update_tencent(stock_code, days)
            if df is not None and not df.empty:
                if verbose: print(f"[OK](腾讯更新 {len(df)}条)")
                return df
            return None
        return self._fetch_stock_update_serialized(
            stock_code,
            days=days,
            verbose=verbose,
        )

    @serialized_baostock
    def _fetch_stock_update_serialized(self, stock_code, days=10, verbose=False):
        """增量更新: 东方财富后复权(本地锚定) → baostock后复权 → 腾讯后复权备选"""
        import baostock as bs
        from contextlib import redirect_stdout
        import os
        import socket
        def sf(val):
            try: return float(val) if val not in ('', None) else None
            except: return None
        def si(val):
            try: return int(float(val)) if val not in ('', None) else 0
            except: return 0

        # 设置socket超时防止baostock login挂死
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(8)
        baostock_ok = False
        try:
            for attempt in range(1):
                try:
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=days + 10)
                    start_str = start_date.strftime('%Y-%m-%d')
                    end_str = end_date.strftime('%Y-%m-%d')
                    if stock_code.startswith('6'):
                        bs_code = f'sh.{stock_code}'
                    else:
                        bs_code = f'sz.{stock_code}'
                    with redirect_stdout(open(os.devnull, 'w')):
                        lg = bs.login()
                    if lg.error_code != '0':
                        break
                    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM"
                    rs = bs.query_history_k_data_plus(
                        bs_code, fields,
                        start_date=start_str, end_date=end_str,
                        frequency="d", adjustflag="1"
                    )
                    if rs.error_code != '0':
                        with redirect_stdout(open(os.devnull, 'w')): bs.logout()
                        break
                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        row = rs.get_row_data()
                        data_list.append({
                            'date': row[0],
                            'open': sf(row[2]) or 0.0,
                            'high': sf(row[3]) or 0.0,
                            'low': sf(row[4]) or 0.0,
                            'close': sf(row[5]) or 0.0,
                            'volume': si(row[6]),
                            'amount': sf(row[7]) or 0.0,
                            'turnover': sf(row[8]) or 0.0,
                            'change_pct': sf(row[9]) or 0.0,
                            'pe_dynamic': sf(row[10]),
                            'pb': sf(row[11]),
                            'ps': sf(row[12]),
                            'pcf': sf(row[13]),
                        })
                    with redirect_stdout(open(os.devnull, 'w')):
                        bs.logout()
                    if data_list:
                        df = pd.DataFrame(data_list)
                        df['date'] = pd.to_datetime(df['date'])
                        df = df.sort_values('date', ascending=False)
                        df = df.head(days)
                        df.attrs['source'] = 'baostock'
                        if verbose: print(f"[OK](baostock更新 {len(df)}条)")
                        baostock_ok = True
                        return df
                    break
                except Exception:
                    break
                finally:
                    try:
                        with redirect_stdout(open(os.devnull, 'w')):
                            bs.logout()
                    except Exception:
                        pass
        except Exception:
            pass
        finally:
            socket.setdefaulttimeout(old_timeout)

        # baostock失败，尝试腾讯API备选
        if verbose: print(f"  baostock不可用，尝试腾讯API...")
        df = self._fetch_stock_update_tencent(stock_code, days)
        if df is not None and not df.empty:
            if verbose: print(f"[OK](腾讯更新 {len(df)}条)")
            return df
        return None


    def _fetch_stock_update_tencent(self, stock_code, days=10):
        """腾讯K线API增量更新（baostock封禁时的备选），后复权(hfq)，返回DataFrame或None"""
        import requests
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 10)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        if stock_code.startswith(('6', '8')):
            tk_code = f'sh{stock_code}'
        else:
            tk_code = f'sz{stock_code}'

        url = 'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
        params = {'param': f'{tk_code},day,{start_str},{end_str},{days+10},hfq'}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://gu.qq.com/',
        }

        for attempt in range(3):
            try:
                resp = session.get(url, params=params, headers=headers, timeout=15)
                if resp.status_code != 200:
                    if attempt < 2: time.sleep(2 ** attempt); continue
                    return None
                # 检测WAF拦截
                if 'waf.tencent.com' in resp.text or '<html' in resp.text[:100].lower():
                    if attempt < 2: time.sleep(5 * (attempt + 1)); continue
                    return None
                data = resp.json()
                if data.get('code') != 0:
                    if attempt < 2: time.sleep(1); continue
                    return None
                stock_data = data.get('data', {}).get(tk_code, {})
                klines = stock_data.get('hfqday', [])
                if not klines:
                    return None  # 新股无历史数据，不算错误

                rows = []
                for k in klines:
                    rows.append({
                        'date': k[0],
                        'open': float(k[1]),
                        'close': float(k[2]),
                        'high': float(k[3]),
                        'low': float(k[4]),
                        'volume': int(float(k[5]) * 100),
                        'amount': 0.0,
                        'turnover': 0.0,
                        'change_pct': 0.0,
                        'pe_dynamic': None,
                        'pb': None,
                        'ps': None,
                        'pcf': None,
                    })
                if rows:
                    df = pd.DataFrame(rows)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date', ascending=False)
                    df = df.head(days)
                    df.attrs['source'] = 'tencent'
                    return df
                return None
            except Exception:
                if attempt < 2: time.sleep(2 ** attempt); continue
        return None

    def _fetch_stock_history_tencent(self, stock_code, years=30):
        """腾讯K线API历史数据（baostock封禁时的备选），后复权，最多约640个交易日"""
        import requests
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * years)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        if stock_code.startswith(('6', '8')):
            tk_code = f'sh{stock_code}'
        else:
            tk_code = f'sz{stock_code}'

        url = 'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
        # 腾讯接口最多返回约640条，请求足够大的count
        params = {'param': f'{tk_code},day,{start_str},{end_str},2000,hfq'}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://gu.qq.com/',
        }

        for attempt in range(3):
            try:
                resp = session.get(url, params=params, headers=headers, timeout=15)
                if resp.status_code != 200:
                    if attempt < 2: time.sleep(2 ** attempt); continue
                    return None
                if 'waf.tencent.com' in resp.text or '<html' in resp.text[:100].lower():
                    if attempt < 2: time.sleep(5 * (attempt + 1)); continue
                    return None
                data = resp.json()
                if data.get('code') != 0:
                    if attempt < 2: time.sleep(1); continue
                    return None
                stock_data = data.get('data', {}).get(tk_code, {})
                klines = stock_data.get('hfqday', [])
                if not klines:
                    return None

                rows = []
                prev_close = None
                for k in klines:
                    close_val = float(k[2])
                    change_pct = 0.0
                    if prev_close is not None and prev_close > 0:
                        change_pct = (close_val - prev_close) / prev_close * 100
                    prev_close = close_val
                    rows.append({
                        'date': k[0],
                        'open': float(k[1]),
                        'close': close_val,
                        'high': float(k[3]),
                        'low': float(k[4]),
                        'volume': int(float(k[5]) * 100),
                        'amount': 0.0,
                        'turnover': 0.0,
                        'change_pct': change_pct,
                        'pe_dynamic': None,
                        'pb': None,
                        'ps': None,
                        'pcf': None,
                    })
                if rows:
                    df = pd.DataFrame(rows)
                    df['date'] = pd.to_datetime(df['date'])
                    df['market_cap'] = 0
                    df = df.sort_values('date', ascending=False)
                    return df
                return None
            except Exception:
                if attempt < 2: time.sleep(2 ** attempt); continue
        return None


    def init_full_data(self, max_stocks=None, skip_failed=False, retry_failed=True, force_full=False):
        """
        用 THSDK 重建全量历史（隔离 staging 校验后切换）。
        :param max_stocks: 限制总数
        :param skip_failed: 是否跳过之前失败的股票（True=跳过，False=重试）
        :param retry_failed: 如果存在失败列表，是否仅重试失败股票（True=仅重试失败，False=全量）
        :param force_full: 强制全量抓取，忽略失败列表
        """
        # The repository's public ``init`` command now has one authoritative
        # source.  A bounded run is staging-only for safe smoke tests; the
        # unbounded command commits only after every stock passes validation.
        from tools.rebuild_all_ths import rebuild as rebuild_ths
        return rebuild_ths(
            self.full_data_dir,
            max_stocks=max_stocks,
            commit=max_stocks is None,
        )

        # Legacy code is intentionally unreachable; it is retained below only
        # for reference while old repair scripts are migrated separately.
        import baostock as bs
        from contextlib import redirect_stdout
        import os
        import json
        from pathlib import Path

        # 读取完整股票列表
        stock_dict = self.get_all_stock_codes()
        if not stock_dict:
            print("无法获取股票列表")
            return

        all_stock_codes = list(stock_dict.keys())
        failed_stocks_file = self.full_data_dir / 'failed_stocks.json'

        # 决定要处理的股票列表
        if force_full:
            # 强制全量，忽略失败列表
            target_codes = all_stock_codes
            print("强制全量模式，将重新抓取所有股票")
        elif retry_failed and failed_stocks_file.exists():
            try:
                with open(failed_stocks_file, 'r', encoding='utf-8') as f:
                    failed_codes = json.load(f)
                if failed_codes:
                    target_codes = failed_codes
                    print(f"[LIST] 检测到失败列表，将仅重试 {len(target_codes)} 只失败股票")
                else:
                    target_codes = all_stock_codes
            except:
                target_codes = all_stock_codes
        else:
            target_codes = all_stock_codes

        # 如果指定了 skip_failed，则从列表中移除已知失败项（旧逻辑保留）
        if skip_failed and failed_stocks_file.exists():
            try:
                with open(failed_stocks_file, 'r', encoding='utf-8') as f:
                    known_failed = set(json.load(f))
                target_codes = [c for c in target_codes if c not in known_failed]
                print(f"  跳过 {len(known_failed)} 只历史失败股票")
            except:
                pass

        if max_stocks:
            target_codes = target_codes[:max_stocks]

        if not target_codes:
            print("没有需要抓取的股票")
            return

        # 获取市值数据（只针对目标股票）
        print("\n正在批量获取流通市值数据...")
        market_cap_map = self._fetch_market_cap_tencent(target_codes)
        if market_cap_map:
            print(f"  [OK]获取到 {len(market_cap_map)} 只股票流通市值")
        else:
            print("  [WARN] 市值获取失败，将使用默认值")

        total = len(target_codes)
        print(f"\n开始抓取 {total} 只股票的历史数据...")
        print("=" * 60)

        tasks = [(code, market_cap_map, self.data_dir) for code in target_codes]

        # baostock 封禁多进程，统一使用单进程模式
        print("  使用单进程模式抓取 (baostock禁多进程)...")
        results = []
        for task in tqdm(tasks, desc="抓取进度"):
            result = _init_one_stock_mp(*task)
            results.append(result)

        # 统计结果
        success = 0
        failed_codes = []
        for code, success_flag, error in results:
            if success_flag:
                success += 1
            else:
                failed_codes.append(code)
                if error:
                    print(f"[FAIL] {code} 失败: {error}")

        # 保存本次失败的股票列表（覆盖或合并？如果是重试模式，应覆盖原列表）
        if failed_codes:
            try:
                with open(failed_stocks_file, 'w', encoding='utf-8') as f:
                    json.dump(failed_codes, f, ensure_ascii=False, indent=2)
                print(f"\n[FILE] 已保存 {len(failed_codes)} 只最终失败的股票到 failed_stocks.json")
            except Exception as e:
                print(f"\n[WARN] 保存失败列表出错: {e}")
        else:
            # 如果全部成功，删除失败列表文件
            if failed_stocks_file.exists():
                failed_stocks_file.unlink()
                print("\n[OK] 所有股票抓取成功，已删除失败列表文件")

        print("=" * 60)
        print(f"全量抓取完成! 成功: {success}, 最终失败: {len(failed_codes)}")
        if failed_codes:
            print(f"提示: 再次运行 python main.py init 将自动重试失败股票")

    def daily_update(self, max_stocks=None, skip_baostock=False):
        """每日增量更新 - 允许任意非盘中时间更新，盘中只拉取不写缓存

        Args:
            max_stocks: 限制更新股票数量
            skip_baostock: 保留旧接口参数，仅为调用方兼容；日更统一走 THSDK。
        """
        from datetime import datetime, timedelta
        import json
        import time
        import pandas as pd
        from tqdm import tqdm

        # Production daily path: THSDK K-lines + THSDK snapshot fields.  The
        # old Eastmoney/Tencent updater remains available as a standalone repair
        # tool, but is deliberately not used here so a normal update cannot
        # reintroduce mixed providers.
        try:
            from tools import update_today_ths as ths_update

            cache_path = self.full_data_dir / ".update_cache.json"
            cache = {}
            if max_stocks is None and cache_path.exists():
                try:
                    cache = json.loads(cache_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    cache = {}
            completed_date = cache.get(
                "last_update_completed_date",
                cache.get("last_update_date"),
            )
            if max_stocks is None and completed_date == ths_update.TODAY_STR and cache.get("last_update_source") == "thsdk":
                ths_update.validate_dataset_manifest(self.full_data_dir)
                print(f"[OK] THSDK 数据已于 {ths_update.TODAY_STR} 更新过，跳过日更")
                return 0
            print(f"[UPDATE] 使用 THSDK K线 + 换手率/流通市值更新 {ths_update.TODAY_STR}")
            exit_code = ths_update.run(data_dir=self.full_data_dir, max_stocks=max_stocks)
        except Exception as exc:
            print(f"[ERROR] THSDK 日更失败，已停止；不会回退到旧数据源: {exc}")
            raise
        if exit_code != 0:
            raise RuntimeError(f"THSDK daily checkpoint validation failed with exit code {exit_code}")
        return 0

        existing_stocks = self.csv_manager.list_all_stocks()
        if not existing_stocks:
            print("没有找到已有数据，请先执行 init")
            return

        if max_stocks:
            existing_stocks = existing_stocks[:max_stocks]

        total = len(existing_stocks)
        updated = 0
        skipped = 0

        print(f"\n开始更新 {total} 只股票的数据...")
        print("=" * 60)

        now = datetime.now()
        today = now.date()
        today_str = today.strftime('%Y-%m-%d')
        current_time = now.time()

        trading_start = datetime.strptime("09:00", "%H:%M").time()
        trading_end = datetime.strptime("15:00", "%H:%M").time()
        is_trading_time = trading_start <= current_time <= trading_end
        is_after_market_close = current_time >= trading_end

        # 读取或初始化更新缓存
        update_cache_file = self.full_data_dir / '.update_cache.json'
        update_cache = {}
        if update_cache_file.exists():
            try:
                with open(update_cache_file, 'r', encoding='utf-8') as f:
                    update_cache = json.load(f)
            except:
                update_cache = {}

        cache_date = update_cache.get('last_update_date')
        cache_date_obj = None
        if cache_date:
            try:
                cache_date_obj = datetime.strptime(cache_date, '%Y-%m-%d').date()
            except:
                pass

        last_trading_day = self._get_last_trading_day(today)
        last_trading_day_str = last_trading_day.strftime('%Y-%m-%d')
        is_today_trading = self._is_trading_day(today)

        # 盘中拦截：交易时段内跳过更新，不请求数据也不刷新缓存
        if is_trading_time and is_today_trading:
            print(f"[SKIP] 交易时段内（{current_time.strftime('%H:%M')}），跳过数据更新，不刷新缓存")
            print("=" * 60)
            return

        # 缓存有效性检查：如果缓存日期有效且不是强制更新模式，则跳过
        if cache_date_obj and not max_stocks:
            # 收盘后且缓存日期为今天 -> 已更新过
            if cache_date_obj == today and is_after_market_close:
                print(f"[OK]数据已于 {cache_date} 收盘后更新过，无需重复更新")
                print("=" * 60)
                return
            # 盘前且缓存日期为上一个交易日 -> 无需重复更新
            if cache_date_obj == last_trading_day and current_time < trading_start and is_today_trading:
                print(f"[OK]当前时间 {current_time.strftime('%H:%M')} 早于开盘，且数据已更新至上一个交易日 {cache_date}，无需重复更新")
                print("=" * 60)
                return
            # 非交易日且缓存日期为上一个交易日 -> 无需重复更新
            if cache_date_obj == last_trading_day and not is_today_trading:
                print(f"[OK]数据已更新至上一个交易日 {cache_date}，今天非交易日，无需重复更新")
                print("=" * 60)
                return

        # 加载股票名称，过滤退市股（退市股无需每日更新）
        stock_names = self._load_local_stock_names()
        delisted_skipped = 0
        if stock_names:
            active_stocks = []
            for code in existing_stocks:
                name = stock_names.get(code, '')
                if '退' in name:
                    delisted_skipped += 1
                    continue
                active_stocks.append(code)
            if delisted_skipped > 0:
                print(f"  跳过 {delisted_skipped} 只退市股票（无需更新）")
                existing_stocks = active_stocks
                total = len(existing_stocks)

        # 检查哪些股票需要更新
        stocks_to_update = []
        print("  正在检查股票更新状态...")
        for idx, code in enumerate(existing_stocks):
            if idx % 500 == 0:
                print(f"    已检查 {idx}/{total} 只股票...")
            path = self.csv_manager.get_stock_path(code)
            if not path.exists():
                stocks_to_update.append((code, 30))
                continue

            try:
                df_quick = pd.read_csv(path, nrows=1)
                if df_quick.empty or 'date' not in df_quick.columns:
                    stocks_to_update.append((code, 30))
                    continue

                latest_date = pd.to_datetime(df_quick.iloc[0]['date']).date()
                if today > latest_date:
                    days_needed = (today - latest_date).days
                    if not is_today_trading:
                        target_date = last_trading_day
                        days_needed = (target_date - latest_date).days
                    days_to_fetch = min(days_needed + 2, 60)
                    if days_needed > 0:
                        stocks_to_update.append((code, days_to_fetch))
                    else:
                        skipped += 1
                elif today == latest_date:
                    if is_after_market_close:
                        stocks_to_update.append((code, 2))
                    else:
                        skipped += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"  读取 {code} 首行异常: {e}，将删除旧文件并重新获取")
                try:
                    path.unlink()
                except:
                    pass
                stocks_to_update.append((code, 30))

        need_update = len(stocks_to_update)
        print(f"  需要更新: {need_update} 只, 已最新: {skipped} 只")

        if need_update == 0:
            print("[OK]所有数据已是最新")
            print("=" * 60)
            return

        # 批量获取最新行情数据（市值+换手率+PE/PB+成交额+涨跌幅）
        print("\n正在批量获取最新行情数据...")
        update_codes = [code for code, _ in stocks_to_update]
        quote_data = self._fetch_quote_batch_tencent(update_codes)
        if not quote_data:
            print("  [WARN] 行情数据获取失败")

        def _update_stock_list(stock_tasks, quote_data, use_threads=False):
            tasks = [(code, days, quote_data, self.data_dir, skip_baostock) for code, days in stock_tasks]
            if use_threads:
                from concurrent.futures import ThreadPoolExecutor, as_completed
                workers = min(8, len(tasks))
                print(f"  使用多线程模式 (腾讯API, {workers}线程)...")
                results = []
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    future_map = {pool.submit(_update_one_stock_mp, *task): task[0] for task in tasks}
                    from tqdm import tqdm as _tqdm
                    for future in _tqdm(as_completed(future_map), total=len(future_map), desc="更新进度"):
                        results.append(future.result())
            else:
                print("  使用单进程模式进行更新 (baostock禁多进程)...")
                results = []
                from tqdm import tqdm as _tqdm
                for task in _tqdm(tasks, desc="更新进度"):
                    code, days, qd, d_dir, _skip = task
                    result = _update_one_stock_mp(code, days, qd, d_dir, skip_baostock=False)
                    results.append(result)
            success_list = []
            fail_list = []
            for code, success, added, error in results:
                if success:
                    success_list.append((code, added))
                else:
                    fail_list.append((code, error))
            return success_list, fail_list

        current_tasks = stocks_to_update
        max_retries = 3
        final_success = []
        final_fail = []
        for retry in range(max_retries + 1):
            if not current_tasks:
                break
            if retry == 0:
                mode_label = "腾讯API" if skip_baostock else "baostock"
                print(f"\n开始更新 {len(current_tasks)} 只股票... ({mode_label})")
            else:
                print(f"\n[RETRY] 第 {retry} 次重试，剩余 {len(current_tasks)} 只股票...")
            success_list, fail_list = _update_stock_list(current_tasks, quote_data, use_threads=skip_baostock)
            final_success.extend(success_list)
            current_tasks = [(code, days) for code, days in current_tasks if code in [f[0] for f in fail_list]]
            final_fail = fail_list
            print(f"  本次成功: {len(success_list)} 只, 失败: {len(fail_list)} 只")
            if not fail_list:
                break

        if final_fail:
            failed_stocks_file = self.full_data_dir / 'failed_update_stocks.json'
            try:
                with open(failed_stocks_file, 'w', encoding='utf-8') as f:
                    json.dump([code for code, _ in final_fail], f, ensure_ascii=False, indent=2)
                print(f"\n[WARN] 有 {len(final_fail)} 只股票更新失败，已写入 {failed_stocks_file}")
            except Exception as e:
                print(f"\n[WARN] 写入失败列表出错: {e}")

        updated = len(final_success)
        failed = len(final_fail)

        # 缓存写入（收盘后/盘前/非交易日）
        write_cache = False
        cache_date_to_write = None

        if not max_stocks and need_update > 0:
            # 收盘后（>=15:00）且今天是交易日：写入今天的日期
            if current_time >= trading_end and is_today_trading:
                cache_date_to_write = today_str
                write_cache = True
            # 盘前（<09:00）且今天是交易日：写入上一个交易日
            elif current_time < trading_start and is_today_trading:
                cache_date_to_write = last_trading_day_str
                write_cache = True
            # 今天非交易日：写入上一个交易日
            elif not is_today_trading:
                cache_date_to_write = last_trading_day_str
                write_cache = True

        if write_cache:
            update_cache['last_update_date'] = cache_date_to_write
            with open(update_cache_file, 'w', encoding='utf-8') as f:
                json.dump(update_cache, f)
            print(f"[OK]缓存已更新，最后更新日期: {cache_date_to_write}")
        else:
            reasons = []
            if max_stocks:
                reasons.append("强制更新模式")
            if not need_update:
                reasons.append("无数据更新")
            if is_trading_time:
                reasons.append("交易时段内（不写缓存）")
            if not is_today_trading:
                reasons.append("非交易日")
            if reasons:
                print(f"[INFO] 缓存未更新 ({', '.join(reasons)})")
            else:
                print(f"[INFO] 缓存未更新")

        print("=" * 60)
        print(f"完成! 更新成功: {updated}, 跳过: {skipped}, 失败: {failed}")

    # -------------------- 交易日判断 --------------------
    def _get_last_trading_day(self, date):
        import akshare as ak
        try:
            trade_cal = ak.tool_trade_date_hist_sina()
            trade_dates = pd.to_datetime(trade_cal['trade_date']).dt.date
            past_trade_dates = [d for d in trade_dates if d < date]
            if past_trade_dates:
                return past_trade_dates[-1]
            else:
                return self._fallback_last_trading_day(date)
        except Exception as e:
            print(f"  获取交易日历失败: {e}")
            return self._fallback_last_trading_day(date)

    def _is_trading_day(self, date):
        try:
            trade_cal = ak.tool_trade_date_hist_sina()
            trade_dates = pd.to_datetime(trade_cal['trade_date']).dt.date
            return date in set(trade_dates)
        except Exception:
            return date.weekday() < 5

    def _fallback_last_trading_day(self, date):
        from datetime import timedelta
        weekday = date.weekday()
        if weekday == 0:
            return date - timedelta(days=3)
        elif weekday == 6:
            return date - timedelta(days=2)
        else:
            return date - timedelta(days=1)
