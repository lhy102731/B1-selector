"""
钉钉群通知模块（纯文本版）
"""
import time
import hmac
import hashlib
import base64
import urllib.parse
import requests
from datetime import datetime


class RateLimiter:
    """限流器 - 控制每分钟发送数量"""

    def __init__(self, max_per_minute=20, min_interval=2.0):
        self.max_per_minute = max_per_minute
        self.min_interval = min_interval
        self.send_times = []
        self._lock_time = 0

    def acquire(self):
        now = time.time()
        self.send_times = [t for t in self.send_times if now - t < 60]
        if now < self._lock_time:
            wait = self._lock_time - now
            time.sleep(wait)
            now = time.time()
        if len(self.send_times) >= self.max_per_minute:
            oldest = self.send_times[0]
            wait = 60 - (now - oldest) + 0.1
            if wait > 0:
                print(f"    ⏱️ 限流: 等待{wait:.1f}秒...")
                time.sleep(wait)
                now = time.time()
                self.send_times = [t for t in self.send_times if now - t < 60]
        if self.send_times:
            last_send = self.send_times[-1]
            elapsed = now - last_send
            if elapsed < self.min_interval:
                wait = self.min_interval - elapsed
                time.sleep(wait)
                now = time.time()
        self.send_times.append(now)
        return now

    def on_rate_limit_error(self, retry_count=0):
        backoff = min(2 ** retry_count, 30)
        self._lock_time = time.time() + backoff
        print(f"    ⏱️ 遇到限速，退避等待{backoff}秒...")
        time.sleep(backoff)


class DingTalkNotifier:
    """钉钉通知器"""

    def __init__(self, webhook_url=None, secret=None):
        self.webhook_url = webhook_url
        self.secret = secret
        self._rate_limiter = RateLimiter(max_per_minute=20, min_interval=2.0)

    def _generate_sign(self):
        if not self.secret:
            return "", ""
        timestamp = str(round(time.time() * 1000))
        secret_enc = self.secret.encode('utf-8')
        string_to_sign = f'{timestamp}\n{self.secret}'
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return timestamp, sign

    def _send_single_markdown(self, title, content, part_info="", max_retries=3):
        if not self.webhook_url:
            return False
        for attempt in range(max_retries + 1):
            self._rate_limiter.acquire()
            timestamp, sign = self._generate_sign()
            if self.secret:
                webhook_url = f"{self.webhook_url}&timestamp={timestamp}&sign={sign}"
            else:
                webhook_url = self.webhook_url
            text = content
            if part_info:
                text = f"> {part_info}\n\n{text}"
            data = {
                "msgtype": "markdown",
                "markdown": {"title": title, "text": text}
            }
            try:
                response = requests.post(webhook_url, json=data, headers={'Content-Type': 'application/json'}, timeout=10)
                if response.status_code == 200:
                    result = response.json()
                    if result.get('errcode') == 0:
                        return True
                    elif result.get('errcode') == 660026:
                        if attempt < max_retries:
                            print(f"    ⚠️ 触发限速，第{attempt+1}次重试...")
                            self._rate_limiter.on_rate_limit_error(attempt)
                            continue
                        else:
                            return False
                    else:
                        return False
                else:
                    if attempt < max_retries:
                        time.sleep(2 ** attempt)
                        continue
                    return False
            except Exception as e:
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                return False
        return False

    def send_markdown(self, title, content):
        MAX_SIZE = 18000
        content_bytes = content.encode('utf-8')
        if len(content_bytes) <= MAX_SIZE:
            return self._send_single_markdown(title, content)
        print(f"消息大小 {len(content_bytes)} 字节，分段发送...")
        lines = content.split('\n')
        parts = []
        current_part = []
        current_size = 0
        for line in lines:
            line_size = len(line.encode('utf-8')) + 1
            if line_size > MAX_SIZE:
                # 超长行截断
                chunk = line[:MAX_SIZE-100] + "..."
                if current_part:
                    parts.append('\n'.join(current_part))
                    current_part = []
                    current_size = 0
                parts.append(chunk)
                continue
            if current_size + line_size > MAX_SIZE and current_part:
                parts.append('\n'.join(current_part))
                current_part = [line]
                current_size = line_size
            else:
                current_part.append(line)
                current_size += line_size
        if current_part:
            parts.append('\n'.join(current_part))
        success = 0
        for i, part in enumerate(parts, 1):
            if self._send_single_markdown(title, part, f"📨 ({i}/{len(parts)})"):
                success += 1
            time.sleep(1)
        return success == len(parts)

    def send_simple_b1_results(self, results: list, min_similarity: float, b3_results: list = None):
        if not results and not b3_results:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        lines = []
        has_b1 = bool(results)

        if has_b1:
            lines = [
                "## 统一B1选股结果（按相似度排序）",
                "",
                f"时间: {now}",
                f"筛选数量: {len(results)} 只 | 相似度阈值: {min_similarity}%",
                "━" * 30,
                "",
            ]
            for i, r in enumerate(results[:15], 1):
                rank = r.get('max_high_vol_rank', 0)
                rank_tag = " [-3量价背离]" if rank == 2 else ""
                hist = r.get('hist_bonus', 0)
                hist_tag = f" [+{hist}历史]" if hist > 0 else (f" [{hist}历史]" if hist < 0 else "")
                lines.append(f"{i}. **{r['code']}** {r['name']}  **相似度: {r['b1_score']:.1f}%**{rank_tag}{hist_tag}")
                washout_tag = " [击穿对手盘]" if r.get('is_washout') else ""
                lines.append(f"   类型: {washout_tag or '标准B1'} | 价格: {r['close']} | J值: {r['J']:.1f}")
                lines.append(f"   建仓涨幅: {r.get('build_gain', 0):.1f}% | 换手累加: {r.get('surge_turnover', 0):.1f}%")
                if r.get('matched_case'):
                    lines.append(f"   匹配案例: {r['matched_case']} ({r.get('matched_date', '')})")
                if r.get('breakdown'):
                    bd = r['breakdown']
                    lines.append(f"   分项: 趋势{bd.get('trend',0):.0f}% KDJ{bd.get('divergence',0):.0f}% 量能{bd.get('volume',0):.0f}% 形态{bd.get('price_shape',0):.0f}%")
                lines.append("")
            lines.append("---")
            lines.append("**策略条件**: 白线>黄线 | J<30 | 均线多头 | 缩量 | 无S1 | 建仓涨幅≤60% | 换手累加≤80%")
            lines.append("**特殊通道**: 击穿对手盘（缩量破黄线后快速收回）")

        # ---- B3选股结果 ----
        if b3_results:
            if has_b1:
                lines.append("")
                lines.append("---")
                lines.append("")
            lines.append("## B3涨停接力选股 (无砖+无择时)")
            lines.append("")
            lines.append(f"时间: {now}")
            lines.append(f"筛选数量: {len(b3_results)} 只")
            lines.append(f"公式: 昨涨≥9% + 今涨2-4% + 收阳 + 影线≤2% + 缩量0.6-0.9 + 双线 + 市值")
            lines.append("━" * 30)
            lines.append("")

            if b3_results:
                for i, r in enumerate(b3_results[:15], 1):
                    lines.append(f"{i}. **{r['code']}** {r['name']}  价格: {r['close']}")
                    lines.append(f"   昨涨: {r['ret_yesterday_pct']:.1f}% | 今涨: {r['ret_today_pct']:.1f}% | "
                                 f"量比: {r['vol_ratio']:.2f} | J值: {r['J']:.1f}")
                    lines.append("")
            else:
                lines.append("今日无B3信号")
                lines.append("")

            lines.append("---")
            lines.append("**B3条件**: 昨涨≥9% | 今涨2-4% | 收阳 | 跳空高开<1% | 上下影线≤2% | 缩量0.6-0.9 | 双线达标 | 市值>50亿")

        self.send_markdown("选股结果", "\n".join(lines))