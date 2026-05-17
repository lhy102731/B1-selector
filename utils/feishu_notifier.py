"""
飞书（Lark）消息通知模块
使用 app_id + app_secret 获取 tenant_access_token，通过 IM API 发送消息
"""
import time
import json
import requests
from datetime import datetime


class FeishuNotifier:
    """飞书通知器"""

    def __init__(self, app_id=None, app_secret=None, chat_id=None):
        self.app_id = app_id
        self.app_secret = app_secret
        self.chat_id = chat_id
        self._token = None
        self._token_expire = 0

    def _get_token(self):
        """获取 tenant_access_token，自动缓存到过期前5分钟"""
        if self._token and time.time() < self._token_expire:
            return self._token

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        resp = requests.post(url, json={
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }, timeout=10)
        data = resp.json()
        if data.get("code") == 0:
            self._token = data["tenant_access_token"]
            self._token_expire = time.time() + data.get("expire", 7200) - 300
            return self._token
        else:
            print(f"  Feishu token error: {data}")
            return None

    def send_text(self, content, title=None):
        """发送文本消息到群聊"""
        if title:
            content = f"{title}\n{content}"
        return self._send_single_text(content)

    def send_markdown(self, title, content):
        """用飞书 interactive 卡片 + markdown 元素发送（支持表格）"""
        if not self.app_id or not self.chat_id:
            print("  Feishu: missing config")
            return False

        token = self._get_token()
        if not token:
            return False

        # 飞书 markdown 元素最长 20000 字符
        MAX_LEN = 18000
        if len(content) <= MAX_LEN:
            return self._send_card(title, content)

        # 分段发送
        lines = content.split('\n')
        parts = []
        current = ''
        for line in lines:
            if len(current) + len(line) + 1 > MAX_LEN:
                parts.append(current)
                current = line
            else:
                current += '\n' + line if current else line
        if current:
            parts.append(current)

        success = True
        for i, part in enumerate(parts):
            hdr = f"{title} [{i+1}/{len(parts)}]"
            if not self._send_card(hdr, part):
                success = False
            time.sleep(0.5)
        return success

    def _send_card(self, title, md_content):
        """发送 interactive 卡片"""
        token = self._get_token()
        if not token:
            return False

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"content": title, "tag": "plain_text"}
            },
            "elements": [
                {"tag": "markdown", "content": md_content}
            ]
        }
        body = {
            "receive_id": self.chat_id,
            "msg_type": "interactive",
            "content": json.dumps(card)
        }
        url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        resp = requests.post(url, data=json.dumps(body), headers=headers, timeout=10)
        return resp.json().get("code") == 0

    def _send_single_text(self, text):
        """发送单条文本"""
        token = self._get_token()
        if not token:
            return False

        content_str = json.dumps({"text": text})
        body = {
            "receive_id": self.chat_id,
            "msg_type": "text",
            "content": content_str
        }
        url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        resp = requests.post(url, data=json.dumps(body), headers=headers, timeout=10)
        return resp.json().get("code") == 0


if __name__ == '__main__':
    # Test
    import yaml
    from pathlib import Path
    with open('config/config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    feishu_cfg = config.get('feishu', {})
    notifier = FeishuNotifier(
        app_id=feishu_cfg.get('app_id'),
        app_secret=feishu_cfg.get('app_secret'),
        chat_id=feishu_cfg.get('chat_id')
    )
    notifier.send_text("飞书通知模块测试成功!")
