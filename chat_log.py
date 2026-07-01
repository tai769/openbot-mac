"""
聊天记录持久化 — 复刻 openbot ChatlogEntity
记录所有对话到 SQLite，支持查询历史。
"""

from __future__ import annotations
import logging
from typing import Optional

from db import DB

logger = logging.getLogger(__name__)


class ChatLogger:
    """
    聊天记录器 — 复刻 openbot 的聊天记录管理
    持久化所有对话，支持按卖家/买家查询。
    """

    def __init__(self, db: DB):
        self.db = db

    async def log(self, seller: str, from_nick: str, to_nick: str,
                  content: str, item_id: str = "", image_url: str = ""):
        """记录一条聊天消息"""
        await self.db.save_chat_log(seller, from_nick, to_nick, content, item_id, image_url)

    async def get_history(self, seller: str, buyer: str, limit: int = 20) -> list[dict]:
        """获取卖家与买家的聊天历史"""
        return await self.db.get_chat_history(seller, buyer, limit)

    async def build_history_context(self, seller: str, buyer: str, limit: int = 10) -> str:
        """
        构建聊天历史上下文 — 用于 AI 回复时参考历史对话
        """
        history = await self.get_history(seller, buyer, limit)
        if not history:
            return ""

        lines = []
        for msg in history:
            role = "买家" if msg["from"] != seller else "卖家"
            lines.append(f"{role}: {msg['content']}")

        return "\n".join(lines)
