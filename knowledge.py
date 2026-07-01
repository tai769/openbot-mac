"""
商品知识库 — 复刻 openbot GoodsKnowledgeEntity
管理商品知识条目，在 AI 回复时注入相关上下文。
"""

from __future__ import annotations
import logging
from typing import Optional

from db import DB

logger = logging.getLogger(__name__)


class KnowledgeBase:
    """
    商品知识库 — 复刻 openbot 的商品知识管理
    提供知识查询和上下文注入功能。
    """

    def __init__(self, db: DB):
        self.db = db

    async def add(self, num_iid: int, title: str, content: str):
        """添加商品知识条目"""
        await self.db.save_knowledge(num_iid, title, content)
        logger.info(f"添加商品知识: {title}")

    async def search(self, keyword: str) -> list[dict]:
        """搜索商品知识"""
        return await self.db.search_knowledge(keyword)

    async def get_all(self) -> list[dict]:
        """获取所有商品知识"""
        return await self.db.get_all_knowledge()

    async def build_context(self, message_text: str, item_id: str = "") -> str:
        """
        根据消息内容构建知识上下文 — 注入到 system prompt 中
        复刻 openbot 在 AI 回复前查找相关商品知识的逻辑。
        """
        context_parts = []

        # 1. 如果有商品 ID，直接查找
        if item_id:
            items = await self.db.search_knowledge(item_id)
            for item in items:
                context_parts.append(
                    f"商品: {item['title']}\n信息: {item['content']}"
                )

        # 2. 从消息文本中提取关键词搜索
        keywords = self._extract_keywords(message_text)
        for kw in keywords[:3]:  # 最多取 3 个关键词
            items = await self.db.search_knowledge(kw)
            for item in items:
                entry = f"商品: {item['title']}\n信息: {item['content']}"
                if entry not in context_parts:
                    context_parts.append(entry)

        if not context_parts:
            return ""

        return "\n---\n".join(context_parts)

    @staticmethod
    def _extract_keywords(text: str) -> list[str]:
        """从消息中提取关键词（简单实现）"""
        # 移除标点和常见停用词
        import re
        text = re.sub(r'[，。！？、；：“”‘’（）\[\]【】\s]+', ' ', text)
        words = text.split()

        # 过滤太短的词
        keywords = [w.strip() for w in words if len(w.strip()) >= 2]
        return keywords[:10]
