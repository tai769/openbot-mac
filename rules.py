"""
规则引擎 — 复刻 openbot RobotRuleEntity
提供关键词匹配 → 回复模板的规则匹配功能。
在 AI 回复前先查规则，规则命中则直接回复（更快、更可控）。
"""

from __future__ import annotations
import logging
from typing import Optional

from db import DB

logger = logging.getLogger(__name__)


class RuleEngine:
    """
    规则引擎 — 复刻 openbot RobotRuleEntity
    支持关键词匹配和模式匹配。
    """

    def __init__(self, db: DB):
        self.db = db
        self._rules_cache: list[dict] = []
        self._cache_loaded = False

    async def load_cache(self):
        """加载规则到内存缓存"""
        cursor = await self.db._conn.execute(
            "SELECT Question, Answer, PatternsJson FROM RobotRuleEntity WHERE IsDeleted = 0"
        )
        rows = await cursor.fetchall()
        self._rules_cache = [
            {
                "question": r[0],
                "answer": r[1],
                "patterns": r[2],
            }
            for r in rows
        ]
        self._cache_loaded = True
        logger.info(f"已加载 {len(self._rules_cache)} 条规则")

    async def add_rule(self, question: str, answer: str, patterns: list[str] = None):
        """添加规则"""
        await self.db.save_rule(question, answer, patterns)
        # 更新缓存
        self._rules_cache.append({
            "question": question,
            "answer": answer,
            "patterns": str(patterns or []),
        })
        logger.info(f"添加规则: {question} -> {answer[:30]}...")

    async def match(self, text: str) -> Optional[str]:
        """
        匹配规则 — 复刻 openbot 的规则匹配逻辑
        返回匹配的答案，或 None。
        """
        if not self._cache_loaded:
            await self.load_cache()

        text_lower = text.lower().strip()

        # 1. 精确匹配
        for rule in self._rules_cache:
            question = rule["question"]
            if question and question.lower().strip() == text_lower:
                logger.info(f"规则精确匹配: {question}")
                return rule["answer"]

        # 2. 包含匹配
        for rule in self._rules_cache:
            question = rule["question"]
            if question and question.lower() in text_lower:
                logger.info(f"规则包含匹配: {question}")
                return rule["answer"]

        # 3. 模式匹配
        import json
        for rule in self._rules_cache:
            try:
                patterns = json.loads(rule["patterns"]) if rule["patterns"] else []
            except (json.JSONDecodeError, TypeError):
                patterns = []
            for pattern in patterns:
                if pattern and pattern.lower() in text_lower:
                    logger.info(f"规则模式匹配: {pattern}")
                    return rule["answer"]

        return None

    async def get_rules_count(self) -> int:
        """获取规则数量"""
        if not self._cache_loaded:
            await self.load_cache()
        return len(self._rules_cache)
