"""
OpenAI-compatible AI 客户端 — 复刻 openbot MyOpenAI.cs
使用 /chat/completions 协议，支持 OpenAI 兼容 base_url。
"""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Optional
import httpx

from config import config

logger = logging.getLogger(__name__)

CUSTOMER_SERVICE_GUARDRAIL = (
    "\n\n强制客服话术要求："
    "你就是店铺客服本人，不要说自己是机器人、AI、自动回复或助手；"
    "不要建议买家联系人工客服、转人工或找客服。"
    "需要核实订单、售后、预约、商品细节时，直接以客服身份说“我这边帮您核实”，"
    "并请买家提供订单号、商品链接或截图。"
)


class AIClient:
    """
    OpenAI-compatible AI 客户端 — 复刻 MyOpenAI
    管理 per-buyer 对话历史，调用 Claude API 生成回复。
    """

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None
        # per-buyer 对话历史 — 复刻 MyOpenAI._chatHistory
        # key: "{seller}#{buyer}", value: list of messages
        self._conversations: dict[str, list[dict]] = {}
        # 上次活跃时间，用于超时清理
        self._last_active: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def init(self):
        """初始化 OpenAI-compatible 客户端"""
        if not config.robot.api_key:
            logger.error("未配置 API Key，请在设置中配置")
            return

        self._client = httpx.AsyncClient(
            base_url=config.robot.base_url.rstrip("/"),
            headers={
                "Authorization": f"Bearer {config.robot.api_key}",
                "Content-Type": "application/json",
            },
            timeout=60,
            trust_env=False,
        )
        logger.info(
            f"AI 客户端已初始化，OpenAI-compatible，模型: {config.robot.model_name}, "
            f"base_url: {config.robot.base_url}"
        )

    async def get_answer(self, seller: str, buyer: str, question: str,
                         knowledge_context: str = "") -> str:
        """
        获取 AI 回复 — 复刻 MyOpenAI.GetAnswer
        维护 per-buyer 对话历史，调用 Claude API。
        """
        if not self._client:
            logger.error("AI 客户端未初始化，跳过自动回复")
            return ""

        conversation_key = f"{seller}#{buyer}"
        now = time.time()

        async with self._lock:
            # 检查会话是否超时 — 复刻 openbot 的会话超时清理
            if conversation_key in self._last_active:
                elapsed = now - self._last_active[conversation_key]
                if elapsed > config.robot.session_timeout:
                    logger.info(f"会话超时，清理: {conversation_key}")
                    self._conversations.pop(conversation_key, None)
                    self._last_active.pop(conversation_key, None)

            # 获取或创建对话历史
            if conversation_key not in self._conversations:
                self._conversations[conversation_key] = []

            history = self._conversations[conversation_key]
            history.append({"role": "user", "content": question})
            self._last_active[conversation_key] = now

        # 构建 system prompt — 复刻 openbot 的 system prompt 机制
        system_prompt = config.robot.system_prompt + CUSTOMER_SERVICE_GUARDRAIL
        if knowledge_context:
            system_prompt += f"\n\n相关商品信息:\n{knowledge_context}"

        # 调用 OpenAI-compatible Chat Completions API
        try:
            messages = [{"role": "system", "content": system_prompt}, *history]
            response = await self._client.post(
                "/chat/completions",
                json={
                    "model": config.robot.model_name,
                    "messages": messages,
                    "max_tokens": 1024,
                },
            )

            if response.status_code == 401:
                logger.error("API Key 无效")
                return ""
            if response.status_code == 429:
                logger.warning("API 调用频率限制")
                return "这边稍后再帮您确认一下。"
            response.raise_for_status()

            data = response.json()
            choices = data.get("choices") or []
            answer = ""
            if choices:
                message = choices[0].get("message") or {}
                answer = message.get("content") or ""

            # 将 AI 回复加入历史
            async with self._lock:
                if conversation_key in self._conversations:
                    self._conversations[conversation_key].append({
                        "role": "assistant",
                        "content": answer
                    })

            logger.info(f"AI 回复 [{buyer}]: {answer[:50]}...")
            return answer

        except httpx.HTTPStatusError as e:
            logger.error(f"AI 调用失败: HTTP {e.response.status_code} {e.response.text[:200]}")
            return ""
        except Exception as e:
            logger.error(f"AI 调用失败: {e}")
            # 回滚 user message
            async with self._lock:
                if conversation_key in self._conversations and self._conversations[conversation_key]:
                    self._conversations[conversation_key].pop()
            return ""

    async def clear_conversation(self, seller: str, buyer: str):
        """清除指定买家的对话历史"""
        key = f"{seller}#{buyer}"
        async with self._lock:
            self._conversations.pop(key, None)
            self._last_active.pop(key, None)

    async def cleanup_expired_sessions(self):
        """定期清理过期会话 — 复刻 openbot 的会话清理机制"""
        now = time.time()
        expired = []
        async with self._lock:
            for key, last_time in self._last_active.items():
                if now - last_time > config.robot.session_timeout:
                    expired.append(key)
            for key in expired:
                self._conversations.pop(key, None)
                self._last_active.pop(key, None)
        if expired:
            logger.info(f"清理了 {len(expired)} 个过期会话")

    def get_active_session_count(self) -> int:
        """获取活跃会话数"""
        return len(self._conversations)

    async def reload_client(self):
        """重新加载客户端（配置变更后调用）"""
        if self._client:
            await self._client.aclose()
        self._client = None
        await self.init()


# 全局单例
ai_client = AIClient()
