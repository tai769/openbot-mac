"""
卖家会话管理 — 复刻 openbot QN.cs
每个卖家一个 session，管理消息接收、AI 调用、自动回复。
"""

from __future__ import annotations
import asyncio
import json
import logging
import time
from typing import Optional

from message import ChatResponse, QNChatMessage
from ws_server import CDPSession, WebSocketServer
from ai_client import ai_client
from knowledge import KnowledgeBase
from rules import RuleEngine
from chat_log import ChatLogger
from config import config

logger = logging.getLogger(__name__)


class SellerSession:
    """
    卖家会话 — 复刻 openbot QN
    绑定一个 CDPSession，处理该卖家的所有消息。
    """

    def __init__(self, cdp: CDPSession, server: WebSocketServer,
                 knowledge: KnowledgeBase, rules: RuleEngine, chat_logger: ChatLogger):
        self.cdp = cdp
        self.server = server
        self.knowledge = knowledge
        self.rules = rules
        self.chat_logger = chat_logger
        self.seller_nick: str = cdp.seller_nick
        self._active_conversations: dict[str, str] = {}  # buyer_nick -> last_question
        self._reply_lock = asyncio.Lock()

    def _select_send_cdp(self) -> CDPSession:
        """Use the real chat WebView for sending; bridge/summary pages cannot call imsdk."""
        for cdp in self.server.sessions.values():
            if cdp.seller_nick == self.seller_nick and cdp.is_chat_session:
                return cdp
        for cdp in self.server.sessions.values():
            if cdp.is_chat_session:
                return cdp
        return self.cdp

    async def handle_native_text_message(self, buyer_nick: str, buyer_uid: str, message_text: str, msg_key: str = ""):
        """处理 macOS 原生旺旺事件里直接带正文的买家消息。"""
        buyer_nick = buyer_nick or buyer_uid
        seller_nick = self.seller_nick or self.cdp.seller_nick or "当前卖家"
        message_text = (message_text or "").strip()
        if not buyer_nick or not message_text:
            return

        if buyer_nick == seller_nick or buyer_nick in seller_nick or seller_nick in buyer_nick:
            return

        logger.info(f"收到买家原生消息 [{buyer_nick}]: {message_text[:50]}")
        self._active_conversations[buyer_nick] = message_text
        await self.chat_logger.log(seller_nick, buyer_nick, seller_nick, message_text)

        if not config.robot.auto_reply:
            logger.debug("自动回复已关闭")
            return

        rule_answer = await self.rules.match(message_text)
        if rule_answer:
            logger.info(f"规则命中 [{buyer_nick}]: {rule_answer[:30]}...")
            await self._send_reply(seller_nick, buyer_nick, rule_answer, is_auto=True)
            return

        knowledge_ctx = await self.knowledge.build_context(message_text, "")
        answer = await ai_client.get_answer(seller_nick, buyer_nick, message_text, knowledge_ctx)
        if answer:
            await asyncio.sleep(config.robot.reply_delay)
            await self._send_reply(seller_nick, buyer_nick, answer, is_auto=True)

    async def handle_new_message(self, *args):
        """
        处理新消息 — 复刻 QN.Cdp_EvRecieveNewMessage
        核心逻辑：过滤 → 规则匹配 → AI 回复 → 自动发送
        """
        response = args[-1] if args else ""
        try:
            # 解析消息 — 复刻 QN 的 ChatResponse 解析
            chat_resp = ChatResponse.from_json(response)
        except Exception as e:
            logger.error(f"解析消息失败: {e}")
            return

        for msg in chat_resp.result:
            await self._process_single_message(msg)

    async def handle_shop_robot_receive(self, *args):
        """
        处理千牛机器人新消息通知 — 复刻 QN.Cdp_EvShopRobotReceriveNewMessage。
        原版在自动回复开启时先打开买家聊天，随后由 receiveNewMsg 进入真正回复链路。
        """
        response = args[-1] if args else ""
        try:
            payload = json.loads(response) if isinstance(response, str) else response
            conversation = payload.get("conversation", {}) if isinstance(payload, dict) else {}
            login_id = payload.get("loginID", {}) if isinstance(payload, dict) else {}
        except Exception as e:
            logger.error(f"解析机器人消息通知失败: {e}")
            return

        buyer_nick = conversation.get("nick", "") if isinstance(conversation, dict) else ""
        seller_nick = login_id.get("nick", "") if isinstance(login_id, dict) else self.seller_nick

        if seller_nick:
            self.seller_nick = seller_nick
        if not buyer_nick:
            return

        logger.info(f"机器人新消息通知 [{buyer_nick}]")

        if config.robot.auto_reply:
            cdp = self._select_send_cdp()
            await cdp.open_chat(buyer_nick)

    async def handle_conversation_change(self, *args):
        """处理会话切换 — 复刻 QN.Cdp_EvBuyerSwitched。"""
        response = args[-1] if args else ""
        try:
            payload = json.loads(response) if isinstance(response, str) else response
            conversation = payload.get("conversation", {}) if isinstance(payload, dict) else {}
            login_id = payload.get("loginID", {}) if isinstance(payload, dict) else {}
        except Exception as e:
            logger.error(f"解析会话切换失败: {e}")
            return

        buyer_nick = conversation.get("nick", "") if isinstance(conversation, dict) else ""
        seller_nick = login_id.get("nick", "") if isinstance(login_id, dict) else ""
        if seller_nick:
            self.seller_nick = seller_nick
        if buyer_nick:
            logger.info(f"当前会话切换: seller={self.seller_nick}, buyer={buyer_nick}")

    async def _process_single_message(self, msg: QNChatMessage):
        """处理单条消息 — 复刻 QN 的消息处理逻辑"""
        # 过滤：只处理买家发给卖家的消息 — 复刻 QN 的过滤条件
        if not msg.is_buyer_send:
            return

        buyer_nick = msg.buyer_nick
        seller_nick = msg.seller_nick or self.seller_nick
        message_text = msg.message_text

        if not message_text or not buyer_nick:
            return

        logger.info(f"收到买家消息 [{buyer_nick}]: {message_text[:50]}")

        # 记录对话 — 复刻 Desk.Inst.AddConversation
        self._active_conversations[buyer_nick] = message_text

        # 保存聊天记录
        await self.chat_logger.log(seller_nick, buyer_nick, seller_nick, message_text)

        # 检查是否开启自动回复
        if not config.robot.auto_reply:
            logger.debug("自动回复已关闭")
            return

        # 1. 先查规则 — 复刻 openbot 的规则优先匹配
        rule_answer = await self.rules.match(message_text)
        if rule_answer:
            logger.info(f"规则命中 [{buyer_nick}]: {rule_answer[:30]}...")
            await self._send_reply(seller_nick, buyer_nick, rule_answer, is_auto=True)
            return

        # 2. 构建知识上下文
        knowledge_ctx = await self.knowledge.build_context(
            message_text, msg.original_data.item_id
        )

        # 3. 调用 AI 生成回复
        answer = await ai_client.get_answer(
            seller_nick, buyer_nick, message_text, knowledge_ctx
        )

        if answer:
            # 延迟发送 — 复刻 openbot 的 2 秒延迟
            await asyncio.sleep(config.robot.reply_delay)
            await self._send_reply(seller_nick, buyer_nick, answer, is_auto=True)

    async def _send_reply(self, seller: str, buyer: str, text: str, is_auto: bool = False):
        """
        发送回复 — 复刻 QN.SendTextAsync + QNRpa.SendTextAsync
        通过 imsdk API 发送消息。
        """
        async with self._reply_lock:
            try:
                cdp = self._select_send_cdp()
                if cdp is not self.cdp:
                    logger.info(
                        "切换到聊天页发送: seller=%s, href=%s",
                        seller,
                        cdp.href,
                    )
                    self.cdp = cdp
                logger.info(
                    "准备发送回复: buyer=%s, href=%s, chat=%s, imsdk=%s, vs=%s",
                    buyer,
                    cdp.href,
                    cdp.is_chat_session,
                    cdp.has_imsdk,
                    cdp.has_vs,
                )

                # 打开买家聊天窗口
                await cdp.open_chat(buyer)
                await asyncio.sleep(0.5)  # 等待窗口切换

                # 插入文本到输入框
                success = await cdp.insert_text_to_inputbox(buyer, text)
                if success:
                    await asyncio.sleep(2.5)
                    logger.info(f"回复文本已准备，准备点击发送 [{buyer}]")
                    send_confirmation = cdp.create_send_confirmation(text)
                    clicked = await cdp.click_send_button()
                    if not clicked:
                        logger.warning(f"点击发送按钮失败，尝试回车兜底 [{buyer}]")
                        await cdp.press_enter()
                    else:
                        logger.info(f"已点击发送按钮 [{buyer}]")
                    success = await cdp.wait_for_send_confirmation(send_confirmation)

                if success:
                    logger.info(f"已发送回复 [{buyer}]: {text[:50]}...")
                    # 记录发送的回复
                    await self.chat_logger.log(seller, seller, buyer, text)
                else:
                    logger.warning(f"发送回复失败或未收到发送确认 [{buyer}]")

            except Exception as e:
                logger.error(f"发送回复异常: {e}")

    def get_active_buyers(self) -> list[str]:
        """获取当前活跃买家列表"""
        return list(self._active_conversations.keys())

    def get_last_question(self, buyer: str) -> str:
        """获取买家最后一条消息"""
        return self._active_conversations.get(buyer, "")


class SessionManager:
    """
    会话管理器 — 复刻 openbot 的 QN.QNSet
    管理所有卖家的会话。
    """

    def __init__(self, server: WebSocketServer, knowledge: KnowledgeBase,
                 rules: RuleEngine, chat_logger: ChatLogger):
        self.server = server
        self.knowledge = knowledge
        self.rules = rules
        self.chat_logger = chat_logger
        self.sessions: dict[str, SellerSession] = {}  # seller_nick -> SellerSession
        self._current_seller: Optional[str] = None
        self._native_msg_seen: set[str] = set()

    def _bind_session_callbacks(self, cdp: CDPSession, session: SellerSession):
        """把 CDP 事件路由到卖家 session。"""
        cdp.on_receive_new_msg = session.handle_new_message
        cdp.on_shop_robot_receive = session.handle_shop_robot_receive
        cdp.on_conversation_change = session.handle_conversation_change
        cdp.on_chat_dlg_active = session.handle_conversation_change

    async def _ensure_session(self, cdp: CDPSession, seller_nick: str) -> Optional[SellerSession]:
        """确保卖家 session 存在；用于启动识别和事件兜底识别。"""
        if not seller_nick:
            return None

        cdp.seller_nick = seller_nick
        if seller_nick in self.sessions:
            session = self.sessions[seller_nick]
            if cdp.is_chat_session or not session.cdp.is_chat_session:
                session.cdp = cdp
                self._bind_session_callbacks(cdp, session)
                if cdp.is_chat_session:
                    self.server.sellers[seller_nick] = cdp
                logger.debug(
                    "卖家会话绑定页面: seller=%s, chat=%s, href=%s",
                    seller_nick,
                    cdp.is_chat_session,
                    cdp.href,
                )
            else:
                logger.debug(
                    "忽略非聊天页面覆盖卖家会话: seller=%s, href=%s",
                    seller_nick,
                    cdp.href,
                )
            return session

        session = SellerSession(
            cdp=cdp,
            server=self.server,
            knowledge=self.knowledge,
            rules=self.rules,
            chat_logger=self.chat_logger,
        )
        self.sessions[seller_nick] = session
        self._bind_session_callbacks(cdp, session)
        if cdp.is_chat_session:
            self.server.sellers[seller_nick] = cdp

        if not self._current_seller:
            self._current_seller = seller_nick

        logger.info(f"卖家会话已创建: {seller_nick}")
        return session

    @staticmethod
    def _decode_event_payload(response: str) -> dict:
        try:
            payload = json.loads(response) if isinstance(response, str) else response
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _decode_nested_json(value):
        """Decode native callback payloads that often arrive as JSON strings."""
        current = value
        for _ in range(2):
            if not isinstance(current, str):
                break
            try:
                current = json.loads(current)
            except Exception:
                break
        return current

    @classmethod
    def _seller_from_event_payload(cls, response: str) -> str:
        payload = cls._decode_event_payload(response)
        login_id = payload.get("loginID", {})
        return login_id.get("nick", "") if isinstance(login_id, dict) else ""

    async def on_seller_connected(self, cdp: CDPSession):
        """卖家连接 — 复刻 MyWebSocketServer 的卖家初始化"""
        nick = cdp.seller_nick
        await self._ensure_session(cdp, nick)

    async def on_bridge_ready(self, cdp: CDPSession, response: str):
        """聊天 WebView bridge 就绪后，从 loginID 兜底绑定卖家 session。"""
        payload = self._decode_event_payload(response)
        login_id = payload.get("loginID", {})
        nick = login_id.get("nick", "") if isinstance(login_id, dict) else ""
        if nick:
            await self._ensure_session(cdp, nick)

    async def on_seller_disconnected(self, cdp: CDPSession):
        """卖家断开"""
        nick = cdp.seller_nick
        if nick and nick in self.sessions:
            if self.sessions[nick].cdp is not cdp:
                return
            del self.sessions[nick]
            logger.info(f"卖家会话已移除: {nick}")

            # 如果当前卖家断开，切换到其他卖家
            if self._current_seller == nick:
                self._current_seller = next(iter(self.sessions), None)

    async def on_message_received(self, cdp: CDPSession, response: str):
        """消息接收 — 转发给对应的卖家 session"""
        nick = cdp.seller_nick
        if not nick:
            try:
                chat_resp = ChatResponse.from_json(response)
                if chat_resp.result:
                    nick = chat_resp.result[0].seller_nick
            except Exception:
                nick = ""

        session = await self._ensure_session(cdp, nick)
        if session:
            await session.handle_new_message(response)

    async def on_shop_robot_receive(self, cdp: CDPSession, response: str):
        """机器人新消息通知 — 支持从事件里兜底识别卖家。"""
        nick = cdp.seller_nick or self._seller_from_event_payload(response)
        session = await self._ensure_session(cdp, nick)
        if session:
            await session.handle_shop_robot_receive(response)

    async def on_conversation_change(self, cdp: CDPSession, response: str):
        """会话切换 — 支持从事件里兜底识别卖家。"""
        nick = cdp.seller_nick or self._seller_from_event_payload(response)
        session = await self._ensure_session(cdp, nick)
        if session:
            await session.handle_conversation_change(response)

    async def on_native_event(self, cdp: CDPSession, response: str):
        """macOS workbench/QN 原生事件，用于定位新版聊天消息链路。"""
        payload = self._decode_event_payload(response)
        event_name = str(payload.get("name", ""))
        data = self._decode_nested_json(payload.get("data", ""))

        if "messageText" in payload:
            source = str(payload.get("source", ""))
            text = str(payload.get("messageText") or "").strip()
            nick = str(payload.get("buyerNick") or "")
            uid = str(payload.get("buyerUid") or "")
            if text:
                logger.info(f"页面消息候选 [{source}] [{nick}/{uid}]: {text[:80]}")
            if source != "dom:messageBubble" or not text:
                return

            dedupe_key = f"pageMessageCandidate:{uid}:{nick}:{text}"
            if dedupe_key in self._native_msg_seen:
                return
            self._native_msg_seen.add(dedupe_key)
            if len(self._native_msg_seen) > 500:
                self._native_msg_seen = set(list(self._native_msg_seen)[-250:])

            seller_nick = cdp.seller_nick or self._current_seller or "当前卖家"
            session = await self._ensure_session(cdp, seller_nick)
            if session:
                await session.handle_native_text_message(nick, uid, text, dedupe_key)
            return

        if event_name == "wangwang.recvU2UMsgBatch":
            items = data if isinstance(data, list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                text = str(item.get("message") or "")
                nick = str(item.get("nick") or item.get("fromuid") or "")
                uid = str(item.get("securityUID") or item.get("uid") or "")
                msg_type = item.get("type", "")
                if text:
                    logger.info(f"原生旺旺消息 [{nick}/{uid}]: {text[:80]}")
                    dedupe_key = f"{event_name}:{uid}:{item.get('time', '')}:{text}"
                    if dedupe_key in self._native_msg_seen:
                        continue
                    self._native_msg_seen.add(dedupe_key)
                    if len(self._native_msg_seen) > 500:
                        self._native_msg_seen = set(list(self._native_msg_seen)[-250:])

                    seller_nick = cdp.seller_nick or self._current_seller or "当前卖家"
                    session = await self._ensure_session(cdp, seller_nick)
                    if session:
                        await session.handle_native_text_message(nick, uid, text, dedupe_key)
                else:
                    logger.info(f"原生旺旺消息无正文 [{nick}/{uid}], type={msg_type}")
            return

        if event_name == "im.singlemsg.onReceiveNewMsg":
            items = data if isinstance(data, list) else []
            for item in items:
                if isinstance(item, dict):
                    ccode = item.get("ccode") or item.get("cid", {}).get("ccode", "")
                    logger.info(f"检测到聊天新消息通知: ccode={ccode}")
            return

        if event_name == "im.singlemsg.onShopRobotReceriveNewMsgs":
            items = data if isinstance(data, list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                cid = item.get("cid", {}) if isinstance(item.get("cid"), dict) else {}
                ccode = cid.get("ccode") or item.get("ccode", "")
                nick = cid.get("nick") or ""
                newmsgs = item.get("newmsgs", [])
                logger.info(f"检测到客服新消息: buyer={nick}, ccode={ccode}, count={len(newmsgs) if isinstance(newmsgs, list) else 0}")

    def get_session(self, seller_nick: str) -> Optional[SellerSession]:
        """获取指定卖家的 session"""
        return self.sessions.get(seller_nick)

    def get_current_session(self) -> Optional[SellerSession]:
        """获取当前活跃卖家的 session"""
        if self._current_seller:
            return self.sessions.get(self._current_seller)
        return None

    def get_all_sellers(self) -> list[str]:
        """获取所有在线卖家"""
        return list(self.sessions.keys())

    def session_count(self) -> int:
        """在线卖家数"""
        return len(self.sessions)
