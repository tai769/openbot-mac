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
        self._buyer_targets: dict[str, dict[str, str]] = {}
        self._current_buyer: str = ""
        self._current_target_id: str = ""
        self._current_ccode: str = ""
        self._reply_lock = asyncio.Lock()
        self._pending_messages: dict[str, list[dict[str, str]]] = {}
        self._pending_reply_tasks: dict[str, asyncio.Task] = {}
        self._pending_activate_tasks: dict[str, asyncio.Task] = {}
        self._recent_incoming: dict[str, float] = {}
        self._recent_reply_intents: dict[str, float] = {}

    def _select_send_cdp(self) -> CDPSession:
        """Use the real chat WebView for sending; bridge/summary pages cannot call imsdk."""
        for cdp in self.server.sessions.values():
            if cdp.seller_nick == self.seller_nick and cdp.is_chat_session:
                return cdp
        for cdp in self.server.sessions.values():
            if cdp.is_chat_session:
                return cdp
        return self.cdp

    async def _select_send_cdp_ready(self) -> CDPSession:
        cdp = self._select_send_cdp()
        if cdp.is_chat_session:
            return cdp
        await self._refresh_page_contexts()
        return self._select_send_cdp()

    def _open_chat_cdps(self) -> list[CDPSession]:
        """All Qianniu WebViews that may accept openChat. mac splits capabilities across pages."""
        candidates: list[CDPSession] = []
        for cdp in self.server.sessions.values():
            same_seller = cdp.seller_nick == self.seller_nick if self.seller_nick else False
            looks_like_qianniu = (
                getattr(cdp, "has_workbench", False) or
                cdp.is_chat_session or
                "alires-webui" in cdp.href or
                "crs-qn" in cdp.href
            )
            if same_seller or looks_like_qianniu:
                candidates.append(cdp)
        if self.cdp not in candidates:
            candidates.append(self.cdp)
        return candidates

    async def _open_chat_everywhere(
        self,
        buyer: str,
        target_id: str = "",
        ccode: str = "",
        *,
        phase: str,
    ):
        """Mirror the author's OpenChat call, but broadcast it to mac's multiple Qianniu WebViews."""
        cdps = self._open_chat_cdps()
        logger.info("向 %s 个千牛页面下发 openChat: buyer=%s phase=%s", len(cdps), buyer, phase)
        for cdp in cdps:
            try:
                await cdp.open_chat_context(buyer, target_id, ccode)
                if buyer:
                    await cdp.open_chat(buyer)
                    if cdp.is_chat_session:
                        await cdp.dom_click_conversation_by_name(buyer)
            except Exception as e:
                logger.info("下发 openChat 失败: buyer=%s href=%s err=%s", buyer, cdp.href, e)

    @staticmethod
    def _target_id_from_ccode(ccode: str) -> str:
        if not ccode:
            return ""
        left = str(ccode).split("-", 1)[0]
        return left.split(".", 1)[0]

    def _remember_buyer_target(
        self,
        buyer: str,
        target_id: str = "",
        ccode: str = "",
        uid: str = "",
    ):
        if not buyer:
            return
        current = self._buyer_targets.setdefault(buyer, {"target_id": "", "ccode": "", "uid": ""})
        parsed_target_id = self._target_id_from_ccode(ccode)
        if target_id or parsed_target_id:
            current["target_id"] = target_id or parsed_target_id
        if ccode:
            current["ccode"] = ccode
        if uid:
            current["uid"] = uid

    def _target_for_ccode(self, ccode: str = "", target_id: str = "") -> tuple[str, str, str]:
        """Find remembered buyer info from a ccode/targetId-only notification."""
        parsed_target_id = self._target_id_from_ccode(ccode)
        target_id = target_id or parsed_target_id
        for buyer, target in self._buyer_targets.items():
            remembered_ccode = target.get("ccode", "")
            remembered_target_id = target.get("target_id", "")
            if ccode and remembered_ccode == ccode:
                return buyer, remembered_target_id or target_id, remembered_ccode
            if target_id and remembered_target_id == target_id:
                return buyer, remembered_target_id, remembered_ccode or ccode
        return "", target_id, ccode

    def _is_current_target(self, buyer: str, target_id: str = "", ccode: str = "") -> bool:
        if buyer and self._current_buyer == buyer:
            return True
        if ccode and self._current_ccode == ccode:
            return True
        if target_id and self._current_target_id == target_id:
            return True
        return False

    def _log_if_target_drifted(self, buyer: str, target_id: str = "", ccode: str = "", phase: str = "") -> bool:
        if self._is_current_target(buyer, target_id, ccode):
            return False
        logger.warning(
            "发送中止，当前会话已偏离目标: phase=%s, target=%s/%s/%s, current=%s/%s/%s",
            phase,
            buyer,
            target_id,
            ccode,
            self._current_buyer,
            self._current_target_id,
            self._current_ccode,
        )
        return True

    async def _refresh_current_from_chat_pages(self) -> bool:
        """Read current conversation directly from chat WebViews; events are not always emitted on mac."""
        refreshed = False
        for cdp in self.server.sessions.values():
            if not cdp.href:
                try:
                    await cdp.get_page_context()
                except Exception:
                    pass
            if not cdp.is_chat_session:
                continue
            try:
                conv = await cdp.get_current_conversation()
            except Exception as e:
                logger.info("读取当前会话失败: href=%s err=%s", cdp.href, e)
                continue
            if not isinstance(conv, dict) or not conv:
                continue
            buyer_nick = str(conv.get("nick") or conv.get("display") or "")
            target_id = str(conv.get("targetId") or "")
            ccode = str(conv.get("ccode") or "")
            if buyer_nick or target_id or ccode:
                self._current_buyer = buyer_nick
                self._current_target_id = target_id
                self._current_ccode = ccode
                if buyer_nick:
                    self._remember_buyer_target(buyer_nick, target_id=target_id, ccode=ccode)
                logger.info(
                    "主动读取当前会话: buyer=%s target_id=%s ccode=%s",
                    buyer_nick,
                    target_id,
                    ccode,
                )
                refreshed = True
                break
        return refreshed

    async def _refresh_page_contexts(self):
        for cdp in list(self.server.sessions.values()):
            if cdp.href and (cdp.has_imsdk or getattr(cdp, "has_workbench", False)):
                continue
            try:
                ctx = await cdp.get_page_context()
                if ctx:
                    logger.info(
                        "刷新页面上下文: session=%s href=%s chat=%s imsdk=%s vs=%s",
                        cdp.session_id,
                        cdp.href,
                        cdp.is_chat_session,
                        cdp.has_imsdk,
                        cdp.has_vs,
                    )
            except Exception as e:
                logger.info("刷新页面上下文失败: session=%s err=%s", cdp.session_id, e)

    async def _wait_for_target_conversation(
        self,
        buyer: str,
        target_id: str = "",
        ccode: str = "",
        timeout: float = 5.0,
    ) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._is_current_target(buyer, target_id, ccode):
                return True
            await self._refresh_current_from_chat_pages()
            if self._is_current_target(buyer, target_id, ccode):
                return True
            await asyncio.sleep(0.35)
        logger.info(
            "等待目标会话超时: target=%s/%s/%s current=%s/%s/%s",
            buyer,
            target_id,
            ccode,
            self._current_buyer,
            self._current_target_id,
            self._current_ccode,
        )
        return False

    @staticmethod
    def _collapse_messages(messages: list[str]) -> str:
        """Collapse a buyer's short burst into one intent, removing repeated adjacent text."""
        collapsed: list[str] = []
        for message in messages:
            text = (message or "").strip()
            if not text:
                continue
            if collapsed and collapsed[-1] == text:
                continue
            collapsed.append(text)
        return "\n".join(collapsed)

    def _remember_incoming(self, buyer: str, text: str, window: float = 60.0) -> bool:
        key = f"{buyer}:{text}"
        now = time.time()
        last_seen = self._recent_incoming.get(key, 0)
        self._recent_incoming[key] = now
        if len(self._recent_incoming) > 500:
            cutoff = now - 600
            self._recent_incoming = {k: v for k, v in self._recent_incoming.items() if v >= cutoff}
        return now - last_seen >= window

    def _remember_reply_intent(self, buyer: str, question: str, window: float = 90.0) -> bool:
        key = f"{buyer}:{question}"
        now = time.time()
        last_seen = self._recent_reply_intents.get(key, 0)
        self._recent_reply_intents[key] = now
        if len(self._recent_reply_intents) > 300:
            cutoff = now - 900
            self._recent_reply_intents = {k: v for k, v in self._recent_reply_intents.items() if v >= cutoff}
        return now - last_seen >= window

    def _queue_auto_reply(self, seller: str, buyer: str, message_text: str, item_id: str = ""):
        """Debounce messages per buyer so one burst produces one reply."""
        if not self._remember_incoming(buyer, message_text):
            logger.info("忽略短时间重复买家消息 [%s]: %s", buyer, message_text[:50])
            return

        self._pending_messages.setdefault(buyer, []).append({
            "seller": seller,
            "text": message_text,
            "item_id": item_id,
        })
        existing = self._pending_reply_tasks.get(buyer)
        if existing and not existing.done():
            existing.cancel()
        self._pending_reply_tasks[buyer] = asyncio.create_task(self._flush_auto_reply(buyer))

    async def _flush_auto_reply(self, buyer: str):
        try:
            await asyncio.sleep(max(1.2, float(config.robot.reply_delay)))
            pending = self._pending_messages.pop(buyer, [])
            if not pending:
                return

            seller = pending[-1].get("seller") or self.seller_nick
            question = self._collapse_messages([item.get("text", "") for item in pending])
            item_id = next((item.get("item_id", "") for item in reversed(pending) if item.get("item_id")), "")
            if not question:
                return
            if not self._remember_reply_intent(buyer, question):
                logger.info("忽略短时间重复回复意图 [%s]: %s", buyer, question[:50])
                return

            rule_answer = await self.rules.match(question)
            if rule_answer:
                logger.info(f"规则命中 [{buyer}]: {rule_answer[:30]}...")
                await self._send_reply(seller, buyer, rule_answer, is_auto=True)
                return

            knowledge_ctx = await self.knowledge.build_context(question, item_id)
            answer = await ai_client.get_answer(seller, buyer, question, knowledge_ctx)
            if answer:
                await self._send_reply(seller, buyer, answer, is_auto=True)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error("合并自动回复异常 [%s]: %s", buyer, e)
        finally:
            current = self._pending_reply_tasks.get(buyer)
            if current is asyncio.current_task():
                self._pending_reply_tasks.pop(buyer, None)

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
        self._remember_buyer_target(buyer_nick, target_id=buyer_uid)
        await self.chat_logger.log(seller_nick, buyer_nick, seller_nick, message_text)

        if not config.robot.auto_reply:
            logger.debug("自动回复已关闭")
            return
        self._queue_auto_reply(seller_nick, buyer_nick, message_text)

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
        target_id = conversation.get("targetId", "") if isinstance(conversation, dict) else ""
        ccode = conversation.get("ccode", "") if isinstance(conversation, dict) else ""
        seller_nick = login_id.get("nick", "") if isinstance(login_id, dict) else self.seller_nick

        if seller_nick:
            self.seller_nick = seller_nick
        if not buyer_nick:
            return
        self._remember_buyer_target(buyer_nick, target_id=target_id, ccode=ccode)

        logger.info(f"机器人新消息通知 [{buyer_nick}]")

        if config.robot.auto_reply:
            cdp = self._select_send_cdp()
            key = ccode or target_id or buyer_nick
            existing = self._pending_activate_tasks.get(key)
            if not existing or existing.done():
                self._pending_activate_tasks[key] = asyncio.create_task(
                    self._activate_buyer_chat(cdp, buyer_nick, target_id, ccode, task_key=key)
                )

    async def _activate_buyer_chat(
        self,
        cdp: CDPSession,
        buyer_nick: str,
        target_id: str = "",
        ccode: str = "",
        task_key: str = "",
    ):
        """Automatically switch Qianniu to the buyer chat so message bodies become available."""
        try:
            if not buyer_nick:
                buyer_nick, target_id, ccode = self._target_for_ccode(ccode, target_id)
            if self._is_current_target(buyer_nick, target_id, ccode):
                logger.info(
                    "已在目标买家会话，无需切换: buyer=%s target_id=%s ccode=%s",
                    buyer_nick,
                    target_id,
                    ccode,
                )
                await cdp.trigger_page_message_scan("alreadyActive")
                return
            for attempt in range(5):
                try:
                    logger.info(
                        "自动切换买家会话尝试: buyer=%s target_id=%s ccode=%s attempt=%s",
                        buyer_nick,
                        target_id,
                        ccode,
                        attempt + 1,
                    )
                    await self._open_chat_everywhere(
                        buyer_nick,
                        target_id,
                        ccode,
                        phase=f"activate:{attempt + 1}",
                    )
                    if buyer_nick and attempt >= 1:
                        await cdp.click_conversation_by_name(buyer_nick)
                    opened = await self._wait_for_target_conversation(
                        buyer_nick,
                        target_id,
                        ccode,
                        timeout=1.6 + attempt * 0.6,
                    )
                    if opened or self._is_current_target(buyer_nick, target_id, ccode):
                        await cdp.trigger_page_message_scan(f"autoOpen:{attempt + 1}")
                        logger.info("已自动切换到买家会话 [%s]", buyer_nick or ccode)
                        return
                except Exception as e:
                    logger.info("自动打开买家会话失败 [%s]: %s", buyer_nick, e)
                await asyncio.sleep(0.7)
            logger.warning("自动切换买家会话未确认: buyer=%s, ccode=%s", buyer_nick, ccode)
        finally:
            if task_key and self._pending_activate_tasks.get(task_key) is asyncio.current_task():
                self._pending_activate_tasks.pop(task_key, None)

    async def handle_conversation_change(self, *args):
        """处理会话切换 — 复刻 QN.Cdp_EvBuyerSwitched。"""
        response = args[-1] if args else ""
        try:
            payload = json.loads(response) if isinstance(response, str) else response
            conversation = payload.get("conversation", {}) if isinstance(payload, dict) else {}
            login_id = payload.get("loginID", {}) if isinstance(payload, dict) else {}
            if not conversation and isinstance(payload, dict):
                raw_name = payload.get("name")
                if isinstance(raw_name, str) and raw_name.lstrip().startswith("{"):
                    raw_conversation = json.loads(raw_name)
                    if isinstance(raw_conversation, dict):
                        conversation = raw_conversation
        except Exception as e:
            logger.error(f"解析会话切换失败: {e}")
            return

        buyer_nick = conversation.get("nick", "") if isinstance(conversation, dict) else ""
        seller_nick = login_id.get("nick", "") if isinstance(login_id, dict) else ""
        if seller_nick:
            self.seller_nick = seller_nick
        if buyer_nick:
            self._current_buyer = buyer_nick
            self._current_target_id = conversation.get("targetId", "") if isinstance(conversation, dict) else ""
            self._current_ccode = conversation.get("ccode", "") if isinstance(conversation, dict) else ""
            self._remember_buyer_target(
                buyer_nick,
                target_id=self._current_target_id,
                ccode=self._current_ccode,
            )
            logger.info(f"当前会话切换: seller={self.seller_nick}, buyer={buyer_nick}")
            try:
                unread_count = int(conversation.get("unreadcount") or 0) if isinstance(conversation, dict) else 0
            except Exception:
                unread_count = 0
            if unread_count > 0:
                cdp = await self._select_send_cdp_ready()
                logger.info("会话切换后发现未读消息，触发页面扫描: buyer=%s unread=%s", buyer_nick, unread_count)
                await cdp.trigger_page_message_scan(f"conversationUnread:{buyer_nick}:{unread_count}")

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
        self._remember_buyer_target(
            buyer_nick,
            target_id=self._target_id_from_ccode(msg.ccode),
            ccode=msg.ccode,
            uid=msg.fromid.uid,
        )

        # 保存聊天记录
        await self.chat_logger.log(seller_nick, buyer_nick, seller_nick, message_text)

        if not config.robot.auto_reply:
            logger.debug("自动回复已关闭")
            return
        self._queue_auto_reply(seller_nick, buyer_nick, message_text, msg.original_data.item_id)

    async def _send_reply(self, seller: str, buyer: str, text: str, is_auto: bool = False):
        """
        发送回复 — 复刻 QN.SendTextAsync + QNRpa.SendTextAsync
        通过 imsdk API 发送消息。
        """
        async with self._reply_lock:
            try:
                cdp = await self._select_send_cdp_ready()
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
                if not cdp.is_chat_session:
                    logger.warning(
                        "未识别到真实聊天页，发送确认只能依赖全局事件/无障碍: href=%s imsdk=%s vs=%s",
                        cdp.href,
                        cdp.has_imsdk,
                        cdp.has_vs,
                    )

                # 打开买家聊天窗口。mac 端 execute 通道不稳定，不能依赖 WebView eval 再确认；
                # 以会话切换事件作为确认来源，必要时用无障碍点击会话兜底。
                buyer_ctx = self._buyer_targets.get(buyer, {})
                target_id = buyer_ctx.get("target_id", "")
                ccode = buyer_ctx.get("ccode", "")
                logger.info(
                    "发送前会话目标: buyer=%s, target_id=%s, ccode=%s, uid=%s, current=%s/%s/%s",
                    buyer,
                    target_id,
                    ccode,
                    buyer_ctx.get("uid", ""),
                    self._current_buyer,
                    self._current_target_id,
                    self._current_ccode,
                )
                opened = self._is_current_target(buyer, target_id, ccode)
                if not opened:
                    await self._open_chat_everywhere(buyer, target_id, ccode, phase="send")
                    opened = await self._wait_for_target_conversation(buyer, target_id, ccode, timeout=4.0)
                if not opened:
                    await cdp.click_conversation_by_name(buyer)
                    opened = await self._wait_for_target_conversation(buyer, target_id, ccode, timeout=2.0)
                if not opened:
                    logger.warning(
                        "未收到会话切换确认，暂停发送避免发错: buyer=%s, current=%s",
                        buyer,
                        self._current_buyer,
                    )
                    return
                await asyncio.sleep(0.3)

                # 插入文本到输入框
                success = False
                if self._log_if_target_drifted(buyer, target_id, ccode, "before_insert"):
                    return
                await cdp.insert_text_to_inputbox(buyer, text)
                await cdp.dom_fill_inputbox(text)
                logger.info("已下发输入框填充命令 [%s]", buyer)
                await asyncio.sleep(1.0)
                if self._log_if_target_drifted(buyer, target_id, ccode, "before_paste"):
                    return
                pasted = await cdp.paste_text_to_inputbox(text)
                if pasted:
                    logger.info("已通过无障碍粘贴回复文本 [%s]", buyer)
                else:
                    logger.warning("无障碍粘贴回复文本失败，继续尝试发送但不重复插入 [%s]", buyer)
                await asyncio.sleep(1.0)

                send_confirmation = cdp.create_send_confirmation(text)
                for send_attempt in range(2):
                    if self._log_if_target_drifted(buyer, target_id, ccode, f"before_enter:{send_attempt + 1}"):
                        return
                    logger.info("回复文本已准备，准备发送 [%s] attempt=%s", buyer, send_attempt + 1)
                    await cdp.press_enter()
                    success = await cdp.wait_for_send_confirmation(
                        send_confirmation,
                        timeout=8.0,
                        keep_pending_on_timeout=True,
                    )
                    if not success:
                        if self._log_if_target_drifted(buyer, target_id, ccode, "before_dom_send"):
                            return
                        logger.warning(f"回车发送未确认，尝试 DOM 点击发送按钮 [{buyer}]")
                        await cdp.dom_click_send_button()
                        success = await cdp.wait_for_send_confirmation(
                            send_confirmation,
                            timeout=5.0,
                            keep_pending_on_timeout=True,
                        )
                    if not success:
                        if self._log_if_target_drifted(buyer, target_id, ccode, "before_ax_send"):
                            return
                        logger.warning(f"DOM 点击发送未确认，尝试无障碍点击发送按钮 [{buyer}]")
                        clicked = await cdp.click_send_button()
                        if clicked:
                            logger.info(f"已点击发送区域 [{buyer}]")
                        success = await cdp.wait_for_send_confirmation(
                            send_confirmation,
                            timeout=8.0,
                            keep_pending_on_timeout=send_attempt == 0,
                        )
                    if success:
                        break
                    if send_attempt == 0:
                        logger.warning("本轮发送未确认，将继续尝试发送现有输入框内容，避免重复插入 [%s]", buyer)
                if not success:
                    await cdp.log_accessibility_snapshot()

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
            if any(other.seller_nick == nick for other in self.server.sessions.values()):
                logger.debug("卖家仍有其他页面连接，保留会话: %s", nick)
                return
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
        event_name = str(payload.get("sid") or payload.get("name") or "")
        data_source = payload.get("data", "")
        if not data_source and str(payload.get("name", "")).lstrip().startswith(("[", "{")):
            data_source = payload.get("name", "")
        data = self._decode_nested_json(data_source)

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
                    cid = item.get("cid", {})
                    ccode = item.get("ccode") or (cid.get("ccode", "") if isinstance(cid, dict) else "")
                    logger.info(f"检测到聊天新消息通知: ccode={ccode}")
                    seller_nick = cdp.seller_nick or self._current_seller or "当前卖家"
                    session = await self._ensure_session(cdp, seller_nick)
                    if session and ccode:
                        buyer, target_id, resolved_ccode = session._target_for_ccode(ccode)
                        key = resolved_ccode or target_id or buyer
                        existing = session._pending_activate_tasks.get(key)
                        if not existing or existing.done():
                            session._pending_activate_tasks[key] = asyncio.create_task(
                                session._activate_buyer_chat(cdp, buyer, target_id, resolved_ccode, task_key=key)
                            )
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
