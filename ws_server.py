"""
WebSocket 服务器 — 复刻 openbot MyWebSocketServer + CDPClient
在 127.0.0.1:41010 启动 WebSocket 服务器，与千牛 JS 注入脚本通信。
"""

from __future__ import annotations
import asyncio
import json
import logging
import subprocess
import time
from typing import Any, Callable, Coroutine, Optional
import websockets
from websockets.asyncio.server import serve, ServerConnection

from message import WSMessage, ChatResponse, ConversationEvent, MessageNotify

logger = logging.getLogger(__name__)

WS_PORT = 41010
WS_HOST = "127.0.0.1"


def _decode_response(response: str) -> Any:
    try:
        return json.loads(response)
    except Exception:
        return {}


def _ability_event_name(response: str) -> str:
    payload = _decode_response(response)
    if isinstance(payload, dict):
        name = payload.get("name")
        if name:
            return str(name)
        args = payload.get("args")
        if isinstance(args, list) and len(args) >= 4:
            try:
                meta = json.loads(args[3])
                return str(meta.get("eventName", ""))
            except Exception:
                return ""
    return ""


def _should_log_ability(response: str) -> bool:
    return False


def _should_log_probe(response: str) -> bool:
    return False


class CDPSession:
    """
    每个千牛浏览器标签页一个 session — 复刻 openbot CDPClient
    封装 WebSocket 连接，提供千牛 API 调用能力。
    """

    def __init__(self, session_id: str, ws: ServerConnection):
        self.session_id = session_id
        self.ws = ws
        self.seller_nick: str = ""
        self.qn_version: str = ""
        self.href: str = ""
        self.has_imsdk: bool = False
        self.has_vs: bool = False
        self._pending_invokes: dict[str, asyncio.Future] = {}
        self._pending_send_confirmations: list[tuple[str, asyncio.Future]] = []
        self._invoke_counter = 0

        # 事件回调 — 复刻 CDPClient 的事件分发
        self.on_receive_new_msg: Optional[Callable] = None
        self.on_conversation_change: Optional[Callable] = None
        self.on_conversation_add: Optional[Callable] = None
        self.on_conversation_close: Optional[Callable] = None
        self.on_chat_dlg_active: Optional[Callable] = None
        self.on_message_notify: Optional[Callable] = None
        self.on_shop_robot_receive: Optional[Callable] = None
        self.on_bridge_ready: Optional[Callable] = None
        self.on_ability_event: Optional[Callable] = None

    def update_context_from_payload(self, response: str):
        """Refresh WebView capability flags from bridgeReady/openbotContextProbe payloads."""
        payload = _decode_response(response)
        if isinstance(payload, dict) and isinstance(payload.get("value"), dict):
            payload = payload["value"]
        if not isinstance(payload, dict):
            return

        href = payload.get("href")
        if href:
            self.href = str(href)
        if "hasImsdk" in payload:
            self.has_imsdk = bool(payload.get("hasImsdk"))
        elif "hasImsdkInvoke" in payload:
            self.has_imsdk = bool(payload.get("hasImsdkInvoke"))
        if "hasVs" in payload:
            self.has_vs = bool(payload.get("hasVs"))

    async def handle_message(self, raw: str):
        """处理收到的消息 — 复刻 CDPClient.OnRecieveMessage"""
        try:
            msg = WSMessage.from_json(raw)
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"解析消息失败: {e}")
            return

        # 心跳
        if msg.type == "hi":
            return

        # execute 响应 — 复刻 CDPClient 的请求/响应模式
        if msg.type == "execute":
            await self._handle_execute_response(msg)
            return
        if msg.type == "executeNoWaitAck":
            return

        # 事件分发 — 复刻 CDPClient 的事件路由
        if msg.type == "receiveNewMsg":
            if self.on_receive_new_msg:
                await self.on_receive_new_msg(self, msg.response)
        elif msg.type == "onConversationChange":
            if self.on_conversation_change:
                await self.on_conversation_change(self, msg.response)
        elif msg.type == "onConversationAdd":
            if self.on_conversation_add:
                await self.on_conversation_add(self, msg.response)
        elif msg.type == "onConversationClose":
            if self.on_conversation_close:
                await self.on_conversation_close(self, msg.response)
        elif msg.type == "onChatDlgActive":
            if self.on_chat_dlg_active:
                await self.on_chat_dlg_active(self, msg.response)
        elif msg.type == "messageCenterNotify":
            if self.on_message_notify:
                await self.on_message_notify(self, msg.response)
        elif msg.type == "onShopRobotReceriveNewMsgs":
            if self.on_shop_robot_receive:
                await self.on_shop_robot_receive(self, msg.response)
        elif msg.type == "bridgeReady":
            self.update_context_from_payload(msg.response)
            if self.on_bridge_ready:
                await self.on_bridge_ready(self, msg.response)
        elif msg.type in (
            "onAbilityEventNotify",
            "onAbilityPrivateEventNotify",
            "onAbilityInvokeNotify",
            "onAbilityNewInvokeNotify",
            "onAbilityPrivateInvokeNotify",
            "onAbilityPrivateNewNotify",
        ):
            if _should_log_ability(msg.response):
                logger.info(f"{msg.type}: {msg.response[:8000]}")
            if self.on_ability_event:
                await self.on_ability_event(self, msg.response)
        elif msg.type == "workbenchProbe":
            self.update_context_from_payload(msg.response)
            if _should_log_probe(msg.response):
                logger.info(f"workbenchProbe: {msg.response[:8000]}")
        elif msg.type == "extensionHookSeen":
            logger.debug(f"extensionHookSeen: {msg.response[:5000]}")
        elif msg.type == "extensionHookLoaded":
            logger.debug(f"extensionHookLoaded: {msg.response[:5000]}")
        elif msg.type == "nativeWangwangEvent":
            logger.debug(f"nativeWangwangEvent: {msg.response[:8000]}")
            if self.on_ability_event:
                await self.on_ability_event(self, msg.response)
        elif msg.type == "pageMessageCandidate":
            logger.debug(f"pageMessageCandidate: {msg.response[:8000]}")
            if self.on_ability_event:
                await self.on_ability_event(self, msg.response)
        elif msg.type == "rawOnEventNotify":
            event_name = _ability_event_name(msg.response)
            if event_name in (
                "im.singlemsg.onReceiveNewMsg",
                "im.singlemsg.onSendNewMsg",
                "im.singlemsg.onMsgSendUpdate",
                "im.uiutil.onConversationChange",
            ):
                logger.info(f"rawOnEventNotify: {msg.response[:3000]}")
            if event_name == "im.singlemsg.onMsgSendUpdate":
                self._handle_send_update(msg.response)
        elif msg.type in ("macOnEventNotify", "macReceiveNewMsgNotify", "macShopRobotNewMsgs"):
            logger.debug(f"{msg.type}: {msg.response[:8000]}")
            if self.on_ability_event:
                await self.on_ability_event(self, msg.response)
        else:
            logger.debug(f"未处理的消息类型: {msg.type}")

    # ─── 千牛 API 调用 — 复刻 CDPClient.Invoke ───

    async def invoke(self, expression: str, timeout: float = 10.0) -> Any:
        """
        在千牛 web 页面中执行 JS 表达式并等待结果 — 复刻 CDPClient.Invoke
        使用 asyncio.Future 实现请求/响应模式。
        """
        self._invoke_counter += 1
        invoke_id = str(self._invoke_counter)

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_invokes[invoke_id] = future

        msg = WSMessage(method="execute", expression=expression)
        await self._send(msg.to_json())

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            logger.warning(f"invoke 超时: {expression[:100]}...")
            return None
        finally:
            self._pending_invokes.pop(invoke_id, None)

    async def invoke_no_wait(self, expression: str) -> bool:
        """Fire-and-forget JS execution for Qianniu APIs that can block eval responses."""
        msg = WSMessage(method="executeNoWait", expression=expression)
        await self._send(msg.to_json())
        return True

    async def _handle_execute_response(self, msg: WSMessage):
        """处理 execute 响应"""
        if self._pending_invokes:
            # 取出最早等待的 future
            invoke_id = next(iter(self._pending_invokes))
            future = self._pending_invokes.pop(invoke_id, None)
            if future and not future.done():
                result = self._decode_execute_response(msg.response)
                future.set_result(result)

    @staticmethod
    def _decode_execute_response(response: str) -> Any:
        """Decode JS execute responses, including accidentally double-encoded JSON."""
        if not response:
            return None

        result: Any = response
        for _ in range(2):
            if not isinstance(result, str):
                break
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                break
        return result

    def create_send_confirmation(self, text: str) -> asyncio.Future:
        """Create a future that resolves when Qianniu reports this exact text was sent."""
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_send_confirmations.append((self._normalize_text(text), future))
        return future

    async def wait_for_send_confirmation(self, future: asyncio.Future, timeout: float = 8.0) -> bool:
        """Wait until onMsgSendUpdate confirms the message was actually sent."""
        try:
            return bool(await asyncio.wait_for(future, timeout=timeout))
        except asyncio.TimeoutError:
            return False
        finally:
            self._pending_send_confirmations = [
                (text, pending)
                for text, pending in self._pending_send_confirmations
                if pending is not future
            ]

    @staticmethod
    def _normalize_text(text: str) -> str:
        return " ".join(str(text or "").split())

    def _handle_send_update(self, response: str):
        if not self._pending_send_confirmations:
            return
        sent_texts = self._extract_sent_texts(response)
        if not sent_texts:
            return
        normalized_sent = {self._normalize_text(text) for text in sent_texts}
        for expected, future in list(self._pending_send_confirmations):
            if future.done():
                continue
            if expected in normalized_sent:
                future.set_result(True)

    @staticmethod
    def _extract_sent_texts(response: str) -> list[str]:
        payload = _decode_response(response)
        if not isinstance(payload, dict):
            return []
        name = payload.get("name")
        if not isinstance(name, str):
            return []
        try:
            events = json.loads(name)
        except Exception:
            return []
        if not isinstance(events, list):
            return []

        texts: list[str] = []
        for event in events:
            if not isinstance(event, dict):
                continue
            original = event.get("originalData")
            if isinstance(original, dict) and isinstance(original.get("text"), str):
                texts.append(original["text"])
        return texts

    # ─── 千牛 API 快捷方法 — 复刻 CDPClient 的各个方法 ───

    async def get_current_user(self) -> Optional[str]:
        """获取当前登录用户 — 复刻 CDPClient.GetCurrentUser"""
        result = await self.invoke(
            "(async()=>{"
            "if(typeof imsdk==='undefined'||typeof imsdk.invoke!=='function')"
            "return {error:'imsdk unavailable'};"
            "return await imsdk.invoke('im.login.GetCurrentLoginID');"
            "})()",
            timeout=3.0,
        )
        if result and isinstance(result, dict):
            nick = result.get("result", {}).get("nick", "")
            self.seller_nick = nick
            return nick
        return None

    async def get_page_context(self) -> dict:
        """获取当前 WebView 环境信息，用于区分聊天页和普通页面。"""
        result = await self.invoke(
            "({"
            "href:String(location.href),"
            "title:String(document.title||''),"
            "readyState:String(document.readyState||''),"
            "hasImsdk:typeof imsdk!=='undefined'&&typeof imsdk.invoke==='function',"
            "hasQN:typeof QN!=='undefined',"
            "hasWorkbench:typeof workbench!=='undefined',"
            "hasQnSdk:typeof qnSdk!=='undefined'||typeof QNPlugin!=='undefined',"
            "hasAbilityNotify:typeof onAbilityEventNotify==='function'||typeof onAbilityPrivateEventNotify==='function',"
            "hasOnEventNotify:typeof window.onEventNotify==='function',"
            "hasVs:typeof window._vs!=='undefined'"
            "})",
            timeout=3.0,
        )
        if isinstance(result, dict):
            self.href = str(result.get("href") or "")
            self.has_imsdk = bool(result.get("hasImsdk"))
            self.has_vs = bool(result.get("hasVs"))
            return result
        return {}

    @property
    def is_chat_session(self) -> bool:
        """Only the real chat WebView has imsdk/_vs and can send text reliably."""
        return self.has_imsdk and self.has_vs and "web_chat-packer/recent.html" in self.href

    async def get_version(self) -> str:
        """获取千牛版本 — 复刻 CDPClient.GetVersion"""
        result = await self.invoke(
            "imsdk.invoke('application.getVersion')"
        )
        if result and isinstance(result, dict):
            self.qn_version = result.get("result", {}).get("version", "")
        return self.qn_version

    async def insert_text_to_inputbox(self, uid: str, text: str) -> bool:
        """插入文本到输入框 — 复刻 CDPClient.InsertText2Inputbox"""
        qn_uid = uid if uid.startswith("cntaobao") else f"cntaobao{uid}"
        param = json.dumps({"uid": qn_uid, "text": text}, ensure_ascii=False)
        result = await self.invoke(
            "(()=>{"
            "if(typeof imsdk==='undefined'||typeof imsdk.invoke!=='function')return {ok:false,error:'imsdk unavailable'};"
            f"const param={param};"
            "try{imsdk.invoke('application.insertText2Inputbox',param);return {ok:true,submitted:true};}"
            "catch(e){return {ok:false,error:String(e&&e.message||e)}}"
            "})()",
            timeout=3.0,
        )
        if isinstance(result, dict) and result.get("error") == "imsdk unavailable":
            return False
        # 千牛这个接口经常执行成功但不返回；超时也当作已提交插入命令。
        return True

    async def open_chat(self, nick: str) -> bool:
        """打开买家聊天窗口 — 复刻 CDPClient.OpenChat"""
        qn_nick = nick if nick.startswith("cntaobao") else f"cntaobao{nick}"
        param = json.dumps({"nick": qn_nick}, ensure_ascii=False)
        return await self.invoke_no_wait(
            "(()=>{"
            "if(typeof imsdk==='undefined'||typeof imsdk.invoke!=='function')return {ok:false,error:'imsdk unavailable'};"
            f"const param={param};"
            "setTimeout(()=>imsdk.invoke('application.openChat',param),0);"
            "})()"
        )

    async def open_chat_context(self, nick: str = "", target_id: str = "", ccode: str = "") -> bool:
        """Open a buyer chat using both imsdk and workbench, for inactive/new conversations."""
        uid = nick if nick.startswith("cntaobao") else (f"cntaobao{nick}" if nick else "")
        params = {
            "uid": uid,
            "nick": uid or nick,
            "cid": ccode or "",
            "securityUID": target_id or "",
            "targetId": target_id or "",
            "bizDomain": "taobao",
            "bizType": "11001",
        }
        payload = json.dumps(params, ensure_ascii=False)
        return await self.invoke_no_wait(
            "(()=>{"
            f"const p={payload};"
            "setTimeout(()=>{"
            "try{if(typeof imsdk!=='undefined'&&imsdk.invoke&&p.nick)imsdk.invoke('application.openChat',{nick:p.nick});}catch(e){}"
            "try{if(typeof workbench!=='undefined'&&workbench.application&&workbench.application.invoke)workbench.application.invoke('qn.openChat',p);}catch(e){}"
            "try{if(typeof workbench!=='undefined'&&workbench.wangwang&&workbench.wangwang.invoke)workbench.wangwang.invoke('qn.openChat',p);}catch(e){}"
            "},0);"
            "})()"
        )

    async def trigger_page_message_scan(self, reason: str = "manual") -> bool:
        payload = json.dumps(reason, ensure_ascii=False)
        return await self.invoke_no_wait(
            "(()=>{"
            f"const reason={payload};"
            "setTimeout(()=>{try{if(window.__openbotScanPageMessages)window.__openbotScanPageMessages(reason);}catch(e){}},0);"
            "})()"
        )

    async def paste_text_to_inputbox(self, text: str) -> bool:
        """Paste text into Qianniu's focused chat input via macOS Accessibility."""
        escaped = json.dumps(text, ensure_ascii=False)
        script = f'''
set replyText to {escaped}
set oldClipboard to ""
try
    set oldClipboard to the clipboard as text
end try

set the clipboard to replyText
delay 0.1

tell application "System Events"
    set targetProcesses to {{"AliWorkbench", "千牛", "Aliworkbench", "Qianniu"}}
    repeat with processName in targetProcesses
        if exists process processName then
            tell process processName
                set frontmost to true
                delay 0.1
                if (count of windows) > 0 then
                    set win to window 1
                    try
                        set p to position of win
                        set s to size of win
                        set x to (item 1 of p) + ((item 1 of s) / 2)
                        set y to (item 2 of p) + (item 2 of s) - 95
                        click at {{x, y}}
                        delay 0.1
                    end try
                end if
            end tell
            keystroke "a" using command down
            delay 0.05
            keystroke "v" using command down
            delay 0.2
            if oldClipboard is not "" then set the clipboard to oldClipboard
            return "pasted"
        end if
    end repeat
end tell

if oldClipboard is not "" then set the clipboard to oldClipboard
return "not_found"
'''

        def _run() -> bool:
            try:
                completed = subprocess.run(
                    ["osascript", "-e", script],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=5,
                    check=False,
                )
                if completed.stdout.strip() == "pasted":
                    return True
                if completed.stderr:
                    logger.debug("粘贴回复失败: %s", completed.stderr.strip())
                return False
            except Exception as e:
                logger.debug("粘贴回复异常: %s", e)
                return False

        return await asyncio.to_thread(_run)

    async def click_send_button(self) -> bool:
        """Click Qianniu's Send button via macOS Accessibility, mirroring QNRpa._sendMessageButton.Click()."""
        script = r'''
on clickWindowSendArea(win)
    tell application "System Events"
        try
            set p to position of win
            set s to size of win
            set baseX to (item 1 of p) + (item 1 of s)
            set baseY to (item 2 of p) + (item 2 of s)
            set candidates to {{baseX - 58, baseY - 44}, {baseX - 82, baseY - 44}, {baseX - 58, baseY - 68}, {baseX - 110, baseY - 44}}
            repeat with pointValue in candidates
                click at pointValue
                delay 0.15
            end repeat
            return true
        end try
    end tell
    return false
end clickWindowSendArea

on clickSendInProcess(processName)
    tell application "System Events"
        if exists process processName then
            tell process processName
                set frontmost to true
                delay 0.15
                if (count of windows) > 0 then
                    if my clickWindowSendArea(window 1) then return true
                end if
            end tell
        end if
    end tell
    return false
end clickSendInProcess

tell application "System Events"
    set targetProcesses to {"AliWorkbench", "千牛", "Aliworkbench", "Qianniu"}
    repeat with processName in targetProcesses
        if my clickSendInProcess(processName) then return "clicked"
    end repeat
end tell
return "not_found"
'''

        def _run() -> bool:
            try:
                completed = subprocess.run(
                    ["osascript", "-e", script],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=3,
                    check=False,
                )
                if completed.stdout.strip() == "clicked":
                    return True
                if completed.stderr:
                    logger.debug("点击发送按钮失败: %s", completed.stderr.strip())
                logger.debug("点击发送按钮未找到控件: stdout=%s", completed.stdout.strip())
                return False
            except Exception as e:
                logger.debug("点击发送按钮异常: %s", e)
                return False

        return await asyncio.to_thread(_run)

    async def press_enter(self) -> bool:
        """通过 macOS 无障碍触发发送，作为发送按钮不可直接调用时的兜底。"""
        script = r'''
tell application "System Events"
    set targetProcesses to {"AliWorkbench", "千牛", "Aliworkbench", "Qianniu"}
    repeat with processName in targetProcesses
        if exists process processName then
            tell process processName
                set frontmost to true
                delay 0.1
            end tell
            key code 36
            delay 0.2
            key code 36 using command down
            return "pressed"
        end if
    end repeat
    key code 36
    delay 0.2
    key code 36 using command down
end tell
return "pressed"
'''

        def _run() -> bool:
            try:
                completed = subprocess.run(
                    ["osascript", "-e", script],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=4,
                    check=False,
                )
                return completed.stdout.strip() == "pressed"
            except Exception:
                return False

        return await asyncio.to_thread(_run)

    async def send_timi_msg(self, user_id: str, text: str) -> bool:
        """发送智能提示消息（旧版千牛） — 复刻 CDPClient.SendTimiMsg"""
        param = json.dumps({"userId": user_id, "smartTip": text}, ensure_ascii=False)
        result = await self.invoke(
            f"imsdk.invoke('intelligentservice.SendSmartTipMsg', {param})"
        )
        return result is not None

    async def get_buyer_info(self, nick: str) -> Optional[dict]:
        """获取买家信息 — 复刻 CDPClient.GetBuyerInfo"""
        result = await self.invoke(
            f"imsdk.invoke('mtop.taobao.znkf.seller.api.getBuyerInfo', {{nick:'{nick}'}})"
        )
        return result

    async def get_item_records(self, uid: str) -> Optional[dict]:
        """获取买家咨询的商品 — 复刻 CDPClient.GetItemRecords"""
        result = await self.invoke(
            f"imsdk.invoke('mtop.taobao.znkf.im.api.getItemRecords', {{uid:'{uid}'}})"
        )
        return result

    # ─── 底层发送 ───

    async def _send(self, data: str):
        """发送原始数据"""
        try:
            await self.ws.send(data)
        except Exception as e:
            logger.error(f"发送失败: {e}")


class WebSocketServer:
    """
    WebSocket 服务器 — 复刻 openbot MyWebSocketServer
    管理所有千牛浏览器标签页的连接。
    """

    def __init__(self):
        self.sessions: dict[str, CDPSession] = {}  # session_id -> CDPSession
        self.sellers: dict[str, CDPSession] = {}  # seller_nick -> CDPSession
        self._server = None
        self._session_counter = 0

        # 事件回调
        self.on_seller_connected: Optional[Callable] = None
        self.on_seller_disconnected: Optional[Callable] = None
        self.on_message_received: Optional[Callable] = None
        self.on_conversation_change: Optional[Callable] = None
        self.on_chat_dlg_active: Optional[Callable] = None
        self.on_shop_robot_receive: Optional[Callable] = None
        self.on_bridge_ready: Optional[Callable] = None
        self.on_ability_event: Optional[Callable] = None

    async def start(self):
        """启动 WebSocket 服务器 — 复刻 MyWebSocketServer.Start"""
        self._server = await serve(self._handle_connection, WS_HOST, WS_PORT)
        logger.info(f"WebSocket 服务器已启动: ws://{WS_HOST}:{WS_PORT}")

    async def stop(self):
        """停止服务器"""
        if self._server:
            self._server.close()
            logger.info("WebSocket 服务器已停止")

    async def _handle_connection(self, ws: ServerConnection):
        """处理新连接 — 复刻 MyWebSocketServer.OnNewSessionConnected"""
        self._session_counter += 1
        session_id = f"session_{self._session_counter}"
        session = CDPSession(session_id, ws)
        self.sessions[session_id] = session

        logger.info(f"新连接: {session_id}")

        try:
            # 注册事件处理
            session.on_receive_new_msg = self._on_receive_new_msg
            session.on_conversation_change = self._on_conversation_change
            session.on_conversation_add = self._on_conversation_add
            session.on_conversation_close = self._on_conversation_close
            session.on_chat_dlg_active = self._on_chat_dlg_active
            session.on_message_notify = self._on_message_notify
            session.on_shop_robot_receive = self._on_shop_robot_receive
            session.on_bridge_ready = self._on_bridge_ready
            session.on_ability_event = self._on_ability_event

            async def read_messages():
                async for message in ws:
                    if isinstance(message, str):
                        await session.handle_message(message)

            reader_task = asyncio.create_task(read_messages())

            context = await session.get_page_context()
            if context:
                logger.info(
                    "连接上下文 %s: url=%s, imsdk=%s, QN=%s, workbench=%s, qnSdk=%s, ability=%s, _vs=%s",
                    session_id,
                    context.get("href", ""),
                    context.get("hasImsdk", False),
                    context.get("hasQN", False),
                    context.get("hasWorkbench", False),
                    context.get("hasQnSdk", False),
                    context.get("hasAbilityNotify", False),
                    context.get("hasVs", False),
                )

            # 获取卖家信息 — 复刻 MyWebSocketServer 的初始化流程
            nick = await session.get_current_user()
            if nick:
                self.sellers[nick] = session
                logger.info(f"卖家已连接: {nick}")
                if self.on_seller_connected:
                    await self.on_seller_connected(session)

            await reader_task

        except websockets.ConnectionClosed:
            logger.info(f"连接断开: {session_id}")
        except Exception as e:
            logger.error(f"连接异常: {e}")
        finally:
            # 清理 — 复刻 MyWebSocketServer.OnSessionClosed
            self.sessions.pop(session_id, None)
            if session.seller_nick:
                if self.sellers.get(session.seller_nick) is session:
                    self.sellers.pop(session.seller_nick, None)
                if self.on_seller_disconnected:
                    await self.on_seller_disconnected(session)
            logger.info(f"连接已清理: {session_id}")

    # ─── 事件转发 ───

    async def _on_receive_new_msg(self, session: CDPSession, response: str):
        """收到新消息 — 复刻 QN.Cdp_EvRecieveNewMessage"""
        if self.on_message_received:
            await self.on_message_received(session, response)

    async def _on_conversation_change(self, session: CDPSession, response: str):
        """会话切换"""
        logger.debug(f"会话切换: {response[:100]}")
        if self.on_conversation_change:
            await self.on_conversation_change(session, response)

    async def _on_conversation_add(self, session: CDPSession, response: str):
        """新会话"""
        logger.debug(f"新会话: {response[:100]}")

    async def _on_conversation_close(self, session: CDPSession, response: str):
        """会话关闭"""
        logger.debug(f"会话关闭: {response[:100]}")

    async def _on_chat_dlg_active(self, session: CDPSession, response: str):
        """聊天窗口激活"""
        logger.debug(f"聊天窗口激活: {response[:100]}")
        if self.on_chat_dlg_active:
            await self.on_chat_dlg_active(session, response)

    async def _on_message_notify(self, session: CDPSession, response: str):
        """消息中心通知"""
        logger.debug(f"消息通知: {response[:100]}")

    async def _on_shop_robot_receive(self, session: CDPSession, response: str):
        """机器人收到消息"""
        logger.debug(f"机器人收到消息: {response[:100]}")
        if self.on_shop_robot_receive:
            await self.on_shop_robot_receive(session, response)

    async def _on_bridge_ready(self, session: CDPSession, response: str):
        """macOS bridge 已加载。"""
        if self.on_bridge_ready:
            await self.on_bridge_ready(session, response)

    async def _on_ability_event(self, session: CDPSession, response: str):
        """macOS workbench ability 事件。"""
        if self.on_ability_event:
            await self.on_ability_event(session, response)

    def get_session_by_seller(self, seller_nick: str) -> Optional[CDPSession]:
        """根据卖家昵称获取 session"""
        return self.sellers.get(seller_nick)
