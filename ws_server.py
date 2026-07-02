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


def _port_owner(port: int) -> str:
    try:
        completed = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=2,
            check=False,
        )
        return completed.stdout.strip()
    except Exception:
        return ""


def _decode_response(response: Any) -> Any:
    if isinstance(response, (dict, list)):
        return response
    try:
        return json.loads(response)
    except Exception:
        return {}


def _preview(value: Any, limit: int = 100) -> str:
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, ensure_ascii=False)
        except Exception:
            text = str(value)
    return text[:limit]


def _ability_event_name(response: str) -> str:
    payload = _decode_response(response)
    if isinstance(payload, dict):
        sid = payload.get("sid")
        if sid:
            return str(sid)
        name = payload.get("name")
        if name:
            name_text = str(name)
            if not name_text.lstrip().startswith(("[", "{")):
                return name_text
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
    payload = _decode_response(response)
    if not isinstance(payload, dict):
        return False
    name = str(payload.get("name") or "")
    return name in {
        "im.remoteMessages",
        "im.remoteMessages:empty",
        "im.remoteMessages:error",
        "messageCandidates",
    }


class CDPSession:
    """
    每个千牛浏览器标签页一个 session — 复刻 openbot CDPClient
    封装 WebSocket 连接，提供千牛 API 调用能力。
    """
    _global_send_confirmations: list[tuple[str, asyncio.Future]] = []

    def __init__(self, session_id: str, ws: ServerConnection):
        self.session_id = session_id
        self.ws = ws
        self.seller_nick: str = ""
        self.qn_version: str = ""
        self.href: str = ""
        self.has_imsdk: bool = False
        self.has_vs: bool = False
        self.has_workbench: bool = False
        self.has_qn: bool = False
        self.has_ability: bool = False
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
        if "hasWorkbench" in payload:
            self.has_workbench = bool(payload.get("hasWorkbench"))
        if "hasQN" in payload:
            self.has_qn = bool(payload.get("hasQN"))
        if "hasAbilityNotify" in payload:
            self.has_ability = bool(payload.get("hasAbilityNotify"))

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
                logger.info("%s: %s", msg.type, _preview(msg.response, 8000))
            if self.on_ability_event:
                await self.on_ability_event(self, msg.response)
        elif msg.type == "workbenchProbe":
            self.update_context_from_payload(msg.response)
            if _should_log_probe(msg.response):
                logger.info("workbenchProbe: %s", _preview(msg.response, 8000))
        elif msg.type == "extensionHookSeen":
            logger.debug("extensionHookSeen: %s", _preview(msg.response, 5000))
        elif msg.type == "extensionHookLoaded":
            logger.debug("extensionHookLoaded: %s", _preview(msg.response, 5000))
        elif msg.type == "nativeWangwangEvent":
            logger.debug("nativeWangwangEvent: %s", _preview(msg.response, 8000))
            if self.on_ability_event:
                await self.on_ability_event(self, msg.response)
        elif msg.type == "pageMessageCandidate":
            logger.info("pageMessageCandidate: %s", _preview(msg.response, 3000))
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
                logger.info("rawOnEventNotify: %s", _preview(msg.response, 3000))
            if event_name == "im.singlemsg.onMsgSendUpdate":
                self._handle_send_update(msg.response)
            elif event_name == "im.uiutil.onConversationChange" and self.on_conversation_change:
                await self.on_conversation_change(self, msg.response)
            elif event_name in (
                "im.singlemsg.onReceiveNewMsg",
                "im.singlemsg.onShopRobotReceriveNewMsgs",
            ) and self.on_ability_event:
                await self.on_ability_event(self, msg.response)
        elif msg.type in ("macOnEventNotify", "macReceiveNewMsgNotify", "macShopRobotNewMsgs"):
            logger.debug("%s: %s", msg.type, _preview(msg.response, 8000))
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
        self._global_send_confirmations.append((self._normalize_text(text), future))
        return future

    async def wait_for_send_confirmation(
        self,
        future: asyncio.Future,
        timeout: float = 8.0,
        *,
        keep_pending_on_timeout: bool = False,
    ) -> bool:
        """Wait until onMsgSendUpdate confirms the message was actually sent."""
        try:
            return bool(await asyncio.wait_for(asyncio.shield(future), timeout=timeout))
        except asyncio.TimeoutError:
            timed_out = True
            return False
        finally:
            if not (keep_pending_on_timeout and not future.done() and locals().get("timed_out")):
                self._pending_send_confirmations = [
                    (text, pending)
                    for text, pending in self._pending_send_confirmations
                    if pending is not future
                ]
                self._global_send_confirmations = [
                    (text, pending)
                    for text, pending in self._global_send_confirmations
                    if pending is not future
                ]

    @staticmethod
    def _normalize_text(text: str) -> str:
        return " ".join(str(text or "").split())

    def _handle_send_update(self, response: str):
        pending_confirmations = list(self._pending_send_confirmations) + [
            item for item in self._global_send_confirmations
            if item not in self._pending_send_confirmations
        ]
        if not pending_confirmations:
            return
        sent_texts = self._extract_sent_texts(response)
        if not sent_texts:
            return
        normalized_sent = {self._normalize_text(text) for text in sent_texts}
        for expected, future in pending_confirmations:
            if future.done():
                continue
            if expected in normalized_sent:
                future.set_result(True)
            elif expected and any(expected in sent for sent in normalized_sent):
                logger.warning("发送确认文本包含预期内容但不完全一致，可能发生重复插入: expected=%s", expected[:80])
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
        """获取当前登录用户。mac 优先同步读 _vs，避免 imsdk promise 卡住。"""
        result = await self.invoke(
            "(()=>{"
            "const login=(typeof window._vs!=='undefined'&&window._vs&&window._vs.loginID)||{};"
            "return JSON.parse(JSON.stringify(login||{}));"
            "})()",
            timeout=1.0,
        )
        if result and isinstance(result, dict):
            nick = str(result.get("nick") or result.get("display") or "")
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
            self.has_workbench = bool(result.get("hasWorkbench"))
            self.has_qn = bool(result.get("hasQN"))
            self.has_ability = bool(result.get("hasAbilityNotify"))
            return result
        return {}

    @property
    def is_chat_session(self) -> bool:
        """Only the real chat WebView has imsdk/_vs and can send text reliably."""
        return self.has_imsdk and self.has_vs and "web_chat-packer/recent.html" in self.href

    async def get_version(self) -> str:
        """获取千牛版本。仅诊断用，短超时避免阻塞 mac WebView。"""
        result = await self.invoke(
            "(()=>{"
            "if(typeof imsdk==='undefined'||typeof imsdk.invoke!=='function')return null;"
            "return Promise.race(["
            "imsdk.invoke('application.getVersion'),"
            "new Promise(resolve=>setTimeout(()=>resolve(null),1500))"
            "]);"
            "})()",
            timeout=2.0,
        )
        if result and isinstance(result, dict):
            self.qn_version = result.get("result", {}).get("version", "")
        return self.qn_version

    async def insert_text_to_inputbox(self, uid: str, text: str) -> bool:
        """插入文本到输入框 — 复刻 CDPClient.InsertText2Inputbox"""
        qn_uid = uid if uid.startswith("cntaobao") else f"cntaobao{uid}"
        param = json.dumps({"uid": qn_uid, "text": text}, ensure_ascii=False)
        # mac 千牛某些 WebView 的 execute 响应不稳定；这里不等待返回，只下发插入命令。
        return await self.invoke_no_wait(
            "(()=>{"
            "if(typeof imsdk==='undefined'||typeof imsdk.invoke!=='function')return {ok:false,error:'imsdk unavailable'};"
            f"const param={param};"
            "setTimeout(()=>{try{imsdk.invoke('application.insertText2Inputbox',param);}catch(e){}},0);"
            "})()"
        )

    async def get_current_conversation(self) -> dict:
        """获取当前聊天会话 — 复刻 CDPClient.GetCurrentConversationID"""
        result = await self.invoke(
            "(()=>{"
            "const conv=(typeof window._conversationId!=='undefined'&&window._conversationId)||"
            "(typeof window._vs!=='undefined'&&window._vs&&window._vs.conversationID)||{};"
            "return JSON.parse(JSON.stringify(conv||{}));"
            "})()",
            timeout=1.0,
        )
        if isinstance(result, dict):
            if isinstance(result.get("result"), dict):
                return result["result"]
            return result
        return {}

    async def is_current_conversation(self, buyer: str = "", target_id: str = "", ccode: str = "") -> bool:
        conv = await self.get_current_conversation()
        if not conv:
            return False
        nick = str(conv.get("nick") or conv.get("display") or "")
        cid = str(conv.get("ccode") or "")
        tid = str(conv.get("targetId") or "")
        buyer_plain = buyer.removeprefix("cntaobao") if buyer else ""
        if ccode and cid == ccode:
            return True
        if target_id and tid == target_id:
            return True
        if buyer_plain and (nick == buyer_plain or nick == f"cntaobao{buyer_plain}" or nick.endswith(buyer_plain)):
            return True
        return False

    async def open_chat(self, nick: str) -> bool:
        """打开买家聊天窗口 — 复刻 CDPClient.OpenChat"""
        qn_nick = nick if nick.startswith("cntaobao") else f"cntaobao{nick}"
        param = json.dumps({"nick": qn_nick}, ensure_ascii=False)
        logger.info("openChat(application.openChat): nick=%s", qn_nick)
        return await self.invoke_no_wait(
            "(()=>{"
            "if(typeof imsdk==='undefined'||typeof imsdk.invoke!=='function')return {ok:false,error:'imsdk unavailable'};"
            f"const param={param};"
            "setTimeout(()=>imsdk.invoke('application.openChat',param),0);"
            "})()"
        )

    async def ensure_chat_open(self, buyer: str = "", target_id: str = "", ccode: str = "", attempts: int = 4) -> bool:
        """Open a buyer chat and verify the active conversation before sending."""
        for attempt in range(attempts):
            if await self.is_current_conversation(buyer, target_id, ccode):
                return True
            await self.open_chat_context(buyer, target_id, ccode)
            if buyer:
                await self.open_chat(buyer)
            await asyncio.sleep(0.7 + attempt * 0.35)
            if await self.is_current_conversation(buyer, target_id, ccode):
                return True
            if buyer and attempt >= 1:
                await self.click_conversation_by_name(buyer)
                await asyncio.sleep(0.5)
                if await self.is_current_conversation(buyer, target_id, ccode):
                    return True
        return False

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
        logger.info(
            "openChat(qn.openChat): nick=%s uid=%s cid=%s targetId=%s",
            params.get("nick", ""),
            params.get("uid", ""),
            params.get("cid", ""),
            params.get("targetId", ""),
        )
        return await self.invoke_no_wait(
            "(()=>{"
            f"const p={payload};"
            "setTimeout(()=>{"
            "try{if(typeof imsdk!=='undefined'&&imsdk.invoke&&p.nick)imsdk.invoke('application.openChat',{nick:p.nick});}catch(e){}"
            "try{if(typeof imsdk!=='undefined'&&imsdk.invoke&&p.uid&&p.uid!==p.nick)imsdk.invoke('application.openChat',{nick:p.uid});}catch(e){}"
            "try{if(typeof workbench!=='undefined'&&workbench.application&&workbench.application.invoke)workbench.application.invoke('qn.openChat',p);}catch(e){}"
            "try{if(typeof workbench!=='undefined'&&workbench.application&&workbench.application.invoke&&p.cid)workbench.application.invoke('qn.openChat',{cid:p.cid,bizDomain:p.bizDomain});}catch(e){}"
            "try{if(typeof workbench!=='undefined'&&workbench.application&&workbench.application.invoke&&p.targetId)workbench.application.invoke('qn.openChat',{targetId:p.targetId,bizDomain:p.bizDomain});}catch(e){}"
            "try{if(typeof workbench!=='undefined'&&workbench.wangwang&&workbench.wangwang.invoke)workbench.wangwang.invoke('qn.openChat',p);}catch(e){}"
            "},0);"
            "})()"
        )

    async def dom_click_conversation_by_name(self, buyer: str) -> bool:
        """Click a buyer row inside the chat WebView DOM when macOS AX cannot see the real list item."""
        if not buyer:
            return False
        buyer_text = json.dumps(buyer.removeprefix("cntaobao"), ensure_ascii=False)
        return await self.invoke_no_wait(
            r'''(()=>{
const buyer=''' + buyer_text + r''';
setTimeout(()=>{
  try{
    const vw=Math.max(document.documentElement.clientWidth||0, window.innerWidth||0);
    const vh=Math.max(document.documentElement.clientHeight||0, window.innerHeight||0);
    const visible=(el)=>{
      const r=el.getBoundingClientRect();
      const s=getComputedStyle(el);
      return r.width>0&&r.height>0&&s.display!=='none'&&s.visibility!=='hidden'&&r.left>=0&&r.top>=0&&r.top<vh;
    };
    const textOf=(el)=>((el.innerText||el.textContent||el.getAttribute('title')||el.getAttribute('aria-label')||'')+'').trim();
    const rows=[...document.querySelectorAll('li,[role="listitem"],[role="treeitem"],[role="option"],.conversation,.conversation-item,.session,.session-item,div,span')]
      .filter(visible)
      .map(el=>({el,r:el.getBoundingClientRect(),text:textOf(el)}))
      .filter(x=>x.text&&x.text.includes(buyer)&&x.r.left < vw*0.45&&x.r.top>35&&x.r.height>=12&&x.r.width>=30)
      .sort((a,b)=>(b.r.width*b.r.height)-(a.r.width*a.r.height));
    const item=rows[0];
    if(!item)return;
    const target=item.el.closest('[role="listitem"],[role="treeitem"],[role="option"],li,.conversation,.conversation-item,.session,.session-item')||item.el;
    target.dispatchEvent(new MouseEvent('mousedown',{bubbles:true,cancelable:true,view:window}));
    target.dispatchEvent(new MouseEvent('mouseup',{bubbles:true,cancelable:true,view:window}));
    target.click();
  }catch(e){}
},0);
})()'''
        )

    async def dom_fill_inputbox(self, text: str) -> bool:
        """Fill the bottom chat editor from the WebView DOM when application.insertText2Inputbox stalls."""
        payload = json.dumps(text, ensure_ascii=False)
        return await self.invoke_no_wait(
            r'''(()=>{
const replyText=''' + payload + r''';
setTimeout(()=>{
  try{
    const vw=Math.max(document.documentElement.clientWidth||0, window.innerWidth||0);
    const vh=Math.max(document.documentElement.clientHeight||0, window.innerHeight||0);
    const visible=(el)=>{
      const r=el.getBoundingClientRect();
      const s=getComputedStyle(el);
      return r.width>0&&r.height>0&&s.display!=='none'&&s.visibility!=='hidden'&&r.left>=vw*0.28&&r.top>=vh*0.55;
    };
    const editors=[...document.querySelectorAll('textarea,input[type="text"],[contenteditable="true"],[role="textbox"]')]
      .filter(visible)
      .sort((a,b)=>b.getBoundingClientRect().top-a.getBoundingClientRect().top);
    const el=editors[0];
    if(!el)return;
    el.focus();
    if('value' in el){
      el.value=replyText;
      el.dispatchEvent(new Event('input',{bubbles:true}));
      el.dispatchEvent(new Event('change',{bubbles:true}));
    }else{
      el.textContent=replyText;
      el.dispatchEvent(new InputEvent('input',{bubbles:true,inputType:'insertText',data:replyText}));
      el.dispatchEvent(new Event('change',{bubbles:true}));
    }
  }catch(e){}
},0);
})()'''
        )

    async def click_conversation_by_name(self, buyer: str) -> bool:
        """Fallback: click the buyer in Qianniu's conversation list via macOS Accessibility."""
        if not buyer:
            return False
        buyer_text = json.dumps(buyer.removeprefix("cntaobao"), ensure_ascii=False)
        script = f'''
set buyerName to {buyer_text}

on textMatchesBuyer(v, buyerName)
    try
        if v is missing value then return false
        set s to v as text
        if s is buyerName then return true
        if s contains buyerName then return true
    end try
    return false
end textMatchesBuyer

on elementCenter(el)
    tell application "System Events"
        try
            set p to position of el
            set s to size of el
            set x to (item 1 of p) + ((item 1 of s) / 2)
            set y to (item 2 of p) + ((item 2 of s) / 2)
            return {{x, y}}
        end try
    end tell
    return {{0, 0}}
end elementCenter

on pointLooksLikeConversationRow(pointValue, winLeft, winTop, winWidth, winHeight)
    try
        set x to item 1 of pointValue
        set y to item 2 of pointValue
        if x < winLeft then return false
        if y < (winTop + 45) then return false
        if y > (winTop + winHeight - 80) then return false
        if x > (winLeft + (winWidth * 0.42)) then return false
        return true
    end try
    return false
end pointLooksLikeConversationRow

on clickElementCenter(el)
    tell application "System Events"
        try
            set p to position of el
            set s to size of el
            set x to (item 1 of p) + ((item 1 of s) / 2)
            set y to (item 2 of p) + ((item 2 of s) / 2)
            click at {{x, y}}
            delay 0.08
            click at {{x, y}}
            return true
        end try
        try
            click el
            delay 0.08
            click el
            return true
        end try
    end tell
    return false
end clickElementCenter

on clickBuyerElement(rootElement, depth, buyerName, winLeft, winTop, winWidth, winHeight)
    if depth > 8 then return false
    tell application "System Events"
        try
            if my textMatchesBuyer(name of rootElement, buyerName) then
                set centerPoint to my elementCenter(rootElement)
                if my pointLooksLikeConversationRow(centerPoint, winLeft, winTop, winWidth, winHeight) then
                    if my clickElementCenter(rootElement) then return true
                end if
            end if
        end try
        try
            if my textMatchesBuyer(value of rootElement, buyerName) then
                set centerPoint to my elementCenter(rootElement)
                if my pointLooksLikeConversationRow(centerPoint, winLeft, winTop, winWidth, winHeight) then
                    if my clickElementCenter(rootElement) then return true
                end if
            end if
        end try
        try
            repeat with childElement in UI elements of rootElement
                if my clickBuyerElement(childElement, depth + 1, buyerName, winLeft, winTop, winWidth, winHeight) then return true
            end repeat
        end try
    end tell
    return false
end clickBuyerElement

tell application "System Events"
    set targetProcesses to {{"AliWorkbench", "千牛", "Aliworkbench", "Qianniu"}}
    repeat with processName in targetProcesses
        if exists process processName then
            tell process processName
                set frontmost to true
                delay 0.15
                if (count of windows) > 0 then
                    repeat with win in windows
                        try
                            set wp to position of win
                            set ws to size of win
                            set winLeft to item 1 of wp
                            set winTop to item 2 of wp
                            set winWidth to item 1 of ws
                            set winHeight to item 2 of ws
                            if my clickBuyerElement(win, 0, buyerName, winLeft, winTop, winWidth, winHeight) then return "clicked_left_list"
                        end try
                    end repeat
                end if
            end tell
        end if
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
                    timeout=5,
                    check=False,
                )
                if completed.stdout.strip().startswith("clicked"):
                    logger.info("无障碍已点击左侧会话候选: %s result=%s", buyer, completed.stdout.strip())
                    return True
                if completed.stderr:
                    logger.info("点击买家会话失败: %s", completed.stderr.strip())
                logger.info("无障碍未找到买家会话: %s, stdout=%s", buyer, completed.stdout.strip())
                return False
            except Exception as e:
                logger.info("点击买家会话异常: %s", e)
                return False

        return await asyncio.to_thread(_run)

    async def trigger_page_message_scan(self, reason: str = "manual") -> bool:
        payload = json.dumps(reason, ensure_ascii=False)
        return await self.invoke_no_wait(
            "(()=>{"
            f"const reason={payload};"
            "[200,800,1600,3000].forEach((delay)=>setTimeout(()=>{"
            "try{if(window.__openbotScanPageMessages)window.__openbotScanPageMessages(reason+':'+delay);}catch(e){}"
            "},delay));"
            "})()"
        )

    async def paste_text_to_inputbox(self, text: str) -> bool:
        """Paste text into Qianniu's focused chat input via macOS Accessibility."""
        escaped = json.dumps(text, ensure_ascii=False)
        script = f'''
set replyText to {escaped}
set the clipboard to replyText
delay 0.15
set didAttemptPaste to false

on clipboardLooksReady(replyText)
    try
        set clipText to the clipboard as text
        if clipText is replyText then return true
    end try
    return false
end clipboardLooksReady

on pointLooksLikeEditor(pointValue, winLeft, winTop, winWidth, winHeight)
    try
        set x to item 1 of pointValue
        set y to item 2 of pointValue
        if x < (winLeft + (winWidth * 0.30)) then return false
        if x > (winLeft + winWidth - 35) then return false
        if y < (winTop + (winHeight * 0.55)) then return false
        if y > (winTop + winHeight - 35) then return false
        return true
    end try
    return false
end pointLooksLikeEditor

on elementCenter(el)
    tell application "System Events"
        try
            set p to position of el
            set s to size of el
            set x to (item 1 of p) + ((item 1 of s) / 2)
            set y to (item 2 of p) + ((item 2 of s) / 2)
            return {{x, y}}
        end try
    end tell
    return {{0, 0}}
end elementCenter

on elementLooksLikeEditor(el)
    tell application "System Events"
        try
            set r to role of el as text
            if r contains "TextArea" then return true
            if r contains "TextField" then return true
            if r contains "AXTextArea" then return true
            if r contains "AXTextField" then return true
        end try
        try
            set d to description of el as text
            if d contains "输入" then return true
            if d contains "编辑" then return true
            if d contains "文本" then return true
        end try
        try
            set n to name of el as text
            if n contains "输入" then return true
            if n contains "编辑" then return true
        end try
    end tell
    return false
end elementLooksLikeEditor

on pasteIntoFocused(replyText)
    tell application "System Events"
        set the clipboard to replyText
        delay 0.15
        if not my clipboardLooksReady(replyText) then return "clipboard_empty"
        keystroke "a" using command down
        delay 0.08
        key code 51
        delay 0.08
        set the clipboard to replyText
        delay 0.15
        keystroke "v" using command down
        delay 0.25
    end tell
    return "pasted_attempted"
end pasteIntoFocused

on pasteIntoEditorElement(rootElement, depth, replyText, winLeft, winTop, winWidth, winHeight)
    if depth > 9 then return "not_found"
    tell application "System Events"
        try
            if my elementLooksLikeEditor(rootElement) then
                set centerPoint to my elementCenter(rootElement)
                if my pointLooksLikeEditor(centerPoint, winLeft, winTop, winWidth, winHeight) then
                    click at centerPoint
                    delay 0.15
                    set pasteResult to my pasteIntoFocused(replyText)
                    if pasteResult is not "not_found" then return "pasted_ax_editor"
                end if
            end if
        end try
        try
            repeat with childElement in UI elements of rootElement
                set childResult to my pasteIntoEditorElement(childElement, depth + 1, replyText, winLeft, winTop, winWidth, winHeight)
                if childResult is not "not_found" then return childResult
            end repeat
        end try
    end tell
    return "not_found"
end pasteIntoEditorElement

tell application "System Events"
    set targetProcesses to {{"AliWorkbench", "千牛", "Aliworkbench", "Qianniu"}}
    repeat with processName in targetProcesses
        if exists process processName then
            tell process processName
                set frontmost to true
                delay 0.1
                repeat with win in windows
                    try
                        set p to position of win
                        set s to size of win
                        set baseX to item 1 of p
                        set baseY to item 2 of p
                        set widthValue to item 1 of s
                        set heightValue to item 2 of s
                        set editorResult to my pasteIntoEditorElement(win, 0, replyText, baseX, baseY, widthValue, heightValue)
                        if editorResult starts with "pasted" then return editorResult
                        set y1 to baseY + heightValue - 96
                        set y2 to baseY + heightValue - 126
                        set y3 to baseY + heightValue - 156
                        set pointsToTry to {{{{baseX + (widthValue * 0.50), y1}}, {{baseX + (widthValue * 0.42), y1}}, {{baseX + (widthValue * 0.60), y1}}, {{baseX + (widthValue * 0.50), y2}}, {{baseX + (widthValue * 0.72), y2}}, {{baseX + (widthValue * 0.50), y3}}}}
                        repeat with pointValue in pointsToTry
                            set didAttemptPaste to true
                            click at pointValue
                            delay 0.15
                            set pasteResult to my pasteIntoFocused(replyText)
                            if pasteResult starts with "pasted" then return "pasted_point"
                            if pasteResult is "clipboard_empty" then return pasteResult
                        end repeat
                    end try
                end repeat
            end tell
        end if
    end repeat
end tell

if didAttemptPaste then return "pasted_attempted"
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
                stdout = completed.stdout.strip()
                if stdout.startswith("pasted"):
                    logger.info("粘贴回复已执行: %s", stdout)
                    return True
                if stdout == "clipboard_empty":
                    logger.warning("粘贴回复失败，剪贴板未写入")
                    return False
                if completed.stderr:
                    logger.info("粘贴回复失败: %s", completed.stderr.strip())
                logger.info("粘贴回复未找到输入框: stdout=%s stderr=%s", stdout, completed.stderr.strip())
                return False
            except Exception as e:
                logger.info("粘贴回复异常: %s", e)
                return False

        return await asyncio.to_thread(_run)

    async def click_send_button(self) -> bool:
        """Click Qianniu's Send button via macOS Accessibility, following QNRpa's idea but not its Windows UIA classes."""
        script = r'''
on textContainsSend(v)
    try
        if v is missing value then return false
        set s to v as text
        if s contains "发送" then return true
        if s contains "Send" then return true
    end try
    return false
end textContainsSend

on elementLooksLikeSend(el)
    tell application "System Events"
        try
            if my textContainsSend(name of el) then return true
        end try
        try
            if my textContainsSend(description of el) then return true
        end try
        try
            if my textContainsSend(value of el) then return true
        end try
        try
            if my textContainsSend(help of el) then return true
        end try
        try
            if my textContainsSend(title of el) then return true
        end try
    end tell
    return false
end elementLooksLikeSend

on clickElementCenter(el)
    tell application "System Events"
        try
            click el
            return true
        end try
        try
            set p to position of el
            set s to size of el
            set x to (item 1 of p) + ((item 1 of s) / 2)
            set y to (item 2 of p) + ((item 2 of s) / 2)
            click at {x, y}
            return true
        end try
    end tell
    return false
end clickElementCenter

on clickSendElement(rootElement, depth)
    if depth > 8 then return false
    tell application "System Events"
        try
            if my elementLooksLikeSend(rootElement) then
                if my clickElementCenter(rootElement) then return true
            end if
        end try
        try
            repeat with childElement in UI elements of rootElement
                if my clickSendElement(childElement, depth + 1) then return true
            end repeat
        end try
    end tell
    return false
end clickSendElement

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
                repeat with win in windows
                    if my clickSendElement(win, 0) then return "clicked_ax"
                end repeat
                repeat with win in windows
                    if my clickWindowSendArea(win) then return true
                end repeat
            end tell
        end if
    end tell
    return ""
end clickSendInProcess

tell application "System Events"
    set targetProcesses to {"AliWorkbench", "千牛", "Aliworkbench", "Qianniu"}
    repeat with processName in targetProcesses
        set clickResult to my clickSendInProcess(processName)
        if clickResult is "clicked_ax" then return "clicked_ax"
        if clickResult is true then return "clicked_area"
    end repeat
end tell
return "not_found"
'''
        area_script = r'''
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
                delay 0.1
            end repeat
            return true
        end try
    end tell
    return false
end clickWindowSendArea

tell application "System Events"
    set targetProcesses to {"AliWorkbench", "千牛", "Aliworkbench", "Qianniu"}
    repeat with processName in targetProcesses
        if exists process processName then
            tell process processName
                set frontmost to true
                delay 0.1
                repeat with win in windows
                    if my clickWindowSendArea(win) then return "clicked_area"
                end repeat
            end tell
        end if
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
                    timeout=2,
                    check=False,
                )
                if completed.stdout.strip().startswith("clicked"):
                    logger.info("点击发送按钮结果: %s", completed.stdout.strip())
                    return True
                if completed.stderr:
                    logger.info("点击发送按钮失败: %s", completed.stderr.strip())
                logger.info("点击发送按钮未找到控件: stdout=%s stderr=%s", completed.stdout.strip(), completed.stderr.strip())
                return False
            except subprocess.TimeoutExpired:
                logger.info("明确发送按钮查找超时，尝试右下角发送区兜底")
                try:
                    completed = subprocess.run(
                        ["osascript", "-e", area_script],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        timeout=2,
                        check=False,
                    )
                    if completed.stdout.strip().startswith("clicked"):
                        logger.info("点击发送按钮结果: %s", completed.stdout.strip())
                        return True
                    logger.info("发送区兜底未命中: stdout=%s stderr=%s", completed.stdout.strip(), completed.stderr.strip())
                    return False
                except Exception as e:
                    logger.info("发送区兜底异常: %s", e)
                    return False
            except Exception as e:
                logger.info("点击发送按钮异常: %s", e)
                return False

        return await asyncio.to_thread(_run)

    async def dom_click_send_button(self) -> bool:
        """Try clicking a DOM send button inside the chat WebView."""
        return await self.invoke_no_wait(
            r'''(()=>{
setTimeout(()=>{
  try{
    const isVisible=(el)=>{
      const r=el.getBoundingClientRect();
      const s=getComputedStyle(el);
      return r.width>0&&r.height>0&&s.visibility!=='hidden'&&s.display!=='none';
    };
    const candidates=[...document.querySelectorAll('button,[role="button"],a,div,span')].filter(el=>{
      const text=(el.innerText||el.textContent||el.getAttribute('aria-label')||el.getAttribute('title')||'').trim();
      return text==='发送'||text==='Send'||text.endsWith('发送');
    }).filter(isVisible);
    const el=candidates[candidates.length-1];
    if(el){
      el.dispatchEvent(new MouseEvent('mousedown',{bubbles:true,cancelable:true,view:window}));
      el.dispatchEvent(new MouseEvent('mouseup',{bubbles:true,cancelable:true,view:window}));
      el.click();
    }
  }catch(e){}
},0);
})()'''
        )

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
            delay 0.35
            key code 76
            delay 0.35
            key code 36 using command down
            delay 0.35
            key code 76 using command down
            return "pressed"
        end if
    end repeat
    key code 36
    delay 0.35
    key code 76
    delay 0.35
    key code 36 using command down
    delay 0.35
    key code 76 using command down
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

    async def log_accessibility_snapshot(self):
        """Log a small Accessibility snapshot near the bottom of Qianniu windows for send-button debugging."""
        script = r'''
on describeElement(el)
    tell application "System Events"
        set out to ""
        try
            set out to out & "role=" & (role of el as text) & " "
        end try
        try
            set out to out & "name=" & (name of el as text) & " "
        end try
        try
            set out to out & "desc=" & (description of el as text) & " "
        end try
        try
            set p to position of el
            set s to size of el
            set out to out & "pos=" & (item 1 of p as text) & "," & (item 2 of p as text) & " size=" & (item 1 of s as text) & "," & (item 2 of s as text)
        end try
        return out
    end tell
end describeElement

on collectElements(rootElement, depth)
    if depth > 5 then return ""
    set output to ""
    tell application "System Events"
        try
            set info to my describeElement(rootElement)
            if info contains "发送" or info contains "Send" or info contains "button" or info contains "AXButton" or info contains "text" or info contains "Text" or info contains "edit" or info contains "输入" or info contains "pz" or info contains "傲娇" then
                set output to output & info & linefeed
            end if
        end try
        try
            repeat with childElement in UI elements of rootElement
                set output to output & my collectElements(childElement, depth + 1)
                if (count paragraphs of output) > 80 then return output
            end repeat
        end try
    end tell
    return output
end collectElements

tell application "System Events"
    set targetProcesses to {"AliWorkbench", "千牛", "Aliworkbench", "Qianniu"}
    repeat with processName in targetProcesses
        if exists process processName then
            tell process processName
                set output to ""
                repeat with win in windows
                    set output to output & my collectElements(win, 0)
                    if (count paragraphs of output) > 80 then return output
                end repeat
                if output is not "" then return output
            end tell
        end if
    end repeat
end tell
return ""
'''

        def _run() -> str:
            try:
                completed = subprocess.run(
                    ["osascript", "-e", script],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=6,
                    check=False,
                )
                return completed.stdout.strip()
            except Exception:
                return ""

        snapshot = await asyncio.to_thread(_run)
        if snapshot:
            logger.info("发送按钮无障碍快照:\n%s", snapshot[:4000])

    async def send_timi_msg(self, user_id: str, text: str) -> bool:
        """发送智能提示消息（旧版千牛）。mac 新链路不依赖它，避免等待返回。"""
        param = json.dumps({"userId": user_id, "smartTip": text}, ensure_ascii=False)
        return await self.invoke_no_wait(
            "(()=>{"
            "try{if(typeof imsdk!=='undefined'&&imsdk.invoke)"
            f"imsdk.invoke('intelligentservice.SendSmartTipMsg', {param});"
            "}catch(e){}"
            "})()"
        )

    async def get_buyer_info(self, nick: str) -> Optional[dict]:
        """获取买家信息。诊断接口，短超时避免阻塞。"""
        result = await self.invoke(
            "(()=>{"
            "if(typeof imsdk==='undefined'||typeof imsdk.invoke!=='function')return null;"
            f"const p={{nick:{json.dumps(nick, ensure_ascii=False)}}};"
            "return Promise.race(["
            "imsdk.invoke('mtop.taobao.znkf.seller.api.getBuyerInfo', p),"
            "new Promise(resolve=>setTimeout(()=>resolve(null),1500))"
            "]);"
            "})()",
            timeout=2.0,
        )
        return result

    async def get_item_records(self, uid: str) -> Optional[dict]:
        """获取买家咨询的商品。诊断接口，短超时避免阻塞。"""
        result = await self.invoke(
            "(()=>{"
            "if(typeof imsdk==='undefined'||typeof imsdk.invoke!=='function')return null;"
            f"const p={{uid:{json.dumps(uid, ensure_ascii=False)}}};"
            "return Promise.race(["
            "imsdk.invoke('mtop.taobao.znkf.im.api.getItemRecords', p),"
            "new Promise(resolve=>setTimeout(()=>resolve(null),1500))"
            "]);"
            "})()",
            timeout=2.0,
        )
        return result

    # ─── 底层发送 ───

    async def _send(self, data: str):
        """发送原始数据"""
        if getattr(self.ws, "state", None) is not None and str(self.ws.state).endswith("CLOSED"):
            logger.debug("跳过已关闭连接发送: %s", self.session_id)
            return
        try:
            await self.ws.send(data)
        except Exception as e:
            logger.debug(f"发送失败: {e}")


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
        try:
            self._server = await serve(
                self._handle_connection,
                WS_HOST,
                WS_PORT,
                ping_interval=10,
                ping_timeout=10,
                close_timeout=2,
            )
        except OSError as e:
            if getattr(e, "errno", None) == 48:
                owner = _port_owner(WS_PORT)
                if owner:
                    logger.error("端口 %s 已被占用，已有机器人实例可能正在运行:\n%s", WS_PORT, owner)
                else:
                    logger.error("端口 %s 已被占用，已有机器人实例可能正在运行", WS_PORT)
                logger.error("请先关闭旧实例，或执行: lsof -nP -iTCP:%s -sTCP:LISTEN 后 kill 对应 PID", WS_PORT)
            raise
        logger.info(f"WebSocket 服务器已启动: ws://{WS_HOST}:{WS_PORT}")

    async def stop(self):
        """停止服务器"""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
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
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=120)
                    except asyncio.TimeoutError:
                        logger.debug("连接空闲超时，主动关闭: %s", session_id)
                        await ws.close()
                        break
                    if isinstance(message, str):
                        await session.handle_message(message)

            reader_task = asyncio.create_task(read_messages())

            await reader_task

        except websockets.ConnectionClosed:
            logger.info(f"连接断开: {session_id}")
        except Exception as e:
            logger.error(f"连接异常: {e}")
        finally:
            # 清理 — 复刻 MyWebSocketServer.OnSessionClosed
            self.sessions.pop(session_id, None)
            if session.seller_nick:
                seller_still_connected = any(
                    other.seller_nick == session.seller_nick
                    for other in self.sessions.values()
                )
                if self.sellers.get(session.seller_nick) is session:
                    replacement = next(
                        (
                            other for other in self.sessions.values()
                            if other.seller_nick == session.seller_nick and other.is_chat_session
                        ),
                        None,
                    ) or next(
                        (
                            other for other in self.sessions.values()
                            if other.seller_nick == session.seller_nick
                        ),
                        None,
                    )
                    if replacement:
                        self.sellers[session.seller_nick] = replacement
                    else:
                        self.sellers.pop(session.seller_nick, None)
                if self.on_seller_disconnected and not seller_still_connected:
                    await self.on_seller_disconnected(session)
            logger.info(f"连接已清理: {session_id}")

    # ─── 事件转发 ───

    async def _on_receive_new_msg(self, session: CDPSession, response: str):
        """收到新消息 — 复刻 QN.Cdp_EvRecieveNewMessage"""
        if self.on_message_received:
            await self.on_message_received(session, response)

    async def _on_conversation_change(self, session: CDPSession, response: str):
        """会话切换"""
        logger.debug("会话切换: %s", _preview(response, 100))
        if self.on_conversation_change:
            await self.on_conversation_change(session, response)

    async def _on_conversation_add(self, session: CDPSession, response: str):
        """新会话"""
        logger.debug("新会话: %s", _preview(response, 100))

    async def _on_conversation_close(self, session: CDPSession, response: str):
        """会话关闭"""
        logger.debug("会话关闭: %s", _preview(response, 100))

    async def _on_chat_dlg_active(self, session: CDPSession, response: str):
        """聊天窗口激活"""
        logger.debug("聊天窗口激活: %s", _preview(response, 100))
        if self.on_chat_dlg_active:
            await self.on_chat_dlg_active(session, response)

    async def _on_message_notify(self, session: CDPSession, response: str):
        """消息中心通知"""
        logger.debug("消息通知: %s", _preview(response, 100))

    async def _on_shop_robot_receive(self, session: CDPSession, response: str):
        """机器人收到消息"""
        logger.debug("机器人收到消息: %s", _preview(response, 100))
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
