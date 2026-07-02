"""
消息模型 — 复刻 openbot ChatResponse.cs + WSocketMessage
解析千牛 WebSocket 消息的 JSON 格式。
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional
import json
import re


# ─── WebSocket 消息 — 复刻 WSocketMessage ───

@dataclass
class WSMessage:
    """WebSocket 消息封装"""
    type: str = ""
    response: Any = ""
    method: str = ""
    expression: str = ""

    @classmethod
    def from_json(cls, data: str | dict) -> WSMessage:
        if isinstance(data, str):
            data = json.loads(data)
        if not isinstance(data, dict):
            data = {}
        return cls(
            type=data.get("type", ""),
            response=data.get("response", ""),
            method=data.get("method", ""),
            expression=data.get("expression", ""),
        )

    def to_json(self) -> str:
        return json.dumps(self.__dict__, ensure_ascii=False)


# ─── 聊天消息 — 复刻 ChatResponse + QNChatMessage ───

@dataclass
class UserId:
    """用户标识"""
    nick: str = ""
    uid: str = ""

    @classmethod
    def from_dict(cls, d: Any) -> UserId:
        if not isinstance(d, dict):
            return cls(nick=str(d or ""), uid="")
        return cls(nick=d.get("nick", ""), uid=d.get("uid", ""))


@dataclass
class OriginalData:
    """原始消息数据"""
    text: str = ""
    item_id: str = ""
    header_summary: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> OriginalData:
        text = ""
        item_id = ""
        header_summary = ""

        if isinstance(d, dict):
            text = d.get("text", "")
            header_summary = d.get("header", {}).get("summary", "") if isinstance(d.get("header"), dict) else ""
            # 提取商品 ID — 复刻 OriginalData.itemId 的正则
            url = d.get("url", "")
            if url:
                match = re.search(r'[?&]id=(\d+)', url)
                if match:
                    item_id = match.group(1)

        return cls(text=text, item_id=item_id, header_summary=header_summary)


@dataclass
class QNChatMessage:
    """千牛聊天消息 — 复刻 QNChatMessage"""
    fromid: UserId = field(default_factory=UserId)
    toid: UserId = field(default_factory=UserId)
    loginid: UserId = field(default_factory=UserId)
    summary: str = ""
    original_data: OriginalData = field(default_factory=OriginalData)
    ccode: str = ""
    msg_id: str = ""
    time: str = ""

    @classmethod
    def from_dict(cls, d: Any) -> QNChatMessage:
        if not isinstance(d, dict):
            d = {}
        return cls(
            fromid=UserId.from_dict(d.get("fromid", {})),
            toid=UserId.from_dict(d.get("toid", {})),
            loginid=UserId.from_dict(d.get("loginid", {})),
            summary=d.get("summary", ""),
            original_data=OriginalData.from_dict(d.get("originalData", {})),
            ccode=d.get("ccode", ""),
            msg_id=d.get("msgid", ""),
            time=d.get("time", ""),
        )

    @property
    def is_buyer_send(self) -> bool:
        """是否是买家发的消息 — 复刻 QNChatMessage.IsBuyerSend"""
        return self.loginid.nick == self.toid.nick

    @property
    def message_text(self) -> str:
        """提取消息文本 — 复刻 QNChatMessage.MessageText"""
        if self.original_data.text:
            return self.original_data.text
        if self.original_data.header_summary:
            return self.original_data.header_summary
        return self.summary

    @property
    def buyer_nick(self) -> str:
        """买家昵称"""
        if self.fromid.nick != self.loginid.nick:
            return self.fromid.nick
        return self.toid.nick

    @property
    def seller_nick(self) -> str:
        """卖家昵称"""
        return self.loginid.nick


@dataclass
class ChatResponse:
    """聊天响应 — 复刻 ChatResponse"""
    code: int = 0
    subcode: int = 0
    result: list[QNChatMessage] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Any) -> ChatResponse:
        if isinstance(d, list):
            d = {"result": d}
        if not isinstance(d, dict):
            d = {}
        result_list = d.get("result", [])
        if isinstance(result_list, dict):
            result_list = result_list.get("msgs", [])
        if isinstance(result_list, str):
            try:
                result_list = json.loads(result_list)
            except json.JSONDecodeError:
                result_list = []
        if isinstance(result_list, dict):
            result_list = result_list.get("msgs", [])
        return cls(
            code=d.get("code", 0),
            subcode=d.get("subcode", 0),
            result=[QNChatMessage.from_dict(m) for m in result_list] if isinstance(result_list, list) else [],
        )

    @classmethod
    def from_json(cls, data: Any) -> ChatResponse:
        if isinstance(data, (dict, list)):
            return cls.from_dict(data)
        return cls.from_dict(json.loads(data))


# ─── 会话变更事件 ───

@dataclass
class ConversationEvent:
    """会话变更事件 — 复刻 onConversationChange 等"""
    login_id: str = ""
    ccode: str = ""
    nick: str = ""
    conversation: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict) -> ConversationEvent:
        conv = d.get("conversation", {})
        login_id_obj = d.get("loginID", {})
        return cls(
            login_id=login_id_obj.get("nick", "") if isinstance(login_id_obj, dict) else str(login_id_obj),
            ccode=conv.get("ccode", ""),
            nick=conv.get("nick", ""),
            conversation=conv,
        )


# ─── 通知消息 ───

@dataclass
class MessageNotify:
    """消息中心通知 — 复刻 MessageNotifyResponse"""
    notify_type: str = ""
    title: str = ""
    content: str = ""
    raw: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict) -> MessageNotify:
        return cls(
            notify_type=d.get("type", ""),
            title=d.get("title", ""),
            content=d.get("content", ""),
            raw=d,
        )
