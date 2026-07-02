import asyncio
import json
import unittest
from types import SimpleNamespace

from message import ChatResponse, WSMessage
from session import SellerSession, SessionManager
from ws_server import CDPSession, _ability_event_name, _decode_response
from config import config


class AsyncRules:
    async def match(self, text):
        return f"reply:{text}"


class AsyncKnowledge:
    async def build_context(self, text, item_id):
        return ""


class AsyncLogger:
    async def log(self, *args):
        return None


class CapturingSellerSession(SellerSession):
    def __init__(self):
        cdp = SimpleNamespace(
            seller_nick="seller",
            is_chat_session=True,
            href="chat",
            has_imsdk=True,
            has_vs=True,
        )
        server = SimpleNamespace(sessions={"chat": cdp})
        super().__init__(cdp, server, AsyncKnowledge(), AsyncRules(), AsyncLogger())
        self.sent = []

    async def _send_reply(self, seller, buyer, text, is_auto=False):
        self.sent.append((seller, buyer, text))


class CoreFlowTests(unittest.TestCase):
    def test_decode_response_accepts_structured_payloads(self):
        payload = {"name": "im.singlemsg.onMsgSendUpdate"}

        self.assertEqual(_decode_response(payload), payload)
        self.assertEqual(_decode_response(json.dumps(payload)), payload)
        self.assertEqual(_decode_response("not-json"), {})

    def test_ws_message_keeps_object_response(self):
        response = {"name": "im.singlemsg.onReceiveNewMsg", "data": "[]"}
        msg = WSMessage.from_json({"type": "rawOnEventNotify", "response": response})

        self.assertEqual(msg.type, "rawOnEventNotify")
        self.assertEqual(msg.response, response)

    def test_chat_response_accepts_result_msgs_shape(self):
        response = {
            "code": 0,
            "result": {
                "msgs": [
                    {
                        "fromid": {"nick": "buyer", "uid": "2043945092"},
                        "toid": {"nick": "seller"},
                        "loginid": {"nick": "seller"},
                        "originalData": {"text": "你好"},
                        "ccode": "2043945092.1-1.1#11001@cntaobao",
                    }
                ]
            },
        }

        chat_resp = ChatResponse.from_dict(response)

        self.assertEqual(len(chat_resp.result), 1)
        self.assertEqual(chat_resp.result[0].message_text, "你好")
        self.assertEqual(chat_resp.result[0].buyer_nick, "buyer")

    def test_raw_event_name_prefers_sid_over_json_name(self):
        response = {
            "sid": "im.singlemsg.onMsgSendUpdate",
            "name": json.dumps([{"originalData": {"text": "已发送文本"}}], ensure_ascii=False),
        }

        self.assertEqual(_ability_event_name(response), "im.singlemsg.onMsgSendUpdate")

    def test_event_name_ignores_json_name_without_sid(self):
        response = {"name": json.dumps([{"cid": {"ccode": "c1"}}], ensure_ascii=False)}

        self.assertEqual(_ability_event_name(response), "")

    def test_raw_conversation_change_is_routed(self):
        async def run():
            cdp = CDPSession("s1", None)
            seen = []

            async def on_change(session, response):
                seen.append((session.session_id, response))

            cdp.on_conversation_change = on_change
            response = {
                "sid": "im.uiutil.onConversationChange",
                "name": json.dumps({"nick": "buyer", "targetId": "t1", "ccode": "c1"}, ensure_ascii=False),
            }

            await cdp.handle_message(WSMessage(type="rawOnEventNotify", response=response).to_json())
            self.assertEqual(len(seen), 1)
            self.assertEqual(seen[0][0], "s1")

        asyncio.run(run())

    def test_raw_receive_new_msg_is_routed_to_native_event(self):
        async def run():
            cdp = CDPSession("s1", None)
            seen = []

            async def on_ability(session, response):
                seen.append((session.session_id, response))

            cdp.on_ability_event = on_ability
            response = {
                "sid": "im.singlemsg.onReceiveNewMsg",
                "name": json.dumps([{"ccode": "c1"}], ensure_ascii=False),
            }

            await cdp.handle_message(WSMessage(type="rawOnEventNotify", response=response).to_json())
            self.assertEqual(len(seen), 1)
            self.assertEqual(seen[0][0], "s1")

        asyncio.run(run())

    def test_chat_response_accepts_dict_payload(self):
        payload = {
            "code": 0,
            "result": [
                {
                    "fromid": {"nick": "buyer", "uid": "u1"},
                    "toid": {"nick": "seller", "uid": "s1"},
                    "loginid": {"nick": "seller", "uid": "s1"},
                    "originalData": {"text": "您好"},
                    "ccode": "c1",
                }
            ],
        }

        parsed = ChatResponse.from_json(payload)

        self.assertEqual(len(parsed.result), 1)
        self.assertTrue(parsed.result[0].is_buyer_send)
        self.assertEqual(parsed.result[0].buyer_nick, "buyer")
        self.assertEqual(parsed.result[0].message_text, "您好")

    def test_extract_sent_texts_accepts_dict_and_string(self):
        events = [{"originalData": {"text": "已发送文本"}}]
        response = {
            "name": json.dumps(events, ensure_ascii=False),
            "sid": "im.singlemsg.onMsgSendUpdate",
        }

        self.assertEqual(CDPSession._extract_sent_texts(response), ["已发送文本"])
        self.assertEqual(CDPSession._extract_sent_texts(json.dumps(response, ensure_ascii=False)), ["已发送文本"])

    def test_send_confirmation_accepts_text_with_duplicate_insert(self):
        async def run():
            cdp = CDPSession("s1", None)
            future = cdp.create_send_confirmation("您好")
            response = {
                "sid": "im.singlemsg.onMsgSendUpdate",
                "name": json.dumps(
                    [{"originalData": {"text": "您好\n您好\n"}, "sendStatus": 0}],
                    ensure_ascii=False,
                ),
            }

            cdp._handle_send_update(response)

            self.assertTrue(await cdp.wait_for_send_confirmation(future, timeout=0.01))

        asyncio.run(run())

    def test_send_confirmation_can_survive_intermediate_timeout(self):
        async def run():
            cdp = CDPSession("s1", None)
            future = cdp.create_send_confirmation("您好")

            self.assertFalse(
                await cdp.wait_for_send_confirmation(
                    future,
                    timeout=0.01,
                    keep_pending_on_timeout=True,
                )
            )

            response = {
                "sid": "im.singlemsg.onMsgSendUpdate",
                "name": json.dumps([{"originalData": {"text": "您好"}, "sendStatus": 0}], ensure_ascii=False),
            }
            cdp._handle_send_update(response)

            self.assertTrue(await cdp.wait_for_send_confirmation(future, timeout=0.01))

        asyncio.run(run())

    def test_seller_disconnect_keeps_session_when_other_page_remains(self):
        old_cdp = SimpleNamespace(seller_nick="seller", session_id="old")
        other_cdp = SimpleNamespace(seller_nick="seller", session_id="other")
        server = SimpleNamespace(sessions={"other": other_cdp})
        manager = SessionManager(server, object(), object(), object())
        manager.sessions["seller"] = SimpleNamespace(cdp=old_cdp)
        manager._current_seller = "seller"

        asyncio.run(manager.on_seller_disconnected(old_cdp))

        self.assertIn("seller", manager.sessions)
        self.assertEqual(manager._current_seller, "seller")

    def test_receive_new_msg_event_tolerates_string_cid(self):
        cdp = SimpleNamespace(seller_nick="seller", is_chat_session=False, href="")
        server = SimpleNamespace(sessions={"chat": cdp})
        manager = SessionManager(server, object(), object(), object())

        response = {
            "name": "im.singlemsg.onReceiveNewMsg",
            "data": json.dumps([{"cid": "not-a-dict"}]),
        }

        asyncio.run(manager.on_native_event(cdp, response))

    def test_repeated_messages_are_replied_once(self):
        async def run():
            old_delay = config.robot.reply_delay
            config.robot.reply_delay = 0.01
            session = CapturingSellerSession()
            try:
                session._queue_auto_reply("seller", "buyer", "您好")
                session._queue_auto_reply("seller", "buyer", "您好")
                session._queue_auto_reply("seller", "buyer", "您好")
                await asyncio.sleep(1.35)
                self.assertEqual(len(session.sent), 1)
                self.assertEqual(session.sent[0], ("seller", "buyer", "reply:您好"))
            finally:
                config.robot.reply_delay = old_delay

        asyncio.run(run())

    def test_message_burst_is_collapsed_to_one_intent(self):
        async def run():
            old_delay = config.robot.reply_delay
            config.robot.reply_delay = 0.01
            session = CapturingSellerSession()
            try:
                session._queue_auto_reply("seller", "buyer", "云冈石窟")
                session._queue_auto_reply("seller", "buyer", "有哪些佛")
                await asyncio.sleep(1.35)
                self.assertEqual(len(session.sent), 1)
                self.assertEqual(session.sent[0][2], "reply:云冈石窟\n有哪些佛")
            finally:
                config.robot.reply_delay = old_delay

        asyncio.run(run())

    def test_buyer_target_is_remembered_from_ccode(self):
        session = CapturingSellerSession()
        session._remember_buyer_target(
            "buyer",
            ccode="3032966192.1-2219383781151.1#11001@cntaobao",
            uid="cntaobaobuyer",
        )

        self.assertEqual(session._buyer_targets["buyer"]["target_id"], "3032966192")
        self.assertEqual(
            session._buyer_targets["buyer"]["ccode"],
            "3032966192.1-2219383781151.1#11001@cntaobao",
        )
        self.assertEqual(session._buyer_targets["buyer"]["uid"], "cntaobaobuyer")

    def test_buyer_target_can_be_resolved_from_ccode_only(self):
        session = CapturingSellerSession()
        ccode = "3032966192.1-2219383781151.1#11001@cntaobao"
        session._remember_buyer_target("buyer", ccode=ccode)

        self.assertEqual(session._target_for_ccode(ccode), ("buyer", "3032966192", ccode))

    def test_open_chat_candidates_tolerate_missing_workbench_flag(self):
        session = CapturingSellerSession()
        chat_cdp = SimpleNamespace(
            seller_nick="seller",
            is_chat_session=True,
            href="https://alires-webui/web_chat-packer/recent.html?debug=true",
            has_imsdk=True,
            has_vs=True,
        )
        session.server.sessions = {"chat": chat_cdp}

        candidates = session._open_chat_cdps()

        self.assertIn(chat_cdp, candidates)

    def test_unread_conversation_change_triggers_page_scan(self):
        async def run():
            session = CapturingSellerSession()
            scans = []

            class ScanCdp:
                async def trigger_page_message_scan(self, reason):
                    scans.append(reason)
                    return True

            async def ready_cdp():
                return ScanCdp()

            session._select_send_cdp_ready = ready_cdp
            response = {
                "name": json.dumps(
                    {
                        "nick": "buyer",
                        "targetId": "3032966192",
                        "ccode": "3032966192.1-2219383781151.1#11001@cntaobao",
                        "unreadcount": 1,
                    },
                    ensure_ascii=False,
                )
            }

            await session.handle_conversation_change(response)
            self.assertEqual(session._current_buyer, "buyer")
            self.assertEqual(scans, ["conversationUnread:buyer:1"])

        asyncio.run(run())

    def test_dom_text_fallback_requires_active_unread_context(self):
        async def run():
            old_delay = config.robot.reply_delay
            config.robot.reply_delay = 0.01
            manager = SessionManager(SimpleNamespace(sessions={}), AsyncKnowledge(), AsyncRules(), AsyncLogger())
            cdp = SimpleNamespace(seller_nick="seller")
            session = CapturingSellerSession()
            session._current_buyer = "buyer"
            session._current_target_id = "3032966192"
            manager.sessions["seller"] = session
            manager._current_seller = "seller"

            try:
                await manager.on_native_event(
                    cdp,
                    {
                        "source": "dom:text",
                        "messageText": "你好",
                        "meta": {"fallback": True, "reason": "conversationUnread:buyer:1:800"},
                    },
                )
                await asyncio.sleep(1.35)
                self.assertEqual(session.sent, [("seller", "buyer", "reply:你好")])

                await manager.on_native_event(
                    cdp,
                    {
                        "source": "dom:text",
                        "messageText": "不要处理",
                        "meta": {"fallback": False, "reason": "timer:3000"},
                    },
                )
                await asyncio.sleep(0.05)
                self.assertEqual(len(session.sent), 1)
            finally:
                config.robot.reply_delay = old_delay

        asyncio.run(run())

    def test_raw_receive_fetches_remote_messages(self):
        async def run():
            old_delay = config.robot.reply_delay
            config.robot.reply_delay = 0.01

            class RemoteCdp:
                seller_nick = "seller"
                is_chat_session = True
                href = "https://alires-webui/web_chat-packer/recent.html?debug=true"

                async def get_remote_messages(self, ccode):
                    return {
                        "code": 0,
                        "result": [
                            {
                                "fromid": {"nick": "buyer", "uid": "2043945092"},
                                "toid": {"nick": "seller"},
                                "loginid": {"nick": "seller"},
                                "originalData": {"text": "新人你好"},
                                "ccode": ccode,
                            }
                        ],
                    }

                async def trigger_page_message_scan(self, reason):
                    return True

            manager = SessionManager(SimpleNamespace(sessions={}), AsyncKnowledge(), AsyncRules(), AsyncLogger())
            session = CapturingSellerSession()
            manager.sessions["seller"] = session
            manager._current_seller = "seller"

            try:
                await manager.on_native_event(
                    RemoteCdp(),
                    {
                        "sid": "im.singlemsg.onReceiveNewMsg",
                        "name": json.dumps(
                            [{"ccode": "2043945092.1-1.1#11001@cntaobao"}],
                            ensure_ascii=False,
                        ),
                    },
                )
                await asyncio.sleep(1.35)
                self.assertEqual(session.sent, [("seller", "buyer", "reply:新人你好")])
            finally:
                config.robot.reply_delay = old_delay

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
