"""
SQLite 数据库层 — 复刻 openbot PersistentParams + DbHelper + SQLiteHelper
使用 aiosqlite 实现异步操作。
"""

import aiosqlite
import json
import time
import os
from typing import Any, Optional

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


class DB:
    """异步 SQLite 数据库 — 复刻 openbot 的 SQLiteHelper + PersistentParams"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.path.join(DB_DIR, "bot.db")
        self._conn: Optional[aiosqlite.Connection] = None
        self._cache: dict[str, Any] = {}  # 内存缓存，复刻 PersistentParams._cache

    async def connect(self):
        """连接数据库并初始化表"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._create_tables()

    async def close(self):
        """关闭连接"""
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def _create_tables(self):
        """创建所有表 — 复刻 openbot 的表结构"""
        await self._conn.executescript("""
            -- 参数表 — 复刻 PersistentParams.ParamItem
            CREATE TABLE IF NOT EXISTS ParamItem (
                Key TEXT PRIMARY KEY,
                Value TEXT
            );

            -- 聊天记录表 — 复刻 ChatlogEntity
            CREATE TABLE IF NOT EXISTS ChatlogEntity (
                EntityId TEXT PRIMARY KEY,
                ModifyTick INTEGER NOT NULL DEFAULT 0,
                SellerNick TEXT NOT NULL DEFAULT '',
                FromNick TEXT NOT NULL DEFAULT '',
                ToNick TEXT NOT NULL DEFAULT '',
                Content TEXT NOT NULL DEFAULT '',
                ItemId TEXT NOT NULL DEFAULT '',
                ImageUrl TEXT NOT NULL DEFAULT '',
                Time TEXT NOT NULL DEFAULT '',
                SendTime TEXT NOT NULL DEFAULT '',
                IsDeleted INTEGER NOT NULL DEFAULT 0
            );

            -- 商品知识库 — 复刻 GoodsKnowledgeEntity
            CREATE TABLE IF NOT EXISTS GoodsKnowledgeEntity (
                EntityId TEXT PRIMARY KEY,
                ModifyTick INTEGER NOT NULL DEFAULT 0,
                NumIid INTEGER NOT NULL DEFAULT 0,
                Title TEXT NOT NULL DEFAULT '',
                Content TEXT NOT NULL DEFAULT '',
                ImgFileName TEXT NOT NULL DEFAULT '',
                IsDeleted INTEGER NOT NULL DEFAULT 0
            );

            -- 机器人规则 — 复刻 RobotRuleEntity
            CREATE TABLE IF NOT EXISTS RobotRuleEntity (
                EntityId TEXT PRIMARY KEY,
                ModifyTick INTEGER NOT NULL DEFAULT 0,
                CatalogId TEXT NOT NULL DEFAULT '',
                Question TEXT NOT NULL DEFAULT '',
                Answer TEXT NOT NULL DEFAULT '',
                PatternsJson TEXT NOT NULL DEFAULT '[]',
                AnswersJson TEXT NOT NULL DEFAULT '[]',
                IsDeleted INTEGER NOT NULL DEFAULT 0
            );

            -- 买家备注 — 复刻 BuyerNoteEntity
            CREATE TABLE IF NOT EXISTS BuyerNoteEntity (
                EntityId TEXT PRIMARY KEY,
                ModifyTick INTEGER NOT NULL DEFAULT 0,
                BuyerMainNick TEXT NOT NULL DEFAULT '',
                Note TEXT NOT NULL DEFAULT '',
                Recorder TEXT NOT NULL DEFAULT '',
                RecordTime TEXT NOT NULL DEFAULT '',
                IsDeleted INTEGER NOT NULL DEFAULT 0
            );
        """)
        await self._conn.commit()

    # ─── 参数读写 — 复刻 PersistentParams ───

    async def get_param(self, key: str, default: Any = None) -> Any:
        """读取参数，带内存缓存"""
        if key in self._cache:
            return self._cache[key]

        cursor = await self._conn.execute(
            "SELECT Value FROM ParamItem WHERE Key = ?", (key,)
        )
        row = await cursor.fetchone()
        if row is None:
            return default

        value = self._deserialize(row[0], default)
        self._cache[key] = value
        return value

    async def set_param(self, key: str, value: Any):
        """写入参数"""
        serialized = self._serialize(value)
        await self._conn.execute(
            "INSERT OR REPLACE INTO ParamItem (Key, Value) VALUES (?, ?)",
            (key, serialized)
        )
        await self._conn.commit()
        self._cache[key] = value

    async def delete_param(self, key: str):
        """删除参数"""
        await self._conn.execute("DELETE FROM ParamItem WHERE Key = ?", (key,))
        await self._conn.commit()
        self._cache.pop(key, None)

    # ─── 聊天记录 — 复刻 ChatlogEntity ───

    async def save_chat_log(self, seller: str, from_nick: str, to_nick: str,
                            content: str, item_id: str = "", image_url: str = ""):
        """保存聊天记录"""
        entity_id = f"{seller}_{from_nick}_{to_nick}_{int(time.time() * 1000)}"
        now = int(time.time() * 10000000)  # tick，复刻 C# DateTime.Now.Ticks
        await self._conn.execute(
            """INSERT OR REPLACE INTO ChatlogEntity
               (EntityId, ModifyTick, SellerNick, FromNick, ToNick, Content,
                ItemId, ImageUrl, Time, SendTime)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (entity_id, now, seller, from_nick, to_nick, content,
             item_id, image_url, str(int(time.time())), str(int(time.time())))
        )
        await self._conn.commit()

    async def get_chat_history(self, seller: str, buyer: str, limit: int = 20) -> list[dict]:
        """获取聊天历史"""
        cursor = await self._conn.execute(
            """SELECT FromNick, ToNick, Content, Time FROM ChatlogEntity
               WHERE SellerNick = ? AND (FromNick = ? OR ToNick = ?)
               AND IsDeleted = 0
               ORDER BY ModifyTick DESC LIMIT ?""",
            (seller, buyer, buyer, limit)
        )
        rows = await cursor.fetchall()
        return [{"from": r[0], "to": r[1], "content": r[2], "time": r[3]} for r in reversed(rows)]

    # ─── 商品知识库 — 复刻 GoodsKnowledgeEntity ───

    async def save_knowledge(self, num_iid: int, title: str, content: str):
        """保存商品知识"""
        entity_id = f"goods_{num_iid}"
        now = int(time.time() * 10000000)
        await self._conn.execute(
            """INSERT OR REPLACE INTO GoodsKnowledgeEntity
               (EntityId, ModifyTick, NumIid, Title, Content)
               VALUES (?, ?, ?, ?, ?)""",
            (entity_id, now, num_iid, title, content)
        )
        await self._conn.commit()

    async def search_knowledge(self, keyword: str) -> list[dict]:
        """搜索商品知识"""
        cursor = await self._conn.execute(
            """SELECT NumIid, Title, Content FROM GoodsKnowledgeEntity
               WHERE (Title LIKE ? OR Content LIKE ?) AND IsDeleted = 0""",
            (f"%{keyword}%", f"%{keyword}%")
        )
        rows = await cursor.fetchall()
        return [{"num_iid": r[0], "title": r[1], "content": r[2]} for r in rows]

    async def get_all_knowledge(self) -> list[dict]:
        """获取所有商品知识"""
        cursor = await self._conn.execute(
            "SELECT NumIid, Title, Content FROM GoodsKnowledgeEntity WHERE IsDeleted = 0"
        )
        rows = await cursor.fetchall()
        return [{"num_iid": r[0], "title": r[1], "content": r[2]} for r in rows]

    # ─── 机器人规则 — 复刻 RobotRuleEntity ───

    async def save_rule(self, question: str, answer: str, patterns: list[str] = None):
        """保存规则"""
        entity_id = f"rule_{int(time.time() * 1000)}"
        now = int(time.time() * 10000000)
        await self._conn.execute(
            """INSERT OR REPLACE INTO RobotRuleEntity
               (EntityId, ModifyTick, Question, Answer, PatternsJson)
               VALUES (?, ?, ?, ?, ?)""",
            (entity_id, now, question, answer, json.dumps(patterns or [], ensure_ascii=False))
        )
        await self._conn.commit()

    async def match_rule(self, text: str) -> Optional[str]:
        """匹配规则，返回答案或 None"""
        cursor = await self._conn.execute(
            "SELECT Question, Answer, PatternsJson FROM RobotRuleEntity WHERE IsDeleted = 0"
        )
        rows = await cursor.fetchall()
        text_lower = text.lower()
        for question, answer, patterns_json in rows:
            patterns = json.loads(patterns_json) if patterns_json else []
            # 检查问题关键词
            if question and question.lower() in text_lower:
                return answer
            # 检查模式匹配
            for pattern in patterns:
                if pattern and pattern.lower() in text_lower:
                    return answer
        return None

    # ─── 买家备注 — 复刻 BuyerNoteEntity ───

    async def save_buyer_note(self, buyer_nick: str, note: str):
        """保存买家备注"""
        entity_id = f"note_{buyer_nick}"
        now = int(time.time() * 10000000)
        await self._conn.execute(
            """INSERT OR REPLACE INTO BuyerNoteEntity
               (EntityId, ModifyTick, BuyerMainNick, Note, RecordTime)
               VALUES (?, ?, ?, ?, ?)""",
            (entity_id, now, buyer_nick, note, str(int(time.time())))
        )
        await self._conn.commit()

    async def get_buyer_note(self, buyer_nick: str) -> Optional[str]:
        """获取买家备注"""
        cursor = await self._conn.execute(
            "SELECT Note FROM BuyerNoteEntity WHERE BuyerMainNick = ? AND IsDeleted = 0",
            (buyer_nick,)
        )
        row = await cursor.fetchone()
        return row[0] if row else None

    # ─── 序列化工具 ───

    @staticmethod
    def _serialize(value: Any) -> str:
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False)

    @staticmethod
    def _deserialize(raw: str, default: Any = None) -> Any:
        if raw is None:
            return default
        if isinstance(default, bool):
            return raw == "1"
        if isinstance(default, int):
            try:
                return int(raw)
            except ValueError:
                return default
        if isinstance(default, float):
            try:
                return float(raw)
            except ValueError:
                return default
        return raw
