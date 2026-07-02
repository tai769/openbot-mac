"""
配置管理 — 复刻 openbot Params.Robot + PersistentParams
从 SQLite 读写所有配置项，支持内存缓存。
"""

import asyncio
from dataclasses import dataclass, field
from db import DB

# 默认值
DEFAULT_BASE_URL = "https://api.aisz.mom/v1"
DEFAULT_MODEL = "gpt-5.5"
DEFAULT_SYSTEM_PROMPT = (
    "你就是店铺客服本人，正在代表店铺和买家沟通。请根据买家的问题给出简洁、友好的回复。"
    "不要说自己是机器人、AI、自动回复或助手；不要建议买家联系人工客服、转人工或找客服。"
    "需要核实时直接说“我这边帮您核实/您发我订单号或商品截图我这边查看”。"
    "回复不要太长，控制在100字以内。"
)
DEFAULT_AUTO_REPLY = True
DEFAULT_REPLY_DELAY = 2.0  # 秒
DEFAULT_SESSION_TIMEOUT = 1800  # 30 分钟无消息自动清理会话


@dataclass
class RobotConfig:
    """AI 机器人配置 — 复刻 openbot Params.Robot"""
    base_url: str = DEFAULT_BASE_URL
    api_key: str = ""
    model_name: str = DEFAULT_MODEL
    system_prompt: str = DEFAULT_SYSTEM_PROMPT
    auto_reply: bool = DEFAULT_AUTO_REPLY
    reply_delay: float = DEFAULT_REPLY_DELAY
    session_timeout: int = DEFAULT_SESSION_TIMEOUT

    # 内存缓存，避免每次都读 DB
    _loaded: bool = field(default=False, repr=False)

    async def load(self, db: DB):
        """从数据库加载配置"""
        self.base_url = await db.get_param("robot.base_url", DEFAULT_BASE_URL)
        self.api_key = await db.get_param("robot.api_key", "")
        self.model_name = await db.get_param("robot.model_name", DEFAULT_MODEL)
        self.system_prompt = await db.get_param("robot.system_prompt", DEFAULT_SYSTEM_PROMPT)
        self.auto_reply = await db.get_param("robot.auto_reply", DEFAULT_AUTO_REPLY)
        self.reply_delay = await db.get_param("robot.reply_delay", DEFAULT_REPLY_DELAY)
        self.session_timeout = await db.get_param("robot.session_timeout", DEFAULT_SESSION_TIMEOUT)
        self._loaded = True

    async def save(self, db: DB):
        """保存配置到数据库"""
        await db.set_param("robot.base_url", self.base_url)
        await db.set_param("robot.api_key", self.api_key)
        await db.set_param("robot.model_name", self.model_name)
        await db.set_param("robot.system_prompt", self.system_prompt)
        await db.set_param("robot.auto_reply", self.auto_reply)
        await db.set_param("robot.reply_delay", self.reply_delay)
        await db.set_param("robot.session_timeout", self.session_timeout)


class Config:
    """全局配置管理器 — 单例模式，复刻 openbot Params"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.robot = RobotConfig()
            cls._instance._db = None
        return cls._instance

    async def init(self, db: DB):
        """初始化，从数据库加载所有配置"""
        self._db = db
        await self.robot.load(db)

    async def save_robot(self):
        """保存机器人配置"""
        if self._db:
            await self.robot.save(self._db)


# 全局单例
config = Config()
