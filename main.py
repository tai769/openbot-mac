"""
macOS 千牛 AI 客服机器人 — 复刻 openbot 架构
主入口：启动 WebSocket 服务器 + 千牛注入 + 守护循环。

架构:
  千牛 macOS App (Electron)
      ↓ JS inject → WebSocket
  Python WebSocket Server (127.0.0.1:41010)
      ↓ 消息解析
  Session Manager (per-seller)
      ↓ 规则匹配 + AI 回复
  Anthropic Claude API → 自动回复
"""

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime

from config import config
from db import DB
from ws_server import WebSocketServer
from session import SessionManager
from ai_client import ai_client
from knowledge import KnowledgeBase
from rules import RuleEngine
from chat_log import ChatLogger
from injector import QianniuInjector, check_qianniu_running
from install_extension_hook import install_all as install_extension_hook

# ─── 日志配置 — 复刻 openbot AppLife 的日志初始化 ───

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"bot_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
)


class _IgnoreWebSocketHandshakeAbort(logging.Filter):
    """千牛 WebView 刷新/关闭时可能中断握手；这是可忽略噪音。"""

    def filter(self, record: logging.LogRecord) -> bool:
        return not (
            record.name == "websockets.server"
            and "opening handshake failed" in record.getMessage()
        )


for handler in logging.getLogger().handlers:
    handler.addFilter(_IgnoreWebSocketHandshakeAbort())

logger = logging.getLogger("openbot-mac")


class OpenBotMac:
    """
    主应用类 — 复刻 openbot 的启动流程
    BootStrap.Init() → DeskScanner + WebSocketServer + QNInject
    """

    def __init__(self):
        self.db: DB = None
        self.server: WebSocketServer = None
        self.session_manager: SessionManager = None
        self.knowledge: KnowledgeBase = None
        self.rules: RuleEngine = None
        self.chat_logger: ChatLogger = None
        self.injector: QianniuInjector = None
        self._running = False
        self._cleanup_task: asyncio.Task = None
        self._extension_hook_task: asyncio.Task = None

    async def start(self):
        """启动 — 复刻 BootStrap.Init()"""
        logger.info("=" * 50)
        logger.info("macOS 千牛 AI 客服机器人启动中...")
        logger.info("=" * 50)

        # 1. 初始化数据库 — 复刻 DbHelper
        self.db = DB()
        await self.db.connect()
        logger.info("数据库已连接")

        # 2. 加载配置 — 复刻 Params.Robot
        await config.init(self.db)
        logger.info(f"配置已加载，模型: {config.robot.model_name}")

        # 3. 初始化各模块
        self.knowledge = KnowledgeBase(self.db)
        self.rules = RuleEngine(self.db)
        self.chat_logger = ChatLogger(self.db)

        # 4. 初始化 AI 客户端 — 复刻 MyOpenAI
        await ai_client.init()

        # 5. 加载规则缓存
        await self.rules.load_cache()

        # 6. 启动 WebSocket 服务器 — 复刻 MyWebSocketServer.Start()
        self.server = WebSocketServer()

        # 7. 初始化会话管理器 — 复刻 QN 相关逻辑
        self.session_manager = SessionManager(
            server=self.server,
            knowledge=self.knowledge,
            rules=self.rules,
            chat_logger=self.chat_logger,
        )

        # 绑定事件回调
        self.server.on_seller_connected = self.session_manager.on_seller_connected
        self.server.on_seller_disconnected = self.session_manager.on_seller_disconnected
        self.server.on_message_received = self.session_manager.on_message_received
        self.server.on_conversation_change = self.session_manager.on_conversation_change
        self.server.on_chat_dlg_active = self.session_manager.on_conversation_change
        self.server.on_shop_robot_receive = self.session_manager.on_shop_robot_receive
        self.server.on_bridge_ready = self.session_manager.on_bridge_ready
        self.server.on_ability_event = self.session_manager.on_native_event

        # 启动 WebSocket 服务器
        await self.server.start()

        # 8. 注入千牛 — 复刻 QNInject.StartInject()
        self.injector = QianniuInjector()
        if check_qianniu_running():
            logger.info("检测到千牛正在运行，尝试注入...")
            self.injector.inject()
        else:
            logger.info("千牛未运行，注入将在千牛启动后生效")

        # 9. 启动扩展 hook 守护。千牛启动时会重建本地扩展目录，这里持续补装。
        self._extension_hook_task = asyncio.create_task(self._extension_hook_loop())

        # 10. 启动定期清理任务
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        self._running = True
        logger.info("启动完成！等待千牛连接...")
        logger.info(f"WebSocket 地址: ws://127.0.0.1:41010")
        logger.info(f"API Key 已配置: {'是' if config.robot.api_key else '否'}")
        logger.info(f"自动回复: {'开启' if config.robot.auto_reply else '关闭'}")

    async def stop(self):
        """停止 — 优雅退出"""
        logger.info("正在停止...")
        self._running = False

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        if self._extension_hook_task:
            self._extension_hook_task.cancel()
            try:
                await self._extension_hook_task
            except asyncio.CancelledError:
                pass

        if self.server:
            await self.server.stop()

        if self.db:
            await self.db.close()

        logger.info("已停止")

    async def _cleanup_loop(self):
        """定期清理过期会话 — 每 5 分钟执行一次"""
        while self._running:
            try:
                await asyncio.sleep(300)  # 5 分钟
                await ai_client.cleanup_expired_sessions()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"清理任务异常: {e}")

    async def _extension_hook_loop(self):
        """持续补装千牛本地扩展 hook，用于真实聊天插件页注入。"""
        last_ok = False
        while self._running:
            try:
                if check_qianniu_running():
                    ok = install_extension_hook(quiet=True) == 0
                    if ok and not last_ok:
                        logger.info("千牛扩展 hook 已安装")
                    last_ok = ok
                else:
                    last_ok = False
                await asyncio.sleep(3)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"扩展 hook 守护异常: {e}")
                await asyncio.sleep(3)

    def print_status(self):
        """打印当前状态"""
        sellers = self.session_manager.get_all_sellers() if self.session_manager else []
        active_sessions = ai_client.get_active_session_count()

        logger.info(f"状态: 在线卖家 {len(sellers)}, 活跃会话 {active_sessions}")
        for nick in sellers:
            session = self.session_manager.get_session(nick)
            if session:
                buyers = session.get_active_buyers()
                logger.info(f"  卖家 {nick}: {len(buyers)} 个活跃买家")


async def main():
    """主函数 — 复刻 openbot StartUp.Main()"""
    bot = OpenBotMac()

    # 信号处理 — 优雅退出
    loop = asyncio.get_event_loop()

    def signal_handler():
        logger.info("收到退出信号")
        asyncio.ensure_future(bot.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await bot.start()

        # 主循环 — 保持运行
        while bot._running:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("用户中断")
    finally:
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
