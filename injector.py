"""
千牛注入器 — 复刻 openbot QNInject.cs
在 macOS 上找到千牛 App 的 web 资源，注入 JS 脚本。
"""

import os
import re
import shutil
import zipfile
import logging
import subprocess

logger = logging.getLogger(__name__)

# macOS 千牛 App 路径
QIANNIU_APP_PATHS = [
    "/Applications/Aliworkbench.app",
    os.path.expanduser("~/Applications/Aliworkbench.app"),
    "/Applications/千牛.app",
    os.path.expanduser("~/Applications/千牛.app"),
    "/Applications/Qianniu.app",
    os.path.expanduser("~/Applications/Qianniu.app"),
    "/Applications/AliWorkbench.app",
    os.path.expanduser("~/Applications/AliWorkbench.app"),
]

# 注入标记 — 避免重复注入
INJECT_MARKER = "___openbot_mac_injected"
EARLY_INJECT_PREFIX = "OPENBOT_EARLY_INJECT"
# 替换的原始 URL（千牛加载的 imsupport 脚本）
ORIGINAL_IM_URL = "https://iseiya.taobao.com/imsupport"
BRIDGE_ARCNAME = "openbot-bridge/index.html"
BRIDGE_URL = "https://alires-webui/openbot-bridge/index.html?openbot=1"
SERVICE_SUMMARY_URL = "https://web.m.taobao.com/app/crs-qn/qn-cs-chat-top-summary/summary"


class QianniuInjector:
    """
    千牛 JS 注入器 — 复刻 QNInject
    找到千牛的 web 资源目录，修改 HTML/JS 文件注入我们的脚本。
    """

    def __init__(self, inject_js_path: str = None):
        self.app_path: str = ""
        self.inject_js_path: str = inject_js_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "inject.js"
        )
        self.backup_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "data", "backups"
        )
        self.temp_root = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "data", "tmp"
        )
        self._injected = False

    def find_qianniu(self) -> str:
        """查找千牛 App 安装路径 — 复刻 QNInject 查找千牛路径"""
        for path in QIANNIU_APP_PATHS:
            if os.path.exists(path):
                self.app_path = path
                logger.info(f"找到千牛: {path}")
                return path

        # 尝试用 mdfind 搜索
        try:
            result = subprocess.run(
                [
                    "mdfind",
                    "(kMDItemCFBundleIdentifier == '*qianniu*' || "
                    "kMDItemCFBundleIdentifier == '*Aliworkbench*' || "
                    "kMDItemFSName == '*Aliworkbench.app')",
                ],
                capture_output=True, text=True, timeout=10
            )
            for line in result.stdout.strip().split("\n"):
                if line.endswith(".app") and os.path.exists(line):
                    self.app_path = line
                    logger.info(f"通过 mdfind 找到千牛: {line}")
                    return line
        except Exception as e:
            logger.warning(f"mdfind 搜索失败: {e}")

        logger.error("未找到千牛 App，请确认已安装")
        return ""

    def find_web_resources(self) -> list[str]:
        """
        查找千牛的 web 资源目录 — 复刻 QNInject 查找 webui.zip
        macOS Electron App 的资源通常在 Contents/Resources/ 下
        """
        if not self.app_path:
            self.find_qianniu()
        if not self.app_path:
            return []

        resources_dir = os.path.join(self.app_path, "Contents", "Resources")
        if not os.path.exists(resources_dir):
            logger.warning(f"Resources 目录不存在: {resources_dir}")
            return []

        web_resources = []

        # 查找 webui.zip — 复刻 QNInject 的 zip 注入方式
        for root, dirs, files in os.walk(resources_dir):
            for f in files:
                if f == "webui.zip":
                    web_resources.append(os.path.join(root, f))
                # 也查找 .asar 文件（Electron 标准格式）
                elif f.endswith(".asar"):
                    web_resources.append(os.path.join(root, f))
                # 查找 recent.html
                elif f == "recent.html":
                    web_resources.append(os.path.join(root, f))

        logger.info(f"找到 {len(web_resources)} 个 web 资源")
        return web_resources

    def inject_into_zip(self, zip_path: str) -> bool:
        """
        注入到 webui.zip — 复刻 QNInject 的 zip 修改逻辑
        1. 解压 recent.html
        2. 替换 imsupport URL
        3. 写回 zip
        """
        try:
            # 备份原文件
            os.makedirs(self.backup_dir, exist_ok=True)
            backup_path = os.path.join(
                self.backup_dir,
                f"{os.path.basename(self.app_path.rstrip(os.sep))}-{os.path.basename(zip_path)}.bak",
            )
            if not os.path.exists(backup_path):
                shutil.copy2(zip_path, backup_path)
                logger.info(f"已备份: {backup_path}")

            # 读取 inject.js 内容
            inject_code = self._read_inject_js()
            has_marker = self._zip_has_inject_marker(zip_path)
            has_bridge = self._zip_has_bridge(zip_path)

            # 修改 zip 中的文件
            os.makedirs(self.temp_root, exist_ok=True)
            temp_dir = os.path.join(
                self.temp_root,
                f"{os.path.basename(self.app_path.rstrip(os.sep))}-{os.path.basename(zip_path)}_temp",
            )
            shutil.rmtree(temp_dir, ignore_errors=True)
            modified = False

            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(temp_dir)

            if self._inject_early_pages(temp_dir, inject_code):
                modified = True

            if not has_marker:
                # 查找并修改 HTML 文件
                for root, dirs, files in os.walk(temp_dir):
                    for f in files:
                        if f.endswith('.html') or f == 'recent.html':
                            html_path = os.path.join(root, f)
                            if self._inject_into_html(html_path, inject_code):
                                modified = True

            if self._write_bridge_page(temp_dir, inject_code):
                modified = True

            if modified:
                # 重新打包 zip
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for root, dirs, files in os.walk(temp_dir):
                        for f in files:
                            file_path = os.path.join(root, f)
                            arcname = os.path.relpath(file_path, temp_dir)
                            zf.write(file_path, arcname)

                logger.info(f"已注入到: {zip_path}")

                # 清空 sign.json — 复刻 QNInject 的资源签名绕过方式。
                # macOS 上直接删除有时会让资源加载路径报错，保留空 JSON 更稳。
                sign_json = os.path.join(os.path.dirname(zip_path), "sign.json")
                if os.path.exists(sign_json):
                    with open(sign_json, "w", encoding="utf-8") as f:
                        f.write("{}")
                    logger.info("已清空 sign.json")

            # 清理临时目录
            shutil.rmtree(temp_dir, ignore_errors=True)
            return modified or self._zip_has_inject_marker(zip_path)

        except Exception as e:
            logger.error(f"注入 zip 失败: {e}")
            return False

    def _inject_early_pages(self, temp_dir: str, inject_code: str) -> bool:
        """Force current injection before page scripts on native chat surfaces."""
        targets = [
            "web_chat-packer/recent.html",
            "dx-h5/index.html",
            "Message/message-notify.html",
        ]
        modified = False
        for arcname in targets:
            html_path = os.path.join(temp_dir, *arcname.split("/"))
            if self._inject_early_page(html_path, arcname, inject_code):
                modified = True
        return modified

    def _inject_early_page(self, html_path: str, arcname: str, inject_code: str) -> bool:
        if not os.path.exists(html_path):
            return False

        try:
            with open(html_path, "r", encoding="utf-8") as f:
                content = f.read()
        except UnicodeDecodeError as e:
            logger.debug(f"跳过非 UTF-8 early 页面 {arcname}: {e}")
            return False

        marker = f"{EARLY_INJECT_PREFIX}:{arcname}"
        script_tag = (
            f"\n<!-- {marker} -->\n"
            f"<script>\n{inject_code}\n</script>\n"
        )

        marker_pattern = re.compile(
            r"\n?<!-- " + re.escape(marker) + r" -->\n<script>\n.*?\n</script>\n?",
            flags=re.DOTALL,
        )
        if marker in content:
            new_content = marker_pattern.sub(lambda _match: script_tag, content, count=1)
            if new_content == content:
                return False
            content = new_content
            action = "更新"
        else:
            action = "早期注入"

            head_match = re.search(r"<head[^>]*>", content, flags=re.IGNORECASE)
            if head_match:
                insert_at = head_match.end()
                content = content[:insert_at] + script_tag + content[insert_at:]
            elif "<body" in content:
                content = content.replace("<body", script_tag + "<body", 1)
            else:
                content = script_tag + content

        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"已{action}页面: {html_path}")
        return True

    def _zip_has_inject_marker(self, zip_path: str) -> bool:
        """检查 zip 内是否已有 OpenBot 注入标记。"""
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                for name in zf.namelist():
                    if not name.endswith(".html"):
                        continue
                    with zf.open(name) as f:
                        try:
                            content = f.read().decode("utf-8")
                        except UnicodeDecodeError:
                            continue
                    if INJECT_MARKER in content:
                        return True
        except Exception as e:
            logger.debug(f"检查注入标记失败: {e}")
        return False

    def _zip_has_bridge(self, zip_path: str) -> bool:
        """检查 zip 内是否已有 macOS 聊天 bridge 页面。"""
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                return BRIDGE_ARCNAME in zf.namelist()
        except Exception as e:
            logger.debug(f"检查 bridge 页面失败: {e}")
        return False

    def _write_bridge_page(self, temp_dir: str, inject_code: str) -> bool:
        """写入 serviceSummaryNew 使用的本地 bridge 页面。"""
        bridge_path = os.path.join(temp_dir, BRIDGE_ARCNAME)
        existing = ""
        if os.path.exists(bridge_path):
            with open(bridge_path, "r", encoding="utf-8") as f:
                existing = f.read()

        os.makedirs(os.path.dirname(bridge_path), exist_ok=True)
        html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OpenBot Bridge</title>
  <style>
    html, body, iframe {{
      width: 100%;
      height: 100%;
      margin: 0;
      padding: 0;
      border: 0;
      overflow: hidden;
      background: transparent;
    }}
  </style>
</head>
<body>
  <iframe src="{SERVICE_SUMMARY_URL}" referrerpolicy="no-referrer-when-downgrade"></iframe>
  <script>
{inject_code}
  </script>
</body>
</html>
"""
        if existing == html:
            return False

        with open(bridge_path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"写入 macOS bridge 页面: {bridge_path}")
        return True

    def inject_into_asar(self, asar_path: str) -> bool:
        """注入到 .asar 文件（Electron 标准格式）"""
        try:
            # asar 需要特殊的打包工具，这里先记录路径
            logger.warning(f"检测到 .asar 文件: {asar_path}")
            logger.warning("需要使用 asar 工具解包后注入，或使用 --remote-debugging-port 方式")
            return False
        except Exception as e:
            logger.error(f"注入 asar 失败: {e}")
            return False

    def inject_into_html(self, html_path: str) -> bool:
        """直接注入 HTML 文件（非 zip/asar 内的）"""
        inject_code = self._read_inject_js()
        return self._inject_into_html(html_path, inject_code)

    def _inject_into_html(self, html_path: str, inject_code: str) -> bool:
        """
        注入 JS 到 HTML — 复刻 QNInject 的 HTML 修改逻辑
        1. 替换 imsupport URL
        2. 在 </body> 前注入我们的脚本
        """
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 检查是否已注入
            if INJECT_MARKER in content:
                logger.debug(f"已注入过: {html_path}")
                return False

            original_content = content

            # 方法 1: 替换 imsupport URL — 复刻 QNInject 的 URL 替换
            if ORIGINAL_IM_URL in content:
                content = content.replace(
                    ORIGINAL_IM_URL,
                    f"data:text/javascript,{inject_code}"
                )
                logger.info(f"替换 imsupport URL: {html_path}")

            # 方法 2: 在 </body> 前注入 script 标签
            if '</body>' in content:
                script_tag = f'\n<script>\n{inject_code}\n</script>\n'
                content = content.replace('</body>', script_tag + '</body>')
                logger.info(f"注入 script 标签: {html_path}")

            if content != original_content:
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                return True

            return False

        except UnicodeDecodeError as e:
            logger.debug(f"跳过非 UTF-8 HTML: {html_path}: {e}")
            return False
        except Exception as e:
            logger.error(f"注入 HTML 失败: {e}")
            return False

    def _read_inject_js(self) -> str:
        """读取 inject.js 内容"""
        with open(self.inject_js_path, 'r', encoding='utf-8') as f:
            return f.read()

    def inject(self) -> bool:
        """
        执行注入 — 复刻 QNInject.StartInject
        自动查找千牛并注入 JS。
        """
        if self._injected:
            logger.info("已注入，跳过")
            return True

        if not self.find_qianniu():
            return False

        resources = self.find_web_resources()
        if not resources:
            logger.warning("未找到 web 资源，跳过注入")
            return False

        success = False
        for resource in resources:
            if resource.endswith('.zip'):
                if self.inject_into_zip(resource):
                    success = True
            elif resource.endswith('.asar'):
                if self.inject_into_asar(resource):
                    success = True
            elif resource.endswith('.html'):
                if self.inject_into_html(resource):
                    success = True

        if self._patch_url_config():
            success = True

        if success:
            self._resign_app()
            self._injected = True
            logger.info("注入完成！请重启千牛生效。")
        else:
            logger.warning("注入未成功，可能需要手动注入或使用其他方式。")

        return success

    def _patch_url_config(self) -> bool:
        """把聊天服务摘要页切到本地 bridge，让注入脚本进入聊天 WebView。"""
        if not self.app_path:
            return False

        config_path = os.path.join(
            self.app_path, "Contents", "Resources", "config", "UrlConfig.json"
        )
        if not os.path.exists(config_path):
            logger.debug(f"UrlConfig 不存在: {config_path}")
            return False

        with open(config_path, "r", encoding="utf-8-sig") as f:
            content = f.read()

        if f'"serviceSummaryNew": "{BRIDGE_URL}"' in content:
            logger.info("serviceSummaryNew 已指向 OpenBot bridge")
            return True

        old = f'"serviceSummaryNew": "{SERVICE_SUMMARY_URL}"'
        new = f'"serviceSummaryNew": "{BRIDGE_URL}"'
        if old not in content:
            logger.warning("未找到 serviceSummaryNew 原始 URL，跳过 bridge 配置")
            return False

        os.makedirs(self.backup_dir, exist_ok=True)
        backup_path = os.path.join(
            self.backup_dir,
            f"{os.path.basename(self.app_path.rstrip(os.sep))}-UrlConfig.json.bak",
        )
        if not os.path.exists(backup_path):
            shutil.copy2(config_path, backup_path)
            logger.info(f"已备份: {backup_path}")

        content = content.replace(old, new)
        with open(config_path, "w", encoding="utf-8-sig") as f:
            f.write(content)
        logger.info(f"已修改 serviceSummaryNew -> {BRIDGE_URL}")
        return True

    def _resign_app(self) -> bool:
        """
        macOS app bundle 修改资源后必须重签名。
        Windows 只要清 sign.json；macOS 还要修 Apple sealed resources。
        """
        if not self.app_path:
            return False

        try:
            subprocess.run(["xattr", "-cr", self.app_path], check=False, timeout=30)
            result = subprocess.run(
                ["codesign", "--force", "--deep", "--sign", "-", self.app_path],
                capture_output=True,
                text=True,
                timeout=120,
            )
            if result.returncode != 0:
                logger.error(f"重签名失败: {result.stderr.strip() or result.stdout.strip()}")
                return False
            logger.info("已完成 macOS ad-hoc 重签名")
            return True
        except Exception as e:
            logger.error(f"重签名异常: {e}")
            return False

    def restore(self) -> bool:
        """恢复备份 — 从 .bak 文件还原"""
        if not self.app_path:
            return False

        resources_dir = os.path.join(self.app_path, "Contents", "Resources")
        restored = False

        for root, dirs, files in os.walk(resources_dir):
            for f in files:
                if f.endswith('.bak'):
                    original = os.path.join(root, f[:-4])
                    backup = os.path.join(root, f)
                    shutil.copy2(backup, original)
                    os.remove(backup)
                    logger.info(f"已恢复: {original}")
                    restored = True

        return restored


def check_qianniu_running() -> bool:
    """检查千牛是否正在运行"""
    try:
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0:
            return False
        process_list = result.stdout.lower()
        return any(
            name.lower() in process_list
            for name in ("Aliworkbench.app/Contents/MacOS/Aliworkbench", "Qianniu", "千牛")
        )
    except Exception:
        return False


def restart_qianniu():
    """重启千牛"""
    try:
        # 先关闭
        subprocess.run(["pkill", "-f", "Aliworkbench|AliWorkbench|Qianniu|千牛"], capture_output=True, timeout=5)
        import time
        time.sleep(2)
        # 再打开
        app_name = "Aliworkbench" if os.path.exists("/Applications/Aliworkbench.app") else "千牛"
        subprocess.Popen(["open", "-a", app_name])
        logger.info("千牛已重启")
    except Exception as e:
        logger.error(f"重启千牛失败: {e}")
