#!/usr/bin/env python3
"""Install the OpenBot page injector into Qianniu's local browser extension."""

from __future__ import annotations

import shutil
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
APP_DATA = Path.home() / "Library/Application Support/Aliworkbench/NewAppData"
MARKER = "OPENBOT_MAC_EXTENSION_HOOK"


HOOK_TEMPLATE = r"""
;(() => {
  const marker = "__OPENBOT_MAC_EXTENSION_HOOK__";
  if (window[marker]) return;
  window[marker] = true;

  const pageMarker = "__OPENBOT_MAC_PAGE_INJECTED__";
  const report = (type, data) => {
    try {
      const ws = new WebSocket("ws://127.0.0.1:41010");
      ws.onopen = () => {
        ws.send(JSON.stringify({ type, response: JSON.stringify(data) }));
        setTimeout(() => ws.close(), 300);
      };
    } catch (_) {}
  };

  const reportLoaded = () => {
    try {
      const href = String(location.href || "");
      if (!/(taobao|tmall|alires-webui|crs-qn)/i.test(href)) return;
      report("extensionHookLoaded", {
        href,
        title: document.title || "",
        readyState: document.readyState || "",
        hasQN: typeof window.QN !== "undefined",
        hasWorkbench: typeof window.workbench !== "undefined",
        hasAbilitycenter: typeof window.abilitycenter !== "undefined",
        hasOnEventNotify: typeof window.onEventNotify === "function"
      });
    } catch (_) {}
  };

  const shouldInject = () => {
    const href = String(location.href || "");
    return /market\.m\.taobao\.com\/app\/crs-qn\/Intelligent-customer-service/i.test(href) ||
      /web\.m\.taobao\.com\/app\/crs-qn\/qn-cs-chat-top-summary/i.test(href) ||
      /alires-webui\/openbot-bridge/i.test(href) ||
      /alires-webui\/dx-h5\/index\.html/i.test(href) ||
      /alires-webui\/Message\//i.test(href) ||
      (/crs-qn/i.test(href) && (
        typeof window.QN !== "undefined" ||
        typeof window.workbench !== "undefined" ||
        typeof window.abilitycenter !== "undefined"
      ));
  };

  const inject = () => {
    if (window[pageMarker] || !shouldInject()) return false;
    window[pageMarker] = true;
    const href = String(location.href || "");
    report("extensionHookSeen", { href, title: document.title || "" });
    const script = document.createElement("script");
    script.textContent = __OPENBOT_PAGE_CODE__ + "\n//# sourceURL=openbot-page.js";
    script.async = false;
    (document.documentElement || document.head || document.body).appendChild(script);
    script.remove();
    return true;
  };

  try {
    [0, 300, 1000, 2500, 5000, 8000, 12000, 20000].forEach((delay) => {
      setTimeout(() => {
        if (delay === 0 || delay === 2500 || delay === 8000) reportLoaded();
        inject();
      }, delay);
    });
  } catch (err) {
    console.error("[OpenBot] extension hook failed", err);
  }
})();
""".strip()


def build_hook(inject_code: str) -> str:
    return HOOK_TEMPLATE.replace("__OPENBOT_PAGE_CODE__", json.dumps(inject_code))


def patch_manifest(manifest: Path) -> None:
    backup = manifest.with_suffix(manifest.suffix + ".openbot.bak")
    if not backup.exists():
        shutil.copy2(manifest, backup)

    data = json.loads(manifest.read_text(encoding="utf-8"))
    content_scripts = data.setdefault("content_scripts", [])
    if not content_scripts:
        content_scripts.append({})

    script = content_scripts[0]
    matches = script.setdefault("matches", [])
    wanted_matches = [
        "https://*.taobao.com/*",
        "https://*.tmall.com/*",
        "https://*.baidu.com/*",
        "https://market.m.taobao.com/*",
        "https://web.m.taobao.com/*",
        "https://alires-webui/*",
    ]
    for pattern in wanted_matches:
        if pattern not in matches:
            matches.append(pattern)

    js_files = script.setdefault("js", [])
    if "content/index.iife.js" not in js_files:
        js_files.append("content/index.iife.js")

    script["all_frames"] = True
    script["match_about_blank"] = True
    script["run_at"] = "document_start"

    manifest.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def install_one(ext_dir: Path, quiet: bool = False) -> bool:
    content_js = ext_dir / "content/index.iife.js"
    inject_js = ROOT / "inject.js"
    manifest = ext_dir / "manifest.json"

    if not content_js.exists() or not inject_js.exists() or not manifest.exists():
        return False

    inject_code = inject_js.read_text(encoding="utf-8")
    current = content_js.read_text(encoding="utf-8")
    backup = content_js.with_suffix(content_js.suffix + ".openbot.bak")
    if not backup.exists():
        shutil.copy2(content_js, backup)

    base = backup.read_text(encoding="utf-8")
    hook = build_hook(inject_code)
    desired = base.rstrip() + "\n\n/* " + MARKER + " */\n" + hook + "\n"
    if current != desired:
        content_js.write_text(desired, encoding="utf-8")
    patch_manifest(manifest)

    if not quiet:
        print(f"已安装扩展注入: {ext_dir}")
    return True


def install_all(quiet: bool = False) -> int:
    if not APP_DATA.exists():
        if not quiet:
            print(f"未找到千牛 NewAppData: {APP_DATA}")
        return 1

    installed = 0
    for ext_dir in APP_DATA.glob("*/Extension/端智能插件/tmp"):
        if install_one(ext_dir, quiet=quiet):
            installed += 1

    if installed == 0:
        if not quiet:
            print("未找到可安装的端智能插件目录")
        return 1

    if not quiet:
        print("完成。请完全退出并重启千牛，让扩展重新加载。")
    return 0


def main() -> int:
    return install_all(quiet=False)


if __name__ == "__main__":
    raise SystemExit(main())
