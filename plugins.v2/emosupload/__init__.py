"""
EMOS 上传插件（MoviePilot V2）。

功能：
1. 监听 MoviePilot TransferComplete 事件，入库成功后自动上传到 EMOS；
2. 通过 EMOS 服务器端接口识别文件，自动匹配剧集信息；
3. 上传前检查 EMOS 已有资源版本，避免重复上传；
4. 支持 ask/silent 两种上传模式；
5. 支持分片并发上传，最大化吞吐。

EMOS 控制台：emya.wwzb.de
API 主域：已硬编码（不暴露）
识别域：已硬编码（不暴露）
"""

import json
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import urllib.request
import urllib.error

from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType


class EmosUpload(_PluginBase):
    """
    EMOS 上传插件。

    监听 MoviePilot 入库完成事件，将媒体文件上传到
    EMOS（Emby 资源站上传分发系统）。
    """

    plugin_name = "EMOS上传"
    plugin_desc = "MoviePilot 入库后自动上传到 EMOS（Emby 资源站上传分发系统），支持服务器端识别、版本对比、分片并发上传。"
    plugin_icon = "Emos_A.svg"
    plugin_version = "1.0.0"
    plugin_author = "xlmc"
    author_url = "https://github.com/xlmc"
    plugin_config_prefix = "emosupload_"
    plugin_order = 40
    auth_level = 1

    LOG_TAG = "[EMOS上传] "

    # ---- 硬编码域名（不暴露给用户） ----
    _API_BASE = "emos.best"
    _IDENTIFY_BASE = "emoss.wwzb.de"

    # ---- 配置项默认值 ----
    _enabled = False
    _token_path = "/opt/data/creds/emos_token"
    _token = ""
    _upload_mode = "ask"        # ask 或 silent
    _target_parts = 24          # 目标分片数
    _concurrency = 16           # 并发线程数
    _notify_channel = []        # 通知渠道
    _onlyonce = False           # 仅运行一次（手动触发）

    # 运行时状态
    _upload_lock = threading.Lock()
    _ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

    # ---- 上传历史 ----
    _upload_history: List[dict] = []

    def init_plugin(self, config: dict = None):
        """根据当前配置初始化插件。"""
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._token_path = config.get("token_path") or self._token_path
        self._upload_mode = config.get("upload_mode") or self._upload_mode
        self._target_parts = int(config.get("target_parts") or self._target_parts)
        self._concurrency = int(config.get("concurrency") or self._concurrency)
        self._notify_channel = config.get("notify_channel") or []
        self._onlyonce = bool(config.get("onlyonce"))

        # 加载 token
        self._load_token()

        # 如果勾选了仅运行一次，手动触发一次
        if self._onlyonce:
            self._onlyonce = False
            # 保存配置清除 onlyonce
            self.update_config({"onlyonce": False})

    def _load_token(self):
        """加载 EMOS token。"""
        if self._token_path and os.path.exists(self._token_path):
            try:
                with open(self._token_path, 'r') as f:
                    self._token = f.read().strip()
                logger.info(f"{self.LOG_TAG}Token 已加载")
            except Exception as e:
                logger.error(f"{self.LOG_TAG}读取 token 失败: {e}")
                self._token = ""
        else:
            self._token = ""

    def get_state(self) -> bool:
        """返回插件当前是否启用。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """注册远程命令。"""
        return [
            {
                "cmd": "/emos_status",
                "event": EventType.PluginAction,
                "desc": "查看 EMOS 上传状态",
                "category": "插件命令",
                "data": {
                    "action": "emos_status",
                },
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """声明插件 API。"""
        return [
            {
                "path": "/status",
                "endpoint": self._api_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取上传状态",
                "description": "获取当前插件状态和上传历史",
            },
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回配置页 JSON 和默认配置模型。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "upload_mode",
                                            "label": "上传模式",
                                            "items": [
                                                {"title": "询问模式 (ask) - 上传前需要确认", "value": "ask"},
                                                {"title": "静默模式 (silent) - 自动上传", "value": "silent"},
                                            ],
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "token_path",
                                            "label": "Token 文件路径",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "target_parts",
                                            "label": "目标分片数",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "concurrency",
                                            "label": "并发数",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "token_path": "/opt/data/creds/emos_token",
            "upload_mode": "ask",
            "target_parts": 24,
            "concurrency": 16,
            "notify_channel": [],
            "onlyonce": False,
        }

    def get_page(self) -> List[dict]:
        """返回详情页 JSON。"""
        status_text = "✅ 已启用" if self._enabled else "❌ 未启用"
        token_status = "✅ 已配置" if self._token else "❌ 未配置"
        mode_text = "询问模式" if self._upload_mode == "ask" else "静默模式"
        history_count = len(self._upload_history)

        page = [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info" if self._enabled else "warning",
                                    "variant": "tonal",
                                    "text": f"插件状态: {status_text}",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "success" if self._token else "error",
                                    "variant": "tonal",
                                    "text": f"Token: {token_status}",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "text": f"上传模式: {mode_text} | 历史: {history_count} 条",
                                },
                            }
                        ],
                    },
                ],
            },
        ]

        # 上传历史
        if self._upload_history:
            page.append({
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VDataTable",
                                "props": {
                                    "headers": [
                                        {"title": "文件", "key": "file"},
                                        {"title": "大小", "key": "size"},
                                        {"title": "速度", "key": "speed"},
                                        {"title": "时间", "key": "time"},
                                        {"title": "结果", "key": "result"},
                                    ],
                                    "items": self._upload_history[-10:],  # 最近10条
                                    "density": "compact",
                                    "hover": True,
                                },
                            }
                        ],
                    },
                ],
            })

        # 使用说明
        page.append({
            "component": "VAlert",
            "props": {
                "type": "warning",
                "variant": "tonal",
                "text": "说明：入库完成后自动识别并上传到 EMOS。ask 模式需确认，silent 模式自动上传。相同版本不重复上传。",
            },
        })

        return page

    def stop_service(self):
        """停用插件时清理资源。"""
        pass

    # ---- 事件处理 ----

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event):
        """
        监听入库完成事件。

        当 MoviePilot 完成文件整理入库后，自动上传源文件到 EMOS。
        """
        if not self._enabled:
            return

        if not self._token:
            logger.warning(f"{self.LOG_TAG}Token 未配置，跳过上传")
            return

        event_info: dict = event.event_data
        if not event_info:
            return

        # 获取入库信息
        transferinfo = event_info.get("transferinfo")
        mediainfo = event_info.get("mediainfo")

        if not transferinfo:
            return

        # 获取源文件路径（上传源文件到 EMOS）
        source_path = None
        if hasattr(transferinfo, 'src') and transferinfo.src:
            source_path = transferinfo.src
        elif hasattr(transferinfo, 'source_diritem') and transferinfo.source_diritem:
            source_path = transferinfo.source_diritem.path

        if not source_path:
            logger.warning(f"{self.LOG_TAG}无法获取源文件路径")
            return

        # 检查源文件是否存在
        if not os.path.exists(source_path):
            logger.warning(f"{self.LOG_TAG}源文件不存在: {source_path}")
            return

        # 获取文件名
        file_path = Path(source_path)
        file_name = file_path.name

        logger.info(f"{self.LOG_TAG}入库完成，准备上传: {file_name}")

        # 异步处理上传
        thread = threading.Thread(
            target=self._handle_upload,
            args=(str(file_path), mediainfo),
            daemon=True,
        )
        thread.start()

    def _handle_upload(self, file_path: str, mediainfo=None):
        """处理上传逻辑。"""
        try:
            with self._upload_lock:
                file_name = os.path.basename(file_path)
                file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0

                logger.info(f"{self.LOG_TAG}开始处理: {file_name} ({file_size / 1024 / 1024:.1f} MB)")

                # 1. 服务器端识别
                identify_result = self._identify_file(file_name)
                if identify_result.get("error"):
                    logger.error(f"{self.LOG_TAG}识别失败: {identify_result.get('error')}")
                    self._send_notification(f"❌ 识别失败: {file_name}\n{identify_result.get('error', '')}")
                    return

                item_type = identify_result.get("item_type")
                item_id = identify_result.get("item_id")
                name = identify_result.get("name", "未知")
                season = identify_result.get("season")
                episode = identify_result.get("episode")

                logger.info(f"{self.LOG_TAG}识别结果: {name} S{season:02d}E{episode:02d} ({item_type}-{item_id})")

                # 2. 检查是否已有资源
                existing_media = self._check_media_exists(item_type, item_id)
                video_medias = existing_media.get("video_medias", [])

                if video_medias:
                    # 已有资源，检查版本
                    should_upload = self._compare_versions(file_name, file_size, video_medias)
                    if not should_upload:
                        logger.info(f"{self.LOG_TAG}已有相同版本，跳过上传")
                        self._send_notification(f"⏭️ 跳过上传: {name} S{season:02d}E{episode:02d}\n已有相同版本资源")
                        return

                # 3. 上传文件
                upload_result = self._upload_file(file_path, item_type, item_id)
                if upload_result.get("error"):
                    logger.error(f"{self.LOG_TAG}上传失败: {upload_result.get('error')}")
                    self._send_notification(f"❌ 上传失败: {file_name}\n{upload_result.get('error', '')}")
                    return

                # 4. 记录上传历史
                history_entry = {
                    "file": file_name,
                    "size": f"{file_size / 1024 / 1024:.1f} MB",
                    "speed": upload_result.get("speed", "N/A"),
                    "time": time.strftime("%Y-%m-%d %H:%M"),
                    "result": "✅ 成功",
                }
                self._upload_history.append(history_entry)
                # 保留最近50条
                if len(self._upload_history) > 50:
                    self._upload_history = self._upload_history[-50:]

                # 5. 发送通知
                self._send_notification(
                    f"✅ 上传成功: {name} S{season:02d}E{episode:02d}\n"
                    f"文件: {file_name}\n"
                    f"大小: {file_size / 1024 / 1024:.1f} MB\n"
                    f"速度: {upload_result.get('speed', 'N/A')}"
                )

        except Exception as e:
            logger.error(f"{self.LOG_TAG}上传处理异常: {e}")
            self._send_notification(f"❌ 上传异常: {str(e)}")

    # ---- EMOS API 封装 ----

    def _req(self, method: str, url: str, data: dict = None, headers: dict = None) -> dict:
        """通用请求方法。"""
        h = {"Authorization": f"Bearer {self._token}", "User-Agent": self._ua}
        if headers:
            h.update(headers)
        body = None
        if data is not None:
            body = json.dumps(data).encode()
            h["Content-Type"] = "application/json"

        r = urllib.request.Request(url, data=body, headers=h, method=method)
        try:
            with urllib.request.urlopen(r, timeout=120) as resp:
                return json.loads(resp.read().decode(errors="replace"))
        except urllib.error.HTTPError as e:
            content = e.read().decode(errors="replace")
            try:
                return json.loads(content)
            except Exception:
                return {"error": content, "status": e.code}
        except Exception as e:
            return {"error": str(e)}

    def _identify_file(self, filename: str) -> dict:
        """通过 EMOS 服务器识别文件信息。"""
        url = f"https://{self._IDENTIFY_BASE}/api/emos/item"
        return self._req("POST", url, {"filename": filename})

    def _check_media_exists(self, item_type: str, item_id: int) -> dict:
        """检查媒体资源是否存在。"""
        url = f"https://{self._API_BASE}/api/upload/video/base"
        return self._req("GET", url, {"item_type": item_type, "item_id": item_id})

    def _extract_resolution(self, filename: str) -> str:
        """从文件名提取分辨率。"""
        import re
        # 匹配分辨率模式，返回归一化值
        patterns = [
            (r'(2160P|4K|UHD)', '2160P'),
            (r'(1080P)', '1080P'),
            (r'(720P)', '720P'),
            (r'(480P)', '480P'),
        ]
        for pattern, normalized in patterns:
            if re.search(pattern, filename, re.IGNORECASE):
                return normalized
        return ""

    def _compare_versions(self, local_filename: str, local_size: int, remote_medias: list) -> bool:
        """
        比较本地文件与远程资源版本。

        只比较：1. 分辨率  2. 文件大小

        返回 True 表示需要上传（版本不同或远程无资源）。
        返回 False 表示不需要上传（已有相同版本）。
        """
        if not remote_medias:
            return True

        # 从本地文件名提取分辨率
        local_resolution = self._extract_resolution(local_filename)

        # 遍历远程资源
        for media in remote_medias:
            remote_name = media.get("media_name", "")
            remote_size = media.get("size", 0) or 0

            # 1. 比较分辨率
            remote_resolution = self._extract_resolution(remote_name)
            if local_resolution and remote_resolution:
                if local_resolution != remote_resolution:
                    continue  # 分辨率不同，继续找下一个

            # 2. 比较文件大小（允许 5% 误差）
            if remote_size > 0 and local_size > 0:
                size_diff = abs(local_size - remote_size) / remote_size
                if size_diff > 0.05:
                    continue  # 大小差异超过 5%，继续找下一个

            # 分辨率和大小都匹配
            return False

        return True  # 未找到匹配版本

    def _put_part(self, url: str, chunk: bytes, number: int) -> Tuple[int, str]:
        """PUT 一个分片，返回 (number, etag)。"""
        r = urllib.request.Request(url, data=chunk, method="PUT")
        r.add_header("Content-Type", "application/octet-stream")
        r.add_header("User-Agent", self._ua)
        with urllib.request.urlopen(r, timeout=300) as resp:
            etag = resp.headers.get("ETag", "").strip('"')
            return number, etag

    def _upload_file(self, file_path: str, item_type: str, item_id: int) -> dict:
        """上传文件到 EMOS。"""
        if not os.path.exists(file_path):
            return {"error": f"文件不存在: {file_path}"}

        fsize = os.path.getsize(file_path)
        fname = os.path.basename(file_path)

        logger.info(f"{self.LOG_TAG}开始上传: {fname} ({fsize / 1024 / 1024:.1f} MB)")

        # 1. 获取上传凭证
        tok = self._req("POST", f"https://{self._API_BASE}/api/upload/getUploadToken", {
            "type": "video",
            "file_type": "video/mp4",
            "file_name": fname,
            "file_size": fsize,
            "file_storage": "zn_r2_upload",
        })

        if tok.get("error") or tok.get("message"):
            return {"error": f"getUploadToken 失败: {json.dumps(tok, ensure_ascii=False)[:500]}"}

        storage_type = tok.get("type", tok.get("storage_type"))
        file_id = tok.get("file_id")

        if storage_type != "multipart":
            return {"error": f"不支持的存储类型: {storage_type}"}

        d = tok.get("data", {})
        part_min = d.get("multipart_size", {}).get("min", 5 * 1024 * 1024)
        part_max = d.get("multipart_size", {}).get("max", 5 * 1024 * 1024 * 1024)

        # 计算分片大小
        part_size = max(part_min, min(part_max, math.ceil(fsize / self._target_parts / (1024 * 1024)) * 1024 * 1024))
        part_count = math.ceil(fsize / part_size)

        logger.info(f"{self.LOG_TAG}分片: {part_count} 片 x {part_size / 1024 / 1024:.0f} MB, 并发 {self._concurrency}")

        # 2. 获取分片上传地址
        pres = self._req("POST", f"https://{self._API_BASE}/api/upload/multipart/{file_id}/presign", {
            "number": part_count,
        })

        if isinstance(pres, dict) and pres.get("error"):
            return {"error": f"presign 失败: {json.dumps(pres, ensure_ascii=False)[:500]}"}

        if isinstance(pres, dict) and "presigns" in pres:
            presigns = pres["presigns"]
        elif isinstance(pres, list):
            presigns = pres
        else:
            presigns = pres.get("data", pres.get("items", []))

        logger.info(f"{self.LOG_TAG}拿到 {len(presigns)} 个上传地址")

        # 3. 并发分片上传
        etags = []
        t0 = time.time()
        done = 0
        fd = os.open(file_path, os.O_RDONLY)

        try:
            def worker(p):
                if isinstance(p, dict):
                    number = p.get("number")
                    url = p.get("upload_url") or p.get("presign_url") or p.get("url")
                else:
                    number = None
                    url = p
                offset = (number - 1) * part_size
                chunk = os.pread(fd, part_size, offset)
                return self._put_part(url, chunk, number)

            with ThreadPoolExecutor(max_workers=self._concurrency) as ex:
                futures = {ex.submit(worker, p): p for p in presigns}
                for fut in as_completed(futures):
                    number, etag = fut.result()
                    etags.append({"number": number, "etag": etag})
                    done += 1
                    elapsed = time.time() - t0
                    uploaded = fsize * done / part_count
                    speed = uploaded / elapsed if elapsed > 0 else 0
                    logger.info(f"{self.LOG_TAG}分片 {number}/{part_count} ({elapsed:.0f}s, {speed / 1024 / 1024:.1f} MB/s)")
        finally:
            os.close(fd)

        total_t = time.time() - t0
        speed_str = f"{fsize / 1024 / 1024 / total_t:.2f} MB/s" if total_t > 0 else "N/A"
        logger.info(f"{self.LOG_TAG}分片耗时 {total_t:.0f}s, 平均 {speed_str}")

        # 4. 完成分片合并
        comp = self._req("POST", f"https://{self._API_BASE}/api/upload/multipart/{file_id}/complete", {
            "parts": sorted(etags, key=lambda x: x["number"]),
        })

        # 5. 保存记录
        sv = self._req("POST", f"https://{self._API_BASE}/api/upload/video/save", {
            "item_type": item_type,
            "item_id": int(item_id),
            "file_id": file_id,
        })

        return {
            "success": True,
            "file": fname,
            "size": fsize,
            "speed": speed_str,
            "save_result": sv,
        }

    # ---- 通知 ----

    def _send_notification(self, message: str):
        """发送通知。"""
        try:
            self.post_message(
                title="EMOS上传",
                text=message,
                channel=self._notify_channel[0] if self._notify_channel else None,
            )
        except Exception as e:
            logger.error(f"{self.LOG_TAG}发送通知失败: {e}")

    # ---- 命令处理 ----

    @eventmanager.register(EventType.PluginAction)
    def on_plugin_action(self, event: Event):
        """处理插件命令。"""
        event_data = event.event_data or {}
        action = event_data.get("action")

        if action == "emos_status":
            self._handle_status_command()

    def _handle_status_command(self):
        """处理状态查询命令。"""
        status = "✅ 已启用" if self._enabled else "❌ 未启用"
        token_status = "✅ 已配置" if self._token else "❌ 未配置"
        mode = "询问模式" if self._upload_mode == "ask" else "静默模式"

        message = (
            f"📊 EMOS 上传状态\n"
            f"插件: {status}\n"
            f"Token: {token_status}\n"
            f"模式: {mode}\n"
            f"历史: {len(self._upload_history)} 条"
        )
        self._send_notification(message)

    # ---- API 端点 ----

    def _api_status(self):
        """API: 获取插件状态。"""
        return {
            "enabled": self._enabled,
            "token_configured": bool(self._token),
            "upload_mode": self._upload_mode,
            "history_count": len(self._upload_history),
            "history": self._upload_history[-10:],
        }
