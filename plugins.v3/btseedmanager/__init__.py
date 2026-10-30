# -*- coding: utf-8 -*-
"""BT 种子管家（MoviePilot V3）

只处理 BT 站点（公开站）下载的种子，PT 站种子一律不碰：
1. 每个种子单独设置上传/下载限速（qBittorrent / Transmission 的「单种子」接口），
   不修改下载器的全局限速设置；
2. 可选的做种时长上限：BT 种子下载完成并做种超过设定小时后只暂停任务，
   不删除任务、不删除文件；
3. BT / PT 判定（优先级从高到低）：
   - 下载历史里的站点名命中「站点管理」中 public=1 的站点 → BT；
   - 下载历史里的站点名命中 PT 站点 → PT，直接跳过；
   - 没有下载历史（或站点已删除）时看 tracker 域名：
     属于某个 PT 站点域名（含子域）→ PT 豁免；其余（公开 tracker、纯 DHT 无 tracker）→ BT；
   - 配置里可以追加 BT 站点/域名白名单，以及额外的 PT 站点/域名；
4. 命种排除标签的种子一律不动；试运行只打印计划，不调用任何修改接口。

配置项（前缀 btseedmanager_）：
enabled / dry_run / onlyonce / notify / notify_channel / interval_minutes /
downloaders / upload_limit / download_limit / seed_time_hours /
exclude_tags / extra_bt_sites / extra_pt_domains

数据项：
last_run（最近一次执行摘要）、paused（已暂停清单）、exempt（手动恢复后不再自动暂停的种子）、
limits（已设置过的限速值，用于避免重复调用）、trackers（tracker 域名缓存）
"""

import datetime
import threading
import time
import traceback
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from app.db.oper.downloadhistory import DownloadHistoryOper
from app.db.oper.site import SiteOper
from app.schemas.types import NotificationChannel
from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase
from app.sdk.services import DownloaderHelper, ServiceConfigHelper

# 支持的下载器类型
SUPPORTED_DOWNLOADER_TYPES = ("qbittorrent", "transmission")
# 进度达到该值即视为「下载完成、正在做种」
COMPLETED_PROGRESS = 1.0
# 种子 tracker 缓存有效期（秒）：tracker 很少变化，不必每轮都查
TRACKER_CACHE_TTL = 86400
# 通知与详情页最多列出的条目数
MAX_LINES = 20
# 已停止/已暂停的状态值
STOPPED_STATES = ("pauseddl", "pausedup", "stoppeddl", "stoppedup", "stopped", "paused")
# 通知渠道类型映射
NOTIFY_TYPE_MAP = {
    "telegram": NotificationChannel.Telegram,
    "wechat": NotificationChannel.Wechat,
    "feishu": NotificationChannel.Feishu,
    "wechatclawbot": NotificationChannel.WechatClawBot,
    "slack": NotificationChannel.Slack,
    "discord": NotificationChannel.Discord,
    "dingtalk": NotificationChannel.DingTalk,
    "synologychat": NotificationChannel.SynologyChat,
    "vocechat": NotificationChannel.VoceChat,
    "web": NotificationChannel.Web,
    "webpush": NotificationChannel.WebPush,
    "apppush": NotificationChannel.AppPush,
    "qqbot": NotificationChannel.QQ,
}


class BtSeedManager(_PluginBase):
    # 插件名称
    plugin_name = "BT种子管家"
    # 插件描述
    plugin_desc = "只对 BT 站点（公开站）种子单独限速、限制做种时长，PT 种子一律不动，不修改下载器全局设置。"
    # 插件图标
    plugin_icon = "btseedmanager.svg"
    # 插件版本
    plugin_version = "3.0.0"
    # 插件作者
    plugin_author = "xlmc"
    # 作者主页
    author_url = "https://github.com/xlmc"
    # 插件配置项 ID 前缀
    plugin_config_prefix = "btseedmanager_"
    # 加载顺序
    plugin_order = 34
    # 可使用的用户级别
    auth_level = 1

    # 日志前缀
    LOG_TAG = "[BT种子管家] "

    def __init__(self):
        """初始化插件对象：只做字段默认值，不连库、不起任务、不联网。"""
        super().__init__()
        # ---- 配置 ----
        self._enabled: bool = False
        self._dry_run: bool = False
        self._onlyonce: bool = False
        self._notify: bool = False
        self._notify_channel: List[str] = []
        self._interval: int = 10
        self._downloaders: List[str] = []
        self._upload_limit: int = 0
        self._download_limit: int = 0
        self._seed_hours: float = 0
        self._exclude_tags: Set[str] = set()
        self._extra_bt: List[str] = []
        self._extra_pt: List[str] = []
        # ---- 调度 ----
        self._scheduler: Optional[BackgroundScheduler] = None
        self._lock = threading.Lock()
        # ---- 数据 ----
        self._last_run: Dict[str, Any] = {}
        self._paused: Dict[str, Dict[str, Any]] = {}
        self._exempt: Dict[str, str] = {}
        self._limits: Dict[str, str] = {}
        self._trackers: Dict[str, Dict[str, Any]] = {}
        # 本轮见到的种子哈希（用于裁剪缓存）
        self._seen_hashes: Set[str] = set()
        # ---- 站点索引（每轮重新加载） ----
        self._bt_site_names: Set[str] = set()
        self._pt_site_names: Set[str] = set()
        self._bt_domains: List[str] = []
        self._pt_domains: List[str] = []

    # ---------------------------------------------------------------- 生命周期
    def init_plugin(self, config: Optional[dict] = None):
        """接收配置、恢复数据、按需启动定时任务。"""
        self.stop_service()
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._dry_run = bool(config.get("dry_run", False))
        self._onlyonce = bool(config.get("onlyonce"))
        self._notify = bool(config.get("notify"))
        self._notify_channel = self._as_list(config.get("notify_channel"))
        self._interval = max(1, self._as_int(config.get("interval_minutes"), 10))
        self._downloaders = self._as_list(config.get("downloaders"))
        self._upload_limit = max(0, self._as_int(config.get("upload_limit"), 0))
        self._download_limit = max(0, self._as_int(config.get("download_limit"), 0))
        self._seed_hours = max(0.0, self._as_float(config.get("seed_time_hours"), 0.0))
        self._exclude_tags = {tag.strip().lower() for tag in self._as_list(config.get("exclude_tags")) if tag.strip()}
        self._extra_bt = self._as_list(config.get("extra_bt_sites"))
        self._extra_pt = self._as_list(config.get("extra_pt_domains"))

        self._last_run = self.get_data("last_run") or {}
        self._paused = self._load_dict("paused")
        self._exempt = self._load_dict("exempt")
        self._limits = self._load_dict("limits")
        self._trackers = self._load_dict("trackers")

        if self._current_config() != config:
            self.update_config(self._current_config())

        if not self._enabled:
            return
        if not (self._upload_limit or self._download_limit or self._seed_hours):
            logger.warning(f"{self.LOG_TAG}未配置限速与做种时长，插件不会做任何事")
            return

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)
        if self._onlyonce:
            self._scheduler.add_job(
                self._run_once,
                "date",
                run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3),
            )
            logger.info(f"{self.LOG_TAG}立即运行一次")
        self._scheduler.add_job(self._run_once, "interval", minutes=self._interval)
        self._scheduler.start()
        logger.info(f"{self.LOG_TAG}定时检查已启动，间隔 {self._interval} 分钟")

    def get_state(self) -> bool:
        """插件是否处于运行状态。"""
        return self._enabled

    def stop_service(self):
        """停止定时任务（卸载或重载配置时调用）。"""
        if self._scheduler:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception as err:
                logger.warning(f"{self.LOG_TAG}停止定时任务失败：{err}")
            self._scheduler = None

    def get_api(self) -> List[Dict[str, Any]]:
        """插件接口。"""
        return [
            {
                "path": "/run",
                "endpoint": self.api_run,
                "methods": ["POST"],
                "summary": "立即执行一次",
                "description": "按当前配置检查所有下载器里的 BT 种子，执行限速与暂停。",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "summary": "查看状态",
                "description": "返回最近一次执行摘要、已暂停清单与豁免清单。",
            },
            {
                "path": "/resume",
                "endpoint": self.api_resume,
                "methods": ["POST"],
                "summary": "恢复已暂停的种子",
                "description": "把本插件暂停过的 BT 种子恢复下载，并加入豁免清单，之后不再自动暂停。",
            },
            {
                "path": "/reset",
                "endpoint": self.api_reset,
                "methods": ["POST"],
                "summary": "清空豁免清单",
                "description": "清空豁免清单，让所有 BT 种子重新参与做种时长判断。",
            },
        ]

    # ---------------------------------------------------------------- 表单/页面
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        插件设置表单。
        """
        # 下载器下拉：已启用且类型受支持
        downloader_items = []
        try:
            for conf in (ServiceConfigHelper.get_downloader_configs() or []):
                if not getattr(conf, "enabled", False):
                    continue
                conf_name = getattr(conf, "name", "") or ""
                conf_type = getattr(conf, "type", "") or ""
                if conf_type in SUPPORTED_DOWNLOADER_TYPES and conf_name:
                    downloader_items.append({"title": conf_name, "value": conf_name})
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取下载器配置失败：{err}")

        # 通知渠道下拉：已启用渠道的类型去重
        notify_items = []
        try:
            for conf in (ServiceConfigHelper.get_notification_configs() or []):
                if not getattr(conf, "enabled", False):
                    continue
                conf_type = getattr(conf, "type", "") or ""
                if conf_type and not any(item.get("value") == conf_type for item in notify_items):
                    notify_items.append({"title": conf_type, "value": conf_type})
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取通知配置失败：{err}")

        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "title": "只动 BT 种子",
                                            "text": "插件通过「单种子」接口给 BT 站点（公开站）种子单独限速、到点只暂停，"
                                                    "PT 站种子一律跳过，也不会修改 qBittorrent / Transmission 的全局限速。",
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
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "dry_run", "label": "试运行（只打印不执行）"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "downloaders",
                                            "label": "下载器",
                                            "placeholder": "留空表示全部已启用的下载器",
                                            "items": downloader_items,
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
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
                                            "model": "notify_channel",
                                            "label": "通知渠道",
                                            "placeholder": "留空表示不发送通知",
                                            "items": notify_items,
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
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
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "upload_limit",
                                            "label": "上传限速（KB/s）",
                                            "placeholder": "0 表示不限制",
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
                                            "model": "download_limit",
                                            "label": "下载限速（KB/s）",
                                            "placeholder": "0 表示不限制",
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
                                            "model": "seed_time_hours",
                                            "label": "做种时长上限（小时）",
                                            "placeholder": "0 表示不启用，可填小数",
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
                                            "model": "interval_minutes",
                                            "label": "检查间隔（分钟）",
                                            "placeholder": "10",
                                            "type": "number",
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "title": "BT / PT 怎么判定",
                                            "text": "① 下载历史里的站点名命中「站点管理」中 public=1 的站点 → BT；"
                                                    "命中 PT 站点 → 跳过；"
                                                    "② 没有下载历史时看 tracker 域名：属于某个 PT 站点域名（含子域）→ 跳过，"
                                                    "其余（公开 tracker、无 tracker 的纯 DHT）→ 按 BT 处理；"
                                                    "③ 判定不准时可在这里补白名单。"
                                                    "做种时长到点只暂停任务，不删任务也不删文件。",
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "exclude_tags",
                                            "label": "排除标签",
                                            "placeholder": "逗号分隔，命中标签的种子一律不动",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "extra_bt_sites",
                                            "label": "额外 BT 站点/域名",
                                            "placeholder": "逗号分隔，站名或域名都行",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "extra_pt_domains",
                                            "label": "额外 PT 站点/域名（豁免）",
                                            "placeholder": "逗号分隔，站名或域名都行",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], self._current_config()

    def get_page(self) -> List[dict]:
        """详情页：最近一次执行摘要 + 已暂停清单。"""
        content: List[dict] = []
        last_run = self._last_run or {}
        if not last_run:
            content.append(
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "title": "还没有执行记录",
                        "text": "启用插件后按检查间隔自动执行，也可以在设置里勾选「立即运行一次」。",
                    },
                }
            )
        else:
            content.append(
                {
                    "component": "VAlert",
                    "props": {
                        "type": "success" if last_run.get("success") else "warning",
                        "variant": "tonal",
                        "title": last_run.get("time") or "最近一次执行",
                        "text": last_run.get("message") or "",
                    },
                }
            )

        items = []
        for hash_string, record in sorted(
            (self._paused or {}).items(), key=lambda kv: str(kv[1].get("at") or ""), reverse=True
        )[:MAX_LINES]:
            items.append(
                {
                    "hash": hash_string[:8],
                    "title": record.get("title") or hash_string[:8],
                    "downloader": record.get("downloader") or "",
                    "hours": f"{float(record.get('hours') or 0):g} 小时",
                    "at": record.get("at") or "",
                }
            )
        content.append(
            {
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
                                        {"title": "种子", "key": "title"},
                                        {"title": "下载器", "key": "downloader"},
                                        {"title": "做种时长", "key": "hours"},
                                        {"title": "暂停时间", "key": "at"},
                                    ],
                                    "items": items,
                                    "density": "compact",
                                    "hover": True,
                                },
                            }
                        ],
                    }
                ],
            }
        )
        return content

    # ---------------------------------------------------------------- 接口实现
    def api_run(self) -> Dict[str, Any]:
        """插件 API：立即执行一次。"""
        return self.run(force=True)

    def api_status(self) -> Dict[str, Any]:
        """插件 API：查看状态。"""
        return {
            "enabled": self._enabled,
            "dry_run": self._dry_run,
            "last_run": self._last_run,
            "paused": self._paused,
            "exempt": self._exempt,
            "limits": self._limits,
        }

    def api_resume(self) -> Dict[str, Any]:
        """插件 API：恢复本插件暂停过的种子，并加入豁免清单。"""
        if not self._paused:
            return {"success": True, "message": "没有需要恢复的种子", "resumed": [], "failed": []}
        downloaders = self._get_downloaders()
        resumed: List[str] = []
        failed: List[str] = []
        for hash_string, record in list(self._paused.items()):
            title = record.get("title") or hash_string[:8]
            target = self._find_downloader(downloaders, record.get("downloader"))
            if not target:
                failed.append(f"{title}（下载器 {record.get('downloader')} 不可用）")
                continue
            _, client, _ = target
            try:
                ok = client.start_torrents(ids=hash_string)
            except Exception as err:
                logger.error(f"{self.LOG_TAG}恢复种子失败：{title} - {err}")
                ok = False
            if not ok:
                failed.append(title)
                continue
            self._exempt[hash_string] = title
            self._paused.pop(hash_string, None)
            resumed.append(title)
        self.save_data("paused", self._paused)
        self.save_data("exempt", self._exempt)
        message = f"已恢复 {len(resumed)} 个种子" + (f"，失败 {len(failed)} 个" if failed else "")
        logger.info(f"{self.LOG_TAG}{message}")
        return {"success": not failed, "message": message, "resumed": resumed, "failed": failed}

    def api_reset(self) -> Dict[str, Any]:
        """插件 API：清空豁免清单。"""
        count = len(self._exempt)
        self._exempt = {}
        self.save_data("exempt", self._exempt)
        return {"success": True, "message": f"已清空豁免清单（{count} 个种子）"}

    # ---------------------------------------------------------------- 执行主体
    def _run_once(self):
        """定时任务入口。"""
        try:
            self.run()
        except Exception as err:
            logger.error(f"{self.LOG_TAG}定时执行出错：{err} - {traceback.format_exc()}")

    def run(self, force: bool = False) -> Dict[str, Any]:
        """执行一轮检查。"""
        if not self._enabled and not force:
            return {"success": False, "message": "插件未启用", "time": self._now_text()}
        if not (self._upload_limit or self._download_limit or self._seed_hours):
            summary = {
                "success": False,
                "message": "未配置限速或做种时长，未执行",
                "time": self._now_text(),
                "records": [],
                "limited": 0,
                "paused": 0,
            }
            self._last_run = summary
            self.save_data("last_run", summary)
            return summary
        if not self._lock.acquire(blocking=False):
            return {"success": False, "message": "上一次检查还在进行中", "time": self._now_text()}
        try:
            self._load_site_index()
            downloaders = self._get_downloaders()
            if not downloaders:
                summary = {
                    "success": False,
                    "message": "没有可用的下载器（只支持 qBittorrent / Transmission）",
                    "time": self._now_text(),
                    "records": [],
                    "limited": 0,
                    "paused": 0,
                }
            else:
                now = time.time()
                records = []
                self._seen_hashes = set()
                for name, client, downloader_type in downloaders:
                    try:
                        records.append(self._process_downloader(name, client, downloader_type, now))
                    except Exception as err:
                        logger.error(f"{self.LOG_TAG}处理下载器 {name} 出错：{err} - {traceback.format_exc()}")
                        records.append(
                            {
                                "downloader": name,
                                "type": downloader_type,
                                "total": 0,
                                "bt": 0,
                                "pt": 0,
                                "limited": [],
                                "paused": [],
                                "skipped": [],
                                "failed": [f"处理出错：{err}"],
                            }
                        )
                limited = sum(len(item.get("limited") or []) for item in records)
                paused = sum(len(item.get("paused") or []) for item in records)
                bt_count = sum(item.get("bt") or 0 for item in records)
                pt_count = sum(item.get("pt") or 0 for item in records)
                failed = sum(len(item.get("failed") or []) for item in records)
                summary = {
                    "success": not failed,
                    "message": self._summary_text(records, bt_count, pt_count, limited, paused, failed),
                    "time": self._now_text(),
                    "records": records,
                    "limited": limited,
                    "paused": paused,
                    "bt": bt_count,
                    "pt": pt_count,
                }
                logger.info(f"{self.LOG_TAG}{summary['message']}")
                self._prune_tracker_cache()
                if self._notify and (limited or paused or failed):
                    self._send_notify(summary)
            self._last_run = summary
            self.save_data("last_run", summary)
            self.save_data("paused", self._paused)
            self.save_data("exempt", self._exempt)
            self.save_data("limits", self._limits)
            self.save_data("trackers", self._trackers)
            return summary
        finally:
            self._lock.release()

    def _process_downloader(self, name: str, client: Any, downloader_type: str, now: float) -> Dict[str, Any]:
        """处理单个下载器。"""
        result: Dict[str, Any] = {
            "downloader": name,
            "type": downloader_type,
            "total": 0,
            "bt": 0,
            "pt": 0,
            "limited": [],
            "paused": [],
            "skipped": [],
            "failed": [],
        }
        torrents, error = client.get_torrents()
        if error:
            logger.warning(f"{self.LOG_TAG}下载器 {name} 读取种子列表失败")
            result["failed"].append("读取种子列表失败")
            return result
        torrents = torrents or []
        result["total"] = len(torrents)
        if not torrents:
            return result

        hashes = [self._torrent_hash(torrent, downloader_type) for torrent in torrents]
        self._seen_hashes.update([hash_string for hash_string in hashes if hash_string])
        history_sites = self._history_sites([hash_string for hash_string in hashes if hash_string])

        for torrent in torrents:
            hash_string = self._torrent_hash(torrent, downloader_type)
            if not hash_string:
                continue
            title = self._torrent_name(torrent, downloader_type)
            if self._excluded(torrent, downloader_type):
                result["skipped"].append(f"{title}（命中排除标签）")
                continue
            kind, reason = self._classify(history_sites.get(hash_string, ""))
            if not kind:
                kind, reason = self._classify_by_trackers(self._tracker_hosts(client, downloader_type, hash_string))
            if kind != "bt":
                result["pt"] += 1
                logger.debug(f"{self.LOG_TAG}跳过非 BT 种子：{title}（{reason}）")
                continue
            result["bt"] += 1
            limit_note = self._apply_limit(client, downloader_type, hash_string)
            if limit_note:
                result["limited"].append(f"{title}：{limit_note}")
            pause_note = self._maybe_pause(client, downloader_type, torrent, hash_string, name, now)
            if pause_note:
                result["paused"].append(f"{title}：{pause_note}")
        return result

    # ---------------------------------------------------------------- BT / PT 判定
    def _load_site_index(self):
        """加载站点索引：public=1 的站点视为 BT 站。"""
        self._bt_site_names = set()
        self._pt_site_names = set()
        self._bt_domains = []
        self._pt_domains = []
        try:
            sites = SiteOper().list() or []
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取站点列表失败，只能靠 tracker 判定：{err}")
            sites = []
        for site in sites:
            name = (getattr(site, "name", "") or "").strip().lower()
            host = self._host(getattr(site, "domain", "")) or self._host(getattr(site, "url", ""))
            if getattr(site, "public", 0):
                if name:
                    self._bt_site_names.add(name)
                if host:
                    self._bt_domains.append(host)
            else:
                if name:
                    self._pt_site_names.add(name)
                if host:
                    self._pt_domains.append(host)
        for entry in self._extra_bt:
            self._add_extra(entry, self._bt_site_names, self._bt_domains)
        for entry in self._extra_pt:
            self._add_extra(entry, self._pt_site_names, self._pt_domains)

    @staticmethod
    def _add_extra(entry: str, names: Set[str], domains: List[str]):
        """把「站名或域名」加入对应集合。"""
        value = (entry or "").strip().lower()
        if not value:
            return
        if "." in value:
            if value not in domains:
                domains.append(value)
        else:
            names.add(value)

    def _classify(self, history_site: str) -> Tuple[Optional[str], str]:
        """按下载历史里的站点名判定；判不出来返回 (None, "")。"""
        site = (history_site or "").strip().lower()
        if not site:
            return None, ""
        if site in self._bt_site_names:
            return "bt", f"下载历史站点 {history_site} 是公开站"
        if site in self._pt_site_names:
            return "pt", f"下载历史站点 {history_site} 是 PT 站"
        return None, ""

    def _classify_by_trackers(self, tracker_hosts: List[str]) -> Tuple[str, str]:
        """按 tracker 域名判定：命中 PT 域名（含子域）→ PT，其余 → BT。"""
        for host in tracker_hosts:
            if self._match_domain(host, self._bt_domains):
                return "bt", f"tracker {host} 命中 BT 白名单"
        for host in tracker_hosts:
            if self._match_domain(host, self._pt_domains):
                return "pt", f"tracker {host} 属于 PT 站点"
        if tracker_hosts:
            return "bt", f"公开 tracker（{tracker_hosts[0]}）"
        return "bt", "没有 tracker（DHT）"

    def _history_sites(self, hashes: List[str]) -> Dict[str, str]:
        """批量取下载历史里的站点名。"""
        if not hashes:
            return {}
        try:
            rows = DownloadHistoryOper().get_by_hashes(hashes) or {}
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取下载历史失败：{err}")
            return {}
        sites: Dict[str, str] = {}
        for hash_string, row in rows.items():
            site = getattr(row, "torrent_site", "") or ""
            if site:
                sites[str(hash_string).lower()] = site
        return sites

    def _tracker_hosts(self, client: Any, downloader_type: str, hash_string: str) -> List[str]:
        """取种子当前生效的 tracker 域名（带缓存）。"""
        cached = self._trackers.get(hash_string)
        now_ts = time.time()
        if isinstance(cached, dict):
            cached_at = cached.get("at")
            urls = cached.get("urls")
            if isinstance(cached_at, (int, float)) and now_ts - float(cached_at) < TRACKER_CACHE_TTL:
                return [self._host(url) for url in (urls or []) if self._host(url)]
        try:
            urls = client.get_trackers(hash_string) or []
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取 tracker 失败（{hash_string[:8]}）：{err}")
            return []
        self._trackers[hash_string] = {"urls": urls, "at": now_ts}
        return [self._host(url) for url in urls if self._host(url)]

    def _prune_tracker_cache(self):
        """只保留本轮仍存在的种子，避免缓存无限增长。"""
        if not self._seen_hashes:
            return
        for hash_string in list(self._trackers.keys()):
            if hash_string not in self._seen_hashes:
                self._trackers.pop(hash_string, None)
        for hash_string in list(self._limits.keys()):
            if hash_string not in self._seen_hashes:
                self._limits.pop(hash_string, None)

    # ---------------------------------------------------------------- 限速
    def _apply_limit(self, client: Any, downloader_type: str, hash_string: str) -> str:
        """给单个种子设置上传/下载限速。"""
        upload_limit = self._upload_limit or None
        download_limit = self._download_limit or None
        if upload_limit is None and download_limit is None:
            return ""
        mark = f"{upload_limit or 0}/{download_limit or 0}"
        if self._limits.get(hash_string) == mark:
            return ""
        if self._dry_run:
            return f"[试运行] 将限速 上传 {upload_limit or 0} / 下载 {download_limit or 0} KB/s"
        try:
            ok = client.change_torrent(
                hash_string=hash_string,
                upload_limit=upload_limit,
                download_limit=download_limit,
            )
        except Exception as err:
            logger.error(f"{self.LOG_TAG}设置限速失败（{hash_string[:8]}）：{err}")
            return ""
        if not ok:
            logger.warning(f"{self.LOG_TAG}设置限速失败（{hash_string[:8]}）")
            return ""
        self._limits[hash_string] = mark
        return f"上传 {upload_limit or 0} / 下载 {download_limit or 0} KB/s"

    # ---------------------------------------------------------------- 做种时长
    def _maybe_pause(
        self,
        client: Any,
        downloader_type: str,
        torrent: Any,
        hash_string: str,
        downloader_name: str,
        now: float,
    ) -> str:
        """做种时长到点只暂停任务。"""
        if not self._seed_hours:
            return ""
        if hash_string in self._exempt or hash_string in self._paused:
            return ""
        progress = self._torrent_progress(torrent, downloader_type)
        if progress is None or progress < COMPLETED_PROGRESS:
            return ""
        seconds = self._seeding_seconds(torrent, downloader_type, now)
        if seconds is None:
            return ""
        limit_seconds = self._seed_hours * 3600
        if seconds < limit_seconds:
            return ""
        hours = seconds / 3600
        title = self._torrent_name(torrent, downloader_type)
        if self._dry_run:
            return f"[试运行] 已做种 {hours:.1f} 小时（上限 {self._seed_hours:g} 小时），将暂停"
        if self._is_stopped(torrent, downloader_type):
            self._paused[hash_string] = {
                "title": title,
                "downloader": downloader_name,
                "at": self._now_text(),
                "hours": round(hours, 1),
            }
            return f"已做种 {hours:.1f} 小时，本来就处于暂停状态，已记入清单"
        try:
            ok = client.stop_torrents(ids=hash_string)
        except Exception as err:
            logger.error(f"{self.LOG_TAG}暂停种子失败：{title} - {err}")
            return ""
        if not ok:
            logger.warning(f"{self.LOG_TAG}暂停种子失败：{title}")
            return ""
        self._paused[hash_string] = {
            "title": title,
            "downloader": downloader_name,
            "at": self._now_text(),
            "hours": round(hours, 1),
        }
        logger.info(f"{self.LOG_TAG}已暂停做种：{title}（已做种 {hours:.1f} 小时，上限 {self._seed_hours:g} 小时）")
        return f"已做种 {hours:.1f} 小时，已暂停"

    # ---------------------------------------------------------------- 通知
    def _send_notify(self, summary: Dict[str, Any]):
        """发送执行通知。"""
        lines = [summary.get("message") or ""]
        for record in summary.get("records") or []:
            for note in (record.get("limited") or [])[:MAX_LINES]:
                lines.append(f"[限速] {note}")
            for note in (record.get("paused") or [])[:MAX_LINES]:
                lines.append(f"[暂停] {note}")
            for note in (record.get("failed") or [])[:MAX_LINES]:
                lines.append(f"[失败] {record.get('downloader')}：{note}")
        text = "\n".join(line for line in lines if line)
        for channel in self._notify_channel:
            notify_channel = NOTIFY_TYPE_MAP.get(str(channel).lower())
            if not notify_channel:
                logger.warning(f"{self.LOG_TAG}不支持的通知渠道：{channel}")
                continue
            try:
                self.post_message(channel=notify_channel, title="【BT种子管家】执行完成", text=text)
            except Exception as err:
                logger.error(f"{self.LOG_TAG}发送通知失败（{channel}）：{err}")

    # ---------------------------------------------------------------- 工具
    def _get_downloaders(self) -> List[Tuple[str, Any, str]]:
        """取可用的下载器：[(名称, 客户端, 类型)]。"""
        helper = DownloaderHelper()
        names = self._downloaders or None
        try:
            services = helper.get_services(name_filters=names) or {}
        except Exception as err:
            logger.error(f"{self.LOG_TAG}读取下载器失败：{err}")
            return []
        downloaders: List[Tuple[str, Any, str]] = []
        for name, service_info in services.items():
            downloader_type = getattr(service_info, "type", "") or ""
            if downloader_type not in SUPPORTED_DOWNLOADER_TYPES:
                continue
            if not helper.is_downloader(service_type=downloader_type, service=service_info):
                continue
            client = getattr(service_info, "instance", None)
            if not client:
                continue
            if getattr(client, "is_inactive", None) and client.is_inactive():
                logger.warning(f"{self.LOG_TAG}下载器 {name} 不可用，已跳过")
                continue
            downloaders.append((name or downloader_type, client, downloader_type))
        return downloaders

    @staticmethod
    def _find_downloader(downloaders: List[Tuple[str, Any, str]], name: Optional[str]):
        """按名称找下载器；找不到时退回第一个。"""
        for item in downloaders:
            if name and item[0] == name:
                return item
        return downloaders[0] if downloaders else None

    def _summary_text(
        self,
        records: List[Dict[str, Any]],
        bt_count: int,
        pt_count: int,
        limited: int,
        paused: int,
        failed: int,
    ) -> str:
        """拼一句执行摘要。"""
        parts = [f"BT 种子 {bt_count} 个（跳过 PT {pt_count} 个）"]
        parts.append(f"限速 {limited} 个")
        if self._seed_hours:
            parts.append(f"暂停 {paused} 个")
        if failed:
            parts.append(f"失败 {failed} 个")
        detail = []
        for record in records:
            detail.append(
                f"{record.get('downloader')}：共 {record.get('total')} 个，"
                f"BT {record.get('bt')} 个，PT {record.get('pt')} 个"
            )
        text = "，".join(parts)
        if detail:
            text += "（" + "；".join(detail) + "）"
        if self._dry_run:
            text = "[试运行] " + text
        return text

    @staticmethod
    def _host(value: Any) -> str:
        """从 URL 或域名里取主机名（小写、去 www）。"""
        if not isinstance(value, str):
            return ""
        text = value.strip().lower()
        if not text:
            return ""
        if "://" not in text:
            text = "http://" + text
        try:
            host = urlparse(text).hostname or ""
        except Exception:
            return ""
        return host[4:] if host.startswith("www.") else host

    @staticmethod
    def _match_domain(host: str, domains: List[str]) -> bool:
        """主机名是否等于某个域名或属于其子域。"""
        if not host:
            return False
        for domain in domains:
            if host == domain or host.endswith("." + domain):
                return True
        return False

    @staticmethod
    def _torrent_hash(torrent: Any, downloader_type: str) -> str:
        """取种子哈希。"""
        if downloader_type == "qbittorrent":
            return str((torrent.get("hash") if isinstance(torrent, dict) else "") or "")
        return str(getattr(torrent, "hashString", "") or getattr(torrent, "id", "") or "")

    @staticmethod
    def _torrent_name(torrent: Any, downloader_type: str) -> str:
        """取种子名称。"""
        if downloader_type == "qbittorrent":
            return str((torrent.get("name") if isinstance(torrent, dict) else "") or "")
        return str(getattr(torrent, "name", "") or "")

    @staticmethod
    def _torrent_progress(torrent: Any, downloader_type: str) -> Optional[float]:
        """取种子进度（0~1）。"""
        value = torrent.get("progress") if isinstance(torrent, dict) else getattr(torrent, "percentDone", None)
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _seeding_seconds(torrent: Any, downloader_type: str, now: float) -> Optional[float]:
        """取已做种秒数。"""
        if downloader_type == "qbittorrent":
            value = torrent.get("seeding_time") if isinstance(torrent, dict) else None
            if value is not None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    pass
            done_at = torrent.get("completion_on") if isinstance(torrent, dict) else None
            if done_at:
                try:
                    return max(0.0, now - float(done_at))
                except (TypeError, ValueError):
                    pass
            return None
        done_at = getattr(torrent, "doneDate", None)
        if done_at:
            try:
                return max(0.0, now - float(done_at))
            except (TypeError, ValueError):
                pass
        return None

    @staticmethod
    def _is_stopped(torrent: Any, downloader_type: str) -> bool:
        """种子是否已处于暂停/停止状态。"""
        state = torrent.get("state") if isinstance(torrent, dict) else getattr(torrent, "status", None)
        return str(state or "").lower() in STOPPED_STATES

    @staticmethod
    def _tags(torrent: Any, downloader_type: str) -> List[str]:
        """取种子标签。"""
        if downloader_type == "qbittorrent":
            raw = torrent.get("tags") if isinstance(torrent, dict) else ""
            return [tag.strip().lower() for tag in str(raw or "").split(",") if tag.strip()]
        labels = getattr(torrent, "labels", None) or []
        return [str(label).strip().lower() for label in labels if str(label).strip()]

    def _excluded(self, torrent: Any, downloader_type: str) -> bool:
        """是否命中排除标签。"""
        if not self._exclude_tags:
            return False
        return any(tag in self._exclude_tags for tag in self._tags(torrent, downloader_type))

    @staticmethod
    def _as_list(value: Any) -> List[str]:
        """把逗号/空白分隔的字符串或列表规范成字符串列表。"""
        if value is None:
            return []
        if isinstance(value, str):
            items = value.replace("\n", ",").replace("，", ",").split(",")
        elif isinstance(value, (list, tuple, set)):
            items = []
            for item in value:
                if isinstance(item, str):
                    items.extend(item.replace("\n", ",").replace("，", ",").split(","))
                elif item is not None:
                    items.append(str(item))
        else:
            items = [str(value)]
        return [str(item).strip() for item in items if str(item).strip()]

    @staticmethod
    def _as_int(value: Any, default: int = 0) -> int:
        """安全转 int。"""
        try:
            if value is None or value == "":
                return default
            return int(float(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _as_float(value: Any, default: float = 0.0) -> float:
        """安全转 float。"""
        try:
            if value is None or value == "":
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def _load_dict(self, key: str) -> Dict[str, Any]:
        """读取字典型数据，容错。"""
        value = self.get_data(key)
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _now_text() -> str:
        """当前时间文本。"""
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _current_config(self) -> Dict[str, Any]:
        """当前配置快照（回写表单与持久化用）。"""
        return {
            "enabled": self._enabled,
            "dry_run": self._dry_run,
            "onlyonce": self._onlyonce,
            "notify": self._notify,
            "notify_channel": self._notify_channel,
            "interval_minutes": self._interval,
            "downloaders": self._downloaders,
            "upload_limit": self._upload_limit,
            "download_limit": self._download_limit,
            "seed_time_hours": self._seed_hours,
            "exclude_tags": ",".join(sorted(self._exclude_tags)),
            "extra_bt_sites": ",".join(self._extra_bt),
            "extra_pt_domains": ",".join(self._extra_pt),
        }
