import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from app.core.config import settings
from app.helper.downloader import DownloaderHelper
from app.helper.service import ServiceConfigHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import ServiceInfo
from app.schemas.types import MessageChannel, NotificationType


class QbUploadLimiter(_PluginBase):
    """
    分享率限速插件。

    当 qBittorrent / Transmission 下载器中已下载的种子分享率达到设定阈值时，
    自动将该种子的上传速度限制为指定值（KB/s）。
    分享率 = 上传量 / 下载量，阈值为正整数，达到阈值后自动限速。
    """

    plugin_name = "QB上传限速"
    plugin_desc = "当 qBittorrent 中已下载的种子分享率达到设定阈值时，自动将该种子的上传速度限制为指定值（KB/s），支持多下载器、按站点筛选、定时检测和停用恢复。"
    plugin_icon = "Qbittorrent_A.png"
    plugin_version = "1.2.8"
    plugin_author = "xlmc"
    author_url = "https://github.com/xlmc"
    plugin_config_prefix = "qbuploadlimiter_"
    plugin_order = 30
    auth_level = 1

    LOG_TAG = "[QB上传限速] "

    _enabled = False
    _onlyonce = False
    _notify_channel = None
    _share_ratio = 1
    _upload_limit = 2000
    _interval = 10
    _downloaders = []
    _sites = []
    _site_domains: Dict[str, str] = {}
    _scheduler = None
    _last_result = None
    _limited_hashes: Dict[str, set] = {}

    # 通知渠道类型（MoviePilot 通知配置的 type）-> MessageChannel 枚举
    _NOTIFY_TYPE_MAP = {
        "telegram": MessageChannel.Telegram,
        "wechat": MessageChannel.Wechat,
        "feishu": MessageChannel.Feishu,
        "wechatclawbot": MessageChannel.WechatClawBot,
        "slack": MessageChannel.Slack,
        "discord": MessageChannel.Discord,
        "synologychat": MessageChannel.SynologyChat,
        "vocechat": MessageChannel.VoceChat,
        "webpush": MessageChannel.WebPush,
        "qqbot": MessageChannel.QQ,
    }

    def init_plugin(self, config: dict = None):
        """
        初始化插件：读取配置并按需立即检测限速、启动定时检测任务。
        插件从启用变为停用时，自动将已限速种子恢复为不限速。
        """
        was_enabled = self._enabled
        old_downloaders = self._downloaders or []
        self._stop_scheduler()

        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._onlyonce = bool(config.get("onlyonce"))
        self._notify_channel = (config.get("notify_channel") or "").strip() or None
        self._share_ratio = max(self._to_int(config.get("share_ratio"), 1), 1)
        self._upload_limit = max(self._to_int(config.get("upload_limit"), 2000), 0)
        self._interval = max(self._to_int(config.get("interval"), 10), 1)
        self._downloaders = config.get("downloaders") or []
        self._sites = [str(site).strip() for site in (config.get("sites") or []) if str(site).strip()]
        self._site_domains = self._load_site_domains()

        # 停用插件时自动恢复已限速种子为不限速
        if was_enabled and not self._enabled:
            self._restore_limits(downloaders=old_downloaders)

        self._limited_hashes = {}

        if self._onlyonce:
            self._onlyonce = False
            self.update_config(self._current_config())
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.apply_limit,
                trigger="date",
                run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3),
                kwargs={"manual": True},
                name="立即检测 QB 上传限速",
            )
            self._start_scheduler()
            return

        if self._enabled:
            self.apply_limit(manual=False)
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.apply_limit,
                trigger="interval",
                minutes=self._interval,
                kwargs={"manual": False},
                name="定时检测 QB 上传限速",
            )
            self._start_scheduler()

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        不注册远程命令。
        """
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """
        不注册额外 API。
        """
        return []

    def get_state(self) -> bool:
        """
        返回插件启用状态。
        """
        return bool(self._enabled)

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        返回 MoviePilot V2 插件配置表单。
        """
        downloader_items = []
        try:
            downloader_items = [
                {"title": config.name, "value": config.name}
                for config in (DownloaderHelper().get_configs() or {}).values()
                if getattr(config, "type", "") in ("qbittorrent", "transmission")
            ]
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取下载器配置失败：{err}")

        notify_items = []
        try:
            seen_types = set()
            for conf in (ServiceConfigHelper.get_notification_configs() or []):
                if not getattr(conf, "enabled", False):
                    continue
                conf_type = getattr(conf, "type", "") or ""
                conf_name = getattr(conf, "name", "") or conf_type
                if conf_type and conf_type not in seen_types:
                    seen_types.add(conf_type)
                    notify_items.append({"title": conf_name, "value": conf_type})
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取通知渠道配置失败：{err}")

        site_items = []
        try:
            from app.helper.sites import SitesHelper
            # 与站点管理排序一致：按优先级（pri）排序，同优先级保持原顺序
            site_list = [
                site for site in (SitesHelper().get_indexers() or [])
                if site.get("is_active") and str(site.get("name") or "").strip()
            ]
            site_list.sort(key=lambda s: s.get("pri") or 0)
            site_items = [
                {
                    "title": str(site.get("name") or "").strip(),
                    "value": str(site.get("name") or "").strip(),
                }
                for site in site_list
            ]
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取站点配置失败：{err}")

        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "onlyonce", "label": "立即运行一次"},
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
                                            "label": "发送通知",
                                            "items": notify_items,
                                            "clearable": True,
                                            "hint": "请选择 MoviePilot 系统设置中已配置并启用的通知渠道；留空表示不发送通知。",
                                            "persistent-hint": True,
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "downloaders",
                                            "label": "下载器",
                                            "items": downloader_items,
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "hint": "留空时不会修改任何下载器；请选择 MoviePilot 中已配置的 qBittorrent 或 Transmission 下载器。",
                                            "persistent-hint": True,
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
                                            "model": "sites",
                                            "label": "站点（按站点筛选）",
                                            "items": site_items,
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "hint": "留空表示对所有种子生效；勾选站点后，仅对所选站点下载的种子进行上传限速。",
                                            "persistent-hint": True,
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
                                            "model": "share_ratio",
                                            "label": "分享率阈值",
                                            "placeholder": "正整数，例如 1；分享率达到该值后限速",
                                            "type": "number",
                                            "min": 1,
                                            "step": 1,
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
                                            "model": "upload_limit",
                                            "label": "上传限速（KB/s）",
                                            "placeholder": "例如 2000；0 表示不限速",
                                            "type": "number",
                                            "min": 0,
                                            "step": 1,
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
                                            "model": "interval",
                                            "label": "定时检测间隔（分钟）",
                                            "placeholder": "建议 10 分钟",
                                            "type": "number",
                                            "min": 1,
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
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "本插件按分享率逐种子限速：种子分享率（上传量/下载量）达到设定的正整数阈值后，其上传速度将被限制为设定值（KB/s）。上传限速填 0 表示恢复该种子不限速。支持 qBittorrent 和 Transmission。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], self._current_config()

    def get_page(self) -> List[dict]:
        """
        无独立详情页：点击插件卡片或通知消息将直接打开插件设置。
        """
        pass

    def stop_service(self):
        """
        停止后台任务；停用或卸载插件时自动将已限速种子恢复为不限速。
        """
        self._stop_scheduler()

        if getattr(self, "_downloaders", None):
            try:
                self._restore_limits()
            except Exception as err:
                logger.error(f"{self.LOG_TAG}恢复上传不限速失败：{err}")

    def _stop_scheduler(self):
        try:
            if getattr(self, "_scheduler", None):
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as err:
            logger.error(f"{self.LOG_TAG}停止定时任务失败：{err}")

    def apply_limit(self, manual: bool = False):
        """
        按分享率阈值对种子应用上传限速。
        """
        if not self._enabled and not manual:
            return
        if manual:
            # 点击「立即运行一次」时自动发送一次测试通知（仅首次发送）
            self._send_test_notify_if_needed()
        self._set_torrent_limits(self._share_ratio, self._upload_limit, channel=self._notify_channel)

    @property
    def service_infos(self) -> Optional[Dict[str, ServiceInfo]]:
        """
        获取已连接的 qBittorrent / Transmission 下载器。
        """
        return self._get_services()

    def _get_services(self, downloaders: Optional[List[str]] = None) -> Optional[Dict[str, ServiceInfo]]:
        names = downloaders if downloaders is not None else self._downloaders
        if not names:
            logger.warning(f"{self.LOG_TAG}尚未选择下载器")
            return None

        services = DownloaderHelper().get_services(name_filters=names)
        if not services:
            logger.warning(f"{self.LOG_TAG}获取下载器实例失败，请检查配置")
            return None

        helper = DownloaderHelper()
        active_services = {}
        for service_name, service_info in services.items():
            if not (helper.is_downloader(service_type="qbittorrent", service=service_info)
                    or helper.is_downloader(service_type="transmission", service=service_info)):
                logger.warning(f"{self.LOG_TAG}下载器 [{service_name}] 不是 qBittorrent/Transmission，已跳过")
                continue
            if not getattr(service_info, "instance", None):
                logger.warning(f"{self.LOG_TAG}下载器 [{service_name}] 实例不存在，已跳过")
                continue
            if service_info.instance.is_inactive():
                logger.warning(f"{self.LOG_TAG}下载器 [{service_name}] 未连接，已跳过")
                continue
            active_services[service_name] = service_info

        if not active_services:
            logger.warning(f"{self.LOG_TAG}没有可用的 qBittorrent/Transmission 下载器")
            return None
        return active_services

    def _set_torrent_limits(self, share_ratio: int, upload_limit: int, channel: Optional[str] = None) -> bool:
        """
        检测所有选中下载器中的种子分享率，达到阈值的设置上传限速。
        """
        services = self.service_infos
        if not services:
            self._last_result = "没有可用的 qBittorrent/Transmission 下载器，未执行限速。"
            return False

        threshold = max(self._to_int(share_ratio, 1), 1)
        limit = max(self._to_int(upload_limit, 0), 0)
        summary_lines = []
        failed_names = []
        sites = set(self._sites or [])
        if sites:
            summary_lines.append(f"站点筛选：{'、'.join(sorted(sites))}")

        for service_name, service_info in services.items():
            downloader = service_info.instance
            downloader_type = getattr(service_info, "type", "")
            try:
                torrents, error = downloader.get_torrents()
                if error or not torrents:
                    failed_names.append(service_name)
                    logger.warning(f"{self.LOG_TAG}获取下载器 [{service_name}] 种子列表失败")
                    continue

                matched = []
                for torrent in torrents:
                    torrent_hash = self._torrent_hash(torrent, downloader_type)
                    if not torrent_hash:
                        continue
                    if sites and self._torrent_site(torrent, downloader_type) not in sites:
                        continue
                    if self._torrent_ratio(torrent, downloader_type) >= threshold:
                        matched.append(torrent)

                new_limited = 0
                already = 0
                failed = 0
                current_limited = self._limited_hashes.setdefault(service_name, set())
                for torrent in matched:
                    torrent_hash = self._torrent_hash(torrent, downloader_type)
                    torrent_name = self._torrent_name(torrent, downloader_type) or torrent_hash
                    if self._torrent_current_limit(torrent, downloader_type, limit):
                        current_limited.add(torrent_hash)
                        already += 1
                        continue
                    try:
                        if downloader.change_torrent(hash_string=torrent_hash, upload_limit=limit):
                            current_limited.add(torrent_hash)
                            new_limited += 1
                            if limit > 0 and channel:
                                self._send_limit_notify(
                                    site=self._torrent_site(torrent, downloader_type),
                                    torrent_name=torrent_name,
                                    limit=limit,
                                    channel=channel,
                                )
                            logger.info(f"{self.LOG_TAG}[{service_name}] 种子 [{torrent_name}] 分享率达到 {threshold}，已限速 {self._format_limit(limit)}")
                        else:
                            failed += 1
                    except Exception as err:
                        failed += 1
                        logger.error(f"{self.LOG_TAG}[{service_name}] 设置种子 [{torrent_name}] 上传限速失败：{err}")

                summary_lines.append(
                    f"{service_name}：达标 {len(matched)} 个，新限速 {new_limited} 个，已满足 {already} 个，失败 {failed} 个"
                )
            except Exception as err:
                failed_names.append(service_name)
                logger.error(f"{self.LOG_TAG}处理下载器 [{service_name}] 失败：{err}")

        if failed_names:
            summary_lines.append(f"处理失败：{'、'.join(failed_names)}")
        self._last_result = "\n".join(summary_lines) if summary_lines else "未检测到符合条件的种子。"

        return not failed_names

    def _send_limit_notify(self, site: str, torrent_name: str, limit: int, channel: str) -> bool:
        """
        发送单条限速通知：{站点}所下的{种子}已经限速{速度} KB/s，变量加粗。
        """
        notify_channel = self._NOTIFY_TYPE_MAP.get(channel)
        if not notify_channel or limit <= 0:
            return False
        site = (site or "").strip()
        name = (torrent_name or "").strip() or "未知种子"
        if site:
            text = f"**{site}**所下的**{name}**已经限速**{limit}** KB/s"
        else:
            text = f"**{name}**已经限速**{limit}** KB/s"
        try:
            self.post_message(
                channel=notify_channel,
                mtype=NotificationType.SiteMessage,
                title="【QB上传限速】",
                text=text,
                link=settings.MP_DOMAIN(f"#/plugins?tab=installed&id={self.__class__.__name__}"),
            )
            return True
        except Exception as err:
            logger.error(f"{self.LOG_TAG}发送限速通知失败：{err}")
            return False

    def _send_test_notify_if_needed(self) -> bool:
        """
        点击「立即运行一次」时自动发送一次测试通知，仅首次发送。
        """
        channel = self._notify_channel
        if not channel:
            return False
        notify_channel = self._NOTIFY_TYPE_MAP.get(channel)
        if not notify_channel:
            return False
        try:
            if self.get_data("notify_test_sent"):
                return False
        except Exception:
            pass
        site = ""
        for name in (self._sites or []):
            name = str(name).strip()
            if name:
                site = name
                break
        if not site:
            site = "测试站点"
        text = f"**{site}**所下的**测试种子**已经限速**{self._upload_limit}** KB/s"
        try:
            self.post_message(
                channel=notify_channel,
                mtype=NotificationType.SiteMessage,
                title="【QB上传限速】测试通知",
                text=text,
                link=settings.MP_DOMAIN(f"#/plugins?tab=installed&id={self.__class__.__name__}"),
            )
            self.save_data("notify_test_sent", True)
            logger.info(f"{self.LOG_TAG}已发送测试通知（仅首次发送）")
            return True
        except Exception as err:
            logger.error(f"{self.LOG_TAG}发送测试通知失败：{err}")
            return False

    def _restore_limits(self, downloaders: Optional[List[str]] = None):
        """
        将本插件限速过的种子恢复为不限速。
        """
        services = self._get_services(downloaders)
        if not services:
            return
        for service_name, service_info in services.items():
            downloader = service_info.instance
            hashes = self._limited_hashes.get(service_name) or set()
            for torrent_hash in hashes:
                try:
                    downloader.change_torrent(hash_string=torrent_hash, upload_limit=0)
                    logger.info(f"{self.LOG_TAG}[{service_name}] 种子 [{torrent_hash}] 已恢复不限速")
                except Exception as err:
                    logger.error(f"{self.LOG_TAG}[{service_name}] 恢复种子 [{torrent_hash}] 上传限速失败：{err}")
            self._limited_hashes[service_name] = set()

    def _start_scheduler(self):
        """
        启动调度器。
        """
        if self._scheduler and self._scheduler.get_jobs():
            self._scheduler.print_jobs()
            self._scheduler.start()

    def _current_config(self) -> Dict[str, Any]:
        """
        返回当前配置。
        """
        return {
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "notify_channel": self._notify_channel,
            "share_ratio": self._share_ratio,
            "upload_limit": self._upload_limit,
            "interval": self._interval,
            "downloaders": self._downloaders,
            "sites": self._sites,
        }

    @staticmethod
    def _torrent_hash(torrent: Any, downloader_type: str) -> str:
        """
        获取种子哈希。
        """
        if downloader_type == "qbittorrent":
            return str(torrent.get("hash") or "").strip() if isinstance(torrent, dict) else ""
        return str(getattr(torrent, "hashString", "") or getattr(torrent, "id", "") or "").strip()

    @staticmethod
    def _torrent_name(torrent: Any, downloader_type: str) -> str:
        """
        获取种子名称。
        """
        if downloader_type == "qbittorrent":
            return str(torrent.get("name") or "") if isinstance(torrent, dict) else ""
        return str(getattr(torrent, "name", "") or "")

    @staticmethod
    def _torrent_ratio(torrent: Any, downloader_type: str) -> float:
        """
        获取种子分享率（上传量 / 下载量）。
        """
        if downloader_type == "qbittorrent":
            if not isinstance(torrent, dict):
                return 0.0
            ratio = torrent.get("ratio")
            if ratio is None:
                uploaded = torrent.get("uploaded") or 0
                downloaded = torrent.get("downloaded") or 0
                ratio = uploaded / downloaded if downloaded else 0
        else:
            ratio = getattr(torrent, "uploadRatio", None)
            if ratio is None:
                ratio = getattr(torrent, "ratio", None)
            if ratio is None:
                uploaded = getattr(torrent, "uploadedEver", 0) or 0
                downloaded = getattr(torrent, "downloadedEver", 0) or 0
                ratio = uploaded / downloaded if downloaded else 0
        try:
            return float(ratio or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _torrent_current_limit(torrent: Any, downloader_type: str, limit_kb: int) -> bool:
        """
        判断种子当前上传限速是否已是目标值。
        """
        if downloader_type == "qbittorrent":
            if not isinstance(torrent, dict):
                return False
            return torrent.get("up_limit") == limit_kb * 1024
        upload_limited = getattr(torrent, "uploadLimited", False)
        upload_limit = getattr(torrent, "uploadLimit", 0) or 0
        return bool(upload_limited) and int(upload_limit) == limit_kb

    def _load_site_domains(self) -> Dict[str, str]:
        """
        构建 站点域名 -> 站点名称 映射，用于识别种子所属站点。
        """
        domains = {}
        try:
            from app.helper.sites import SitesHelper
            for site in SitesHelper().get_indexers() or []:
                if not site.get("is_active"):
                    continue
                name = str(site.get("name") or "").strip()
                if not name:
                    continue
                domain = str(site.get("domain") or "").strip().lower()
                if domain:
                    domains[domain] = name
                url = str(site.get("url") or "").strip()
                if url:
                    url_domain = self._normalize_domain(url)
                    if url_domain:
                        domains[url_domain] = name
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取站点配置失败：{err}")
        return domains

    def _torrent_site(self, torrent: Any, downloader_type: str) -> str:
        """
        识别种子所属站点：优先通过 tracker 域名匹配，其次匹配标签/分类中的站点名。
        """
        for url in self._torrent_tracker_urls(torrent, downloader_type):
            domain = self._normalize_domain(url)
            if domain:
                hit = self._lookup_site_by_domain(domain)
                if hit:
                    return hit
        names = {name.lower(): name for name in self._site_domains.values()}
        for tag in self._torrent_tags(torrent, downloader_type):
            hit = names.get(str(tag).strip().lower())
            if hit:
                return hit
        category = self._torrent_category(torrent, downloader_type)
        if category:
            hit = names.get(str(category).strip().lower())
            if hit:
                return hit
        return ""

    def _lookup_site_by_domain(self, host: str) -> str:
        """
        按域名（含子域名逐级回退）查找站点名称。
        """
        host = (host or "").strip().lower()
        if not host:
            return ""
        if host.startswith("www."):
            host = host[4:]
        if host in self._site_domains:
            return self._site_domains[host]
        labels = host.split(".")
        for i in range(1, len(labels)):
            candidate = ".".join(labels[i:])
            if candidate in self._site_domains:
                return self._site_domains[candidate]
        return ""

    @staticmethod
    def _torrent_tracker_urls(torrent: Any, downloader_type: str) -> List[str]:
        """
        获取种子 tracker 地址列表。
        """
        urls = []
        if downloader_type == "qbittorrent":
            if isinstance(torrent, dict):
                tracker = torrent.get("tracker") or ""
                if tracker:
                    urls.append(str(tracker))
        else:
            tracker_list = str(getattr(torrent, "trackerList", "") or "").strip()
            if tracker_list:
                urls.extend(
                    url.strip() for url in tracker_list.splitlines() if url.strip()
                )
            trackers = getattr(torrent, "trackers", None) or []
            for tracker in trackers:
                if isinstance(tracker, dict):
                    announce = tracker.get("announce") or ""
                else:
                    announce = getattr(tracker, "announce", "") or ""
                if announce:
                    urls.append(str(announce))
        return urls

    @staticmethod
    def _torrent_tags(torrent: Any, downloader_type: str) -> List[str]:
        """
        获取种子标签。
        """
        if downloader_type == "qbittorrent":
            if not isinstance(torrent, dict):
                return []
            tags = torrent.get("tags") or ""
            return [str(tag).strip() for tag in str(tags).split(",") if str(tag).strip()]
        labels = getattr(torrent, "labels", None) or []
        return [str(label).strip() for label in labels if str(label).strip()]

    @staticmethod
    def _torrent_category(torrent: Any, downloader_type: str) -> str:
        """
        获取种子分类。
        """
        if downloader_type == "qbittorrent" and isinstance(torrent, dict):
            return str(torrent.get("category") or "").strip()
        return ""

    @staticmethod
    def _normalize_domain(url: str) -> str:
        """
        提取 URL 的域名部分（去除协议、端口与路径）。
        """
        try:
            host = (urlparse(str(url or "")).hostname or "").strip().lower()
        except Exception:
            return ""
        if host.startswith("www."):
            host = host[4:]
        return host

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        """
        安全转换整数。
        """
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _format_limit(limit: int) -> str:
        """
        格式化限速显示。
        """
        return "不限速" if limit <= 0 else f"{limit} KB/s"
