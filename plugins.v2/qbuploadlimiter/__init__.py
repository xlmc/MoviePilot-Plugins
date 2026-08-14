"""
QB上传限速插件（MoviePilot v2/v3）。

功能：
1. 定时轮询已选下载器（qBittorrent / Transmission）中的种子；
2. 种子分享率（上传量 / 下载量）达到设定阈值后，自动限制该种子上传速度为指定值（KB/s）；上传速度填 0 时不做限速处理；
3. 支持按站点筛选：勾选站点时仅处理所选站点下载的种子，未勾选时处理全部种子；
4. 停用或卸载插件时，自动将本插件限速过的种子恢复为不限速；
5. 限速通知支持多选 MoviePilot 已启用通知渠道，测试通知仅首次发送；
6. 支持监控超时取消：下载完成后达不到限速值、或限速后持续超时/速度低于限速值 80% 时，取消监控不再设置限速。
"""

import datetime
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from app.core.config import settings
from app.helper.downloader import DownloaderHelper
from app.helper.service import ServiceConfigHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import ServiceInfo
from app.schemas.types import MessageChannel


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
    plugin_version = "1.2.18"
    plugin_author = "xlmc"
    author_url = "https://github.com/xlmc"
    plugin_config_prefix = "qbuploadlimiter_"
    plugin_order = 30
    auth_level = 1

    LOG_TAG = "[QB上传限速] "

    # ---- 配置项默认值 ----
    _enabled = False
    _onlyonce = False
    # 已选择的通知渠道类型（如 telegram / wechat），留空表示不发通知
    _notify_channel = []
    # 分享率阈值（正整数）
    _share_ratio = 1
    # 上传速度 KB/s，0 表示分享率达到阈值后不做限速处理
    _upload_limit = 2000
    # 定时检测间隔（秒）
    _interval_seconds = 30
    # 已选择的下载器名称
    _downloaders = []
    # 已选择的站点名称，为空表示对所有种子生效
    _sites = []
    # 站点域名(小写) -> 站点名称，用于 tracker 域名匹配
    _site_domains: Dict[str, str] = {}
    # 站点名称(小写) -> 原始名称，用于标签/分类匹配
    _site_names: Dict[str, str] = {}

    _scheduler = None
    _last_result = None
    # 已被本插件限速的种子：{下载器名称: {种子Hash}}，用于停用/卸载时恢复
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

    # ---------------------------------------------------------------- 生命周期

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
        self._notify_channel = self._normalize_channels(config.get("notify_channel"))
        self._share_ratio = max(self._to_int(config.get("share_ratio"), 1), 1)
        self._upload_limit = max(self._to_int(config.get("upload_limit"), 2000), 0)
        self._interval_seconds = max(self._to_int(config.get("interval_seconds"), 30), 10)
        # 监控超时取消配置（秒），0 表示不启用
        self._complete_timeout = max(self._to_int(config.get("complete_timeout"), 0), 0)
        self._limit_timeout = max(self._to_int(config.get("limit_timeout"), 0), 0)
        self._downloaders = config.get("downloaders") or []
        self._sites = [str(site).strip() for site in (config.get("sites") or []) if str(site).strip()]
        # 站点映射（域名 -> 名称、名称小写 -> 名称）只构建一次，供本轮所有种子复用
        self._site_domains = self._load_site_domains()
        self._site_names = {name.lower(): name for name in self._site_domains.values() if name}

        # 版本升级后允许重新发送一次测试通知（同一版本内仍仅发送一次），
        # 便于升级后验证通知渠道是否可用
        try:
            if self.get_data("last_version") != self.plugin_version:
                self.save_data("notify_test_sent", False)
                self.save_data("last_version", self.plugin_version)
        except Exception:
            pass

        # 停用插件时自动恢复已限速种子为不限速
        if was_enabled and not self._enabled:
            self._restore_limits(downloaders=old_downloaders)

        # 每次重新初始化时清空限速记录，避免旧记录影响新会话
        self._limited_hashes = {}
        self._limited_times = {}
        self._slow_since = {}
        self._speed_ok = {}
        self._canceled_hashes = {}

        # 「立即运行一次」：延迟 3 秒执行一次手动检测（含首次测试通知）
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

        # 启用插件：先立即检测一次，再按间隔定时检测
        if self._enabled:
            self.apply_limit(manual=False)
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.apply_limit,
                trigger="interval",
                seconds=self._interval_seconds,
                kwargs={"manual": False},
                name="定时检测 QB 上传限速",
            )
            self._start_scheduler()

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

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """不注册远程命令。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """不注册额外 API。"""
        return []

    def get_state(self) -> bool:
        """返回插件启用状态。"""
        return bool(self._enabled)

    def get_page(self) -> List[dict]:
        """
        无独立详情页：点击插件卡片或通知消息将直接打开插件设置。
        """
        pass

    # ---------------------------------------------------------------- 设置表单
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        插件设置表单：
        第一行：启用插件 / 立即运行一次 / 发送通知（多选渠道）；
        第二行：下载器（多选）/ 站点（多选，按站点筛选）；
        第三行：分享率阈值 / 上传速度 / 定时检测间隔；
        第四行：下载完成后监控超时 / 限速后取消监控超时；
        第五行：功能说明。
        """
        # 下载器下拉：MoviePilot 已配置并启用的 qBittorrent / Transmission
        downloader_items = []
        try:
            for conf in (ServiceConfigHelper.get_downloader_configs() or []):
                if not getattr(conf, "enabled", False):
                    continue
                conf_name = getattr(conf, "name", "") or ""
                conf_type = getattr(conf, "type", "") or ""
                if conf_type in ("qbittorrent", "transmission") and conf_name:
                    downloader_items.append({"title": conf_name, "value": conf_name})
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取下载器配置失败：{err}")

        # 通知渠道下拉：MoviePilot 已启用渠道的类型去重（如 telegram / wechat）
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

        # 站点下拉：与站点管理排序一致（按优先级 pri 升序，同优先级保持原顺序）
        site_items = []
        try:
            from app.helper.sites import SitesHelper
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
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "hint": "可多选 MoviePilot 系统设置中已配置并启用的通知渠道；留空表示不发送通知。",
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
                                            "label": "上传速度（KB/s）",
                                            "placeholder": "例如 2000；0 表示达到阈值后不做限速处理",
                                            "type": "number",
                                            "min": 0,
                                            "step": 1,
                                            "hide-spin-buttons": True,
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
                                            "model": "interval_seconds",
                                            "label": "定时检测间隔（秒）",
                                            "placeholder": "建议设置 30 秒以上",
                                            "type": "number",
                                            "min": 10,
                                            "step": 10,
                                            "hide-spin-buttons": True,
                                            "hint": "建议设置 30 秒以上",
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "complete_timeout",
                                            "label": "下载完成后监控超时（秒）",
                                            "placeholder": "0 表示不启用；例如 300",
                                            "type": "number",
                                            "min": 0,
                                            "step": 1,
                                            "hide-spin-buttons": True,
                                            "hint": "种子下载完成后，若在设定秒数内上传速度始终达不到限速值，则取消监控，不再设置限速",
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "limit_timeout",
                                            "label": "限速后取消监控超时（秒）",
                                            "placeholder": "0 表示不启用；例如 600",
                                            "type": "number",
                                            "min": 0,
                                            "step": 1,
                                            "hide-spin-buttons": True,
                                            "hint": "种子被限速后，持续限速或上传速度低于限速值 80% 达到设定秒数时，取消监控，不再设置限速",
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "本插件按分享率逐种子限速：种子分享率（上传量/下载量）达到设定的正整数阈值后，其上传速度将被限制为设定值（KB/s）。上传速度填 0 表示分享率达到阈值后不做限速处理；上传速度、检测间隔与两个监控超时均须为正整数（两个监控超时填 0 表示不启用对应功能）。支持 qBittorrent 和 Transmission。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], self._current_config()

    def _current_config(self) -> Dict[str, Any]:
        """返回当前配置，供表单回填。"""
        return {
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "notify_channel": self._notify_channel,
            "share_ratio": self._share_ratio,
            "upload_limit": self._upload_limit,
            "interval_seconds": self._interval_seconds,
            "complete_timeout": self._complete_timeout,
            "limit_timeout": self._limit_timeout,
            "downloaders": self._downloaders,
            "sites": self._sites,
        }

    # ---------------------------------------------------------------- 核心逻辑

    def apply_limit(self, manual: bool = False):
        """
        按分享率阈值对种子应用上传限速。
        手动运行（点击「立即运行一次」）时，额外发送一次仅首启的测试通知。
        """
        if not self._enabled and not manual:
            return
        if manual:
            self._send_test_notify_if_needed()
        self._set_torrent_limits(self._share_ratio, self._upload_limit, channel=self._notify_channel)

    @property
    def service_infos(self) -> Optional[Dict[str, ServiceInfo]]:
        """获取已连接的 qBittorrent / Transmission 下载器。"""
        return self._get_services()

    def _get_services(self, downloaders: Optional[List[str]] = None) -> Optional[Dict[str, ServiceInfo]]:
        """
        获取已启用且可连接的 qBittorrent / Transmission 下载器实例。

        :param downloaders: 下载器名称列表；为空时使用插件配置中的下载器
        :return: {下载器名称: ServiceInfo}，无可用下载器时返回 None
        """
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

    def _selected_sites(self) -> Optional[Set[str]]:
        """
        返回已勾选站点的规范化（小写）集合。

        :return: 勾选了站点时返回小写站点名集合；未勾选返回 None（表示不筛选站点）
        """
        sites = [str(site).strip() for site in (self._sites or []) if str(site).strip()]
        return {site.lower() for site in sites} if sites else None

    def _set_torrent_limits(self, share_ratio: int, upload_limit: int, channel: Any = None) -> bool:
        """
        检测所有选中下载器中的种子分享率，达到阈值的设置上传限速。

        站点筛选逻辑：
        - 勾选了站点：仅处理能识别出站点且属于勾选站点的种子；
        - 未勾选站点：处理全部种子。

        :param share_ratio: 分享率阈值
        :param upload_limit: 上传限速 KB/s
        :param channel: 通知渠道配置（原始值，可为字符串或列表）
        :return: 是否所有下载器均处理成功
        """
        services = self._get_services()
        if not services:
            self._last_result = "没有可用的 qBittorrent/Transmission 下载器，未执行限速。"
            return False

        threshold = max(self._to_int(share_ratio, 1), 1)
        limit = max(self._to_int(upload_limit, 0), 0)
        # 上传速度为 0：分享率达到阈值后不做限速处理
        if limit == 0:
            self._last_result = "上传速度为 0，分享率达到阈值后不做限速处理。"
            return True
        # 站点筛选集合（None 表示不过滤）
        selected = self._selected_sites()
        summary_lines = []
        if selected:
            summary_lines.append(f"站点筛选：{'、'.join(sorted(self._sites))}")
        failed_names = []

        for service_name, service_info in services.items():
            downloader = service_info.instance
            downloader_type = getattr(service_info, "type", "")
            try:
                torrents, error = downloader.get_torrents()
                if error or not torrents:
                    failed_names.append(service_name)
                    logger.warning(f"{self.LOG_TAG}获取下载器 [{service_name}] 种子列表失败")
                    continue

                # 站点识别缓存：{种子Hash: 站点名称}，同一轮内每个种子只计算一次
                site_cache: Dict[str, str] = {}
                # 筛选出达标且（可选）属于勾选站点的种子
                matched = self._collect_matched_torrents(
                    torrents=torrents,
                    downloader_type=downloader_type,
                    threshold=threshold,
                    selected=selected,
                    site_cache=site_cache,
                )
                # 对达标种子应用限速并统计结果
                new_limited, already, failed, canceled = self._apply_limits(
                    service_name=service_name,
                    downloader=downloader,
                    downloader_type=downloader_type,
                    matched=matched,
                    limit=limit,
                    threshold=threshold,
                    channel=channel,
                    site_cache=site_cache,
                )
                summary_lines.append(
                    f"{service_name}：达标 {len(matched)} 个，新限速 {new_limited} 个，已满足 {already} 个，失败 {failed} 个，取消监控 {canceled} 个"
                )
            except Exception as err:
                failed_names.append(service_name)
                logger.error(f"{self.LOG_TAG}处理下载器 [{service_name}] 失败：{err}")

        if failed_names:
            summary_lines.append(f"处理失败：{'、'.join(failed_names)}")
        self._last_result = "\n".join(summary_lines) if summary_lines else "未检测到符合条件的种子。"
        return not failed_names

    def _collect_matched_torrents(
        self,
        torrents: List[Any],
        downloader_type: str,
        threshold: int,
        selected: Optional[Set[str]],
        site_cache: Dict[str, str],
    ) -> List[Any]:
        """
        从种子列表中筛选出达到分享率阈值且（可选）属于勾选站点的种子。

        站点筛选启用时，先一次性批量查询下载历史（hash -> 站点），
        优先使用 MoviePilot 记录的权威站点信息，避免逐种子猜测。
        """
        # 仅当勾选站点时才需要做站点识别，避免无谓的数据库查询
        history_sites: Dict[str, str] = {}
        if selected:
            hashes = [self._torrent_hash(t, downloader_type) for t in torrents]
            history_sites = self._load_history_sites(hashes)

        matched = []
        for torrent in torrents:
            torrent_hash = self._torrent_hash(torrent, downloader_type)
            if not torrent_hash:
                continue
            # 站点筛选：识别站点并校验是否属于勾选列表
            if selected:
                site = self._resolve_site(torrent, torrent_hash, downloader_type, history_sites, site_cache)
                if not site or site.lower() not in selected:
                    continue
            # 分享率筛选
            if self._torrent_ratio(torrent, downloader_type) < threshold:
                continue
            matched.append(torrent)
        return matched

    def _apply_limits(
        self,
        service_name: str,
        downloader: Any,
        downloader_type: str,
        matched: List[Any],
        limit: int,
        threshold: int,
        channel: Any,
        site_cache: Dict[str, str],
    ) -> Tuple[int, int, int, int]:
        """
        对达标种子逐个设置上传限速，返回 (新增限速数, 已满足数, 失败数, 取消监控数)。

        监控超时取消机制（对应配置项为 0 时关闭）：
        - 下载完成后超时：种子下载完成后，若在设定秒数内上传速度始终达不到限速值，
          取消监控，不再设置限速；
        - 限速后超时：种子被限速后，持续限速或上传速度低于限速值 80% 达到设定秒数时，
          取消监控，不再设置限速。
        """
        new_limited = already = failed = canceled = 0
        limited_hashes = self._limited_hashes.setdefault(service_name, set())
        canceled_hashes = self._canceled_hashes.setdefault(service_name, set())
        # 限速值大于 0 时才需要发通知
        channels = self._normalize_channels(channel) if limit > 0 else []

        for torrent in matched:
            torrent_hash = self._torrent_hash(torrent, downloader_type)
            torrent_name = self._torrent_name(torrent, downloader_type) or torrent_hash
            # 已取消监控的种子：跳过，不再设置限速
            if torrent_hash in canceled_hashes:
                continue

            now = time.time()

            # 已限速种子：按「限速后超时」规则判断是否取消监控
            if torrent_hash in limited_hashes:
                if self._limit_timeout > 0 and limit > 0 and self._check_limit_timeout(
                    service_name, torrent, downloader_type, torrent_hash, limit, now
                ):
                    self._cancel_monitoring(service_name, torrent_hash, torrent_name, reason="限速后超时")
                    canceled += 1
                    continue
                # 当前限速已是目标值：计入「已满足」，避免重复调用下载器接口
                if self._torrent_current_limit(torrent, downloader_type, limit):
                    limited_hashes.add(torrent_hash)
                    already += 1
                    continue
            elif self._complete_timeout > 0 and limit > 0 and self._check_complete_timeout(
                service_name, torrent, downloader_type, torrent_hash, limit, now
            ):
                # 未限速的达标种子：下载完成后超时仍达不到限速值，取消监控
                self._cancel_monitoring(service_name, torrent_hash, torrent_name, reason="下载完成后达不到限速值")
                canceled += 1
                continue

            try:
                if not downloader.change_torrent(hash_string=torrent_hash, upload_limit=limit):
                    failed += 1
                    continue
                limited_hashes.add(torrent_hash)
                # 记录本次限速时间，用于「限速后超时」计时
                self._limited_times.setdefault(service_name, {})[torrent_hash] = now
                new_limited += 1
                logger.info(
                    f"{self.LOG_TAG}[{service_name}] 种子 [{torrent_name}] 分享率达到 {threshold}，"
                    f"已限速 {self._format_limit(limit)}"
                )
                if channels:
                    site = site_cache.get(torrent_hash, "") or self._torrent_site(torrent, downloader_type)
                    self._send_limit_notify(site=site, torrent_name=torrent_name, limit=limit, channels=channels)
            except Exception as err:
                failed += 1
                logger.error(f"{self.LOG_TAG}[{service_name}] 设置种子 [{torrent_name}] 上传限速失败：{err}")
        return new_limited, already, failed, canceled

    # ---------------------------------------------------------------- 站点识别
    def _load_site_domains(self) -> Dict[str, str]:
        """
        构建 站点域名(小写) -> 站点名称 映射，用于识别种子所属站点。
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

    @staticmethod
    def _load_history_sites(hashes: List[str]) -> Dict[str, str]:
        """
        批量查询下载历史，返回 {种子Hash: 站点名称}，作为站点识别的权威依据。

        MoviePilot 在添加下载时会记录种子所属站点（torrent_site），
        优先使用该记录比 tracker/标签猜测更准确。
        """
        # 去重且过滤空值，减少一次数据库查询的行数
        hashes = [h for h in dict.fromkeys(hashes) if h]
        if not hashes:
            return {}
        try:
            from app.db.downloadhistory_oper import DownloadHistoryOper
            histories = DownloadHistoryOper().get_by_hashes(hashes)
            return {
                history.download_hash: (history.torrent_site or "").strip()
                for history in histories.values()
                if history and (history.torrent_site or "").strip()
            }
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}查询下载历史站点失败：{err}")
            return {}

    def _resolve_site(
        self,
        torrent: Any,
        torrent_hash: str,
        downloader_type: str,
        history_sites: Dict[str, str],
        site_cache: Dict[str, str],
    ) -> str:
        """
        识别种子所属站点（带缓存）。

        优先级：下载历史记录 -> tracker 域名 -> 标签 -> 分类。
        同一轮检测中每个种子只识别一次，结果写入 site_cache 复用。
        """
        if torrent_hash in site_cache:
            return site_cache[torrent_hash]
        site = history_sites.get(torrent_hash) or self._torrent_site(torrent, downloader_type)
        site_cache[torrent_hash] = site or ""
        return site_cache[torrent_hash]

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
        for tag in self._torrent_tags(torrent, downloader_type):
            hit = self._site_names.get(str(tag).strip().lower())
            if hit:
                return hit
        category = self._torrent_category(torrent, downloader_type)
        if category:
            hit = self._site_names.get(str(category).strip().lower())
            if hit:
                return hit
        return ""

    def _lookup_site_by_domain(self, host: str) -> str:
        """
        按域名（含子域名逐级回退）查找站点名称。

        例如 tracker.hdchina.org 会依次尝试 hdchina.org、org。
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
    def _normalize_domain(url: str) -> str:
        """提取 URL 的域名部分（去除协议、端口与路径），统一转为小写。"""
        try:
            host = (urlparse(str(url or "")).hostname or "").strip().lower()
        except Exception:
            return ""
        if host.startswith("www."):
            host = host[4:]
        return host

    @staticmethod
    def _torrent_tracker_urls(torrent: Any, downloader_type: str) -> List[str]:
        """获取种子 tracker 地址列表。"""
        urls = []
        if downloader_type == "qbittorrent":
            if isinstance(torrent, dict):
                tracker = torrent.get("tracker") or ""
                if tracker:
                    urls.append(str(tracker))
        else:
            tracker_list = str(getattr(torrent, "trackerList", "") or "").strip()
            if tracker_list:
                urls.extend(url.strip() for url in tracker_list.splitlines() if url.strip())
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
        """获取种子标签列表。"""
        if downloader_type == "qbittorrent":
            if not isinstance(torrent, dict):
                return []
            tags = torrent.get("tags") or ""
            return [str(tag).strip() for tag in str(tags).split(",") if str(tag).strip()]
        labels = getattr(torrent, "labels", None) or []
        return [str(label).strip() for label in labels if str(label).strip()]

    @staticmethod
    def _torrent_category(torrent: Any, downloader_type: str) -> str:
        """获取种子分类（仅 qBittorrent）。"""
        if downloader_type == "qbittorrent" and isinstance(torrent, dict):
            return str(torrent.get("category") or "").strip()
        return ""

    # ---------------------------------------------------------------- 种子属性

    @staticmethod
    def _torrent_hash(torrent: Any, downloader_type: str) -> str:
        """获取种子哈希。"""
        if downloader_type == "qbittorrent":
            return str(torrent.get("hash") or "").strip() if isinstance(torrent, dict) else ""
        return str(getattr(torrent, "hashString", "") or getattr(torrent, "id", "") or "").strip()

    @staticmethod
    def _torrent_name(torrent: Any, downloader_type: str) -> str:
        """获取种子名称。"""
        if downloader_type == "qbittorrent":
            return str(torrent.get("name") or "") if isinstance(torrent, dict) else ""
        return str(getattr(torrent, "name", "") or "")

    @staticmethod
    def _torrent_ratio(torrent: Any, downloader_type: str) -> float:
        """
        获取种子分享率（上传量 / 下载量）。

        优先使用下载器返回的 ratio 字段，缺失时按 上传量 / 下载量 计算。
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
        判断种子当前上传限速是否已是目标值，避免重复调用下载器接口。

        :param limit_kb: 目标限速 KB/s，0 表示不限速
        """
        if downloader_type == "qbittorrent":
            if not isinstance(torrent, dict):
                return False
            return torrent.get("up_limit") == limit_kb * 1024
        upload_limited = bool(getattr(torrent, "uploadLimited", False))
        upload_limit = int(getattr(torrent, "uploadLimit", 0) or 0)
        if limit_kb == 0:
            # 目标是不限速：只要当前没有开启限速即视为已满足
            return not upload_limited
        return upload_limited and upload_limit == limit_kb

    @staticmethod
    def _torrent_upload_speed(torrent: Any, downloader_type: str) -> float:
        """
        获取种子当前上传速度（字节/秒）。

        qBittorrent 字段 upspeed、Transmission 字段 rateUpload 均为字节/秒。
        """
        if downloader_type == "qbittorrent":
            if not isinstance(torrent, dict):
                return 0.0
            try:
                return float(torrent.get("upspeed") or 0)
            except (TypeError, ValueError):
                return 0.0
        try:
            return float(getattr(torrent, "rateUpload", 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _torrent_completion_time(torrent: Any, downloader_type: str) -> int:
        """
        获取种子下载完成时间（Unix 时间戳，秒），未完成时为 0。

        qBittorrent 字段 completion_on、Transmission 字段 doneDate。
        """
        if downloader_type == "qbittorrent":
            if not isinstance(torrent, dict):
                return 0
            try:
                return int(torrent.get("completion_on") or 0)
            except (TypeError, ValueError):
                return 0
        try:
            return int(getattr(torrent, "doneDate", 0) or 0)
        except (TypeError, ValueError):
            return 0

    # ---------------------------------------------------------------- 监控超时取消

    def _check_complete_timeout(
        self, service_name: str, torrent: Any, downloader_type: str, torrent_hash: str, limit: int, now: float
    ) -> bool:
        """
        判断种子是否应因「下载完成后超时仍达不到限速值」而取消监控。

        规则：种子下载完成后，若在设定秒数内上传速度始终达不到限速值，则取消监控。
        - 上传速度曾达到限速值（或本轮已达到）的种子不会被取消；
        - 尚未下载完成的种子不参与判断。
        """
        completion_ts = self._torrent_completion_time(torrent, downloader_type)
        if not completion_ts:
            return False
        speed_bps = self._torrent_upload_speed(torrent, downloader_type)
        if speed_bps >= limit * 1024:
            # 上传速度已达到限速值，标记为达标，不再因本规则取消
            self._speed_ok.setdefault(service_name, {})[torrent_hash] = True
            return False
        if self._speed_ok.get(service_name, {}).get(torrent_hash):
            return False
        # 下载完成已超过设定秒数，且上传速度仍未达到限速值
        return now - completion_ts >= self._complete_timeout

    def _check_limit_timeout(
        self, service_name: str, torrent: Any, downloader_type: str, torrent_hash: str, limit: int, now: float
    ) -> bool:
        """
        判断已限速种子是否应因「限速后超时」而取消监控。

        规则：种子被限速后，持续限速或上传速度低于限速值 80% 达到设定秒数时取消监控。
        """
        # 持续限速计时：从本次设置限速起算
        limit_time = self._limited_times.get(service_name, {}).get(torrent_hash)
        if limit_time and now - limit_time >= self._limit_timeout:
            return True
        # 上传速度低于限速值 80% 的连续时长计时
        speed_bps = self._torrent_upload_speed(torrent, downloader_type)
        if speed_bps < limit * 1024 * 0.8:
            slow_since = self._slow_since.get(service_name, {}).get(torrent_hash)
            if slow_since is None:
                self._slow_since.setdefault(service_name, {})[torrent_hash] = now
            elif now - slow_since >= self._limit_timeout:
                return True
        else:
            self._slow_since.get(service_name, {}).pop(torrent_hash, None)
        return False

    def _cancel_monitoring(self, service_name: str, torrent_hash: str, torrent_name: str, reason: str):
        """
        取消对单个种子的监控：移出限速记录并清理计时状态，后续轮询不再设置限速。

        下载器中的限速值保持现状，插件不再重复设置。
        """
        self._limited_hashes.get(service_name, set()).discard(torrent_hash)
        self._canceled_hashes.setdefault(service_name, set()).add(torrent_hash)
        self._limited_times.get(service_name, {}).pop(torrent_hash, None)
        self._slow_since.get(service_name, {}).pop(torrent_hash, None)
        self._speed_ok.get(service_name, {}).pop(torrent_hash, None)
        logger.info(f"{self.LOG_TAG}[{service_name}] 种子 [{torrent_name}] 已取消监控（{reason}），不再设置限速")

    # ---------------------------------------------------------------- 通知
    def _send_limit_notify(self, site: str, torrent_name: str, limit: int, channels: List[str]) -> bool:
        """
        发送单条限速通知：{站点}所下的{种子}已经限速{速度} KB/s，变量加粗；支持多个通知渠道。

        注意：故意不传 mtype（消息类型），以绕过 MoviePilot 通知渠道的
        「通知场景」开关过滤——用户已在本插件中显式勾选渠道，应保证必定送达。
        """
        if limit <= 0:
            return False
        site = (site or "").strip()
        name = (torrent_name or "").strip() or "未知种子"
        if site:
            text = f"**{site}**所下的**{name}**已经限速**{limit}** KB/s"
        else:
            text = f"**{name}**已经限速**{limit}** KB/s"
        sent = False
        for channel in channels:
            notify_channel = self._NOTIFY_TYPE_MAP.get(channel)
            if not notify_channel:
                continue
            try:
                self.post_message(
                    channel=notify_channel,
                    title="【QB上传限速】",
                    text=text,
                    link=settings.MP_DOMAIN(f"#/plugins?tab=installed&id={self.__class__.__name__}"),
                )
                sent = True
            except Exception as err:
                logger.error(f"{self.LOG_TAG}发送限速通知失败（{channel}）：{err}")
        return sent

    def _send_test_notify_if_needed(self) -> bool:
        """
        点击「立即运行一次」时自动发送一次测试通知（支持多个渠道），仅首次发送。
        """
        channels = self._normalize_channels(self._notify_channel)
        if not channels:
            return False
        try:
            if self.get_data("notify_test_sent"):
                logger.info(f"{self.LOG_TAG}测试通知已发送过（仅首次发送），本次跳过")
                return False
        except Exception:
            pass

        # 测试通知的站点取第一个勾选站点，未勾选时使用「测试站点」
        site = next((str(name).strip() for name in (self._sites or []) if str(name).strip()), "")
        if not site:
            site = "测试站点"
        text = f"**{site}**所下的**测试种子**已经限速**{self._upload_limit}** KB/s"
        sent = False
        for channel in channels:
            notify_channel = self._NOTIFY_TYPE_MAP.get(channel)
            if not notify_channel:
                continue
            try:
                self.post_message(
                    channel=notify_channel,
                    title="【QB上传限速】测试通知",
                    text=text,
                    link=settings.MP_DOMAIN(f"#/plugins?tab=installed&id={self.__class__.__name__}"),
                )
                sent = True
            except Exception as err:
                logger.error(f"{self.LOG_TAG}发送测试通知失败（{channel}）：{err}")
        if sent:
            self.save_data("notify_test_sent", True)
            logger.info(f"{self.LOG_TAG}已发送测试通知（仅首次发送）")
        return sent

    @staticmethod
    def _normalize_channels(value: Any) -> List[str]:
        """
        将通知渠道配置规范化为去重后的字符串列表，兼容旧版单个字符串配置。
        """
        if value is None:
            return []
        if isinstance(value, str):
            raw = [value]
        else:
            try:
                raw = list(value)
            except TypeError:
                raw = [value]
        channels = []
        for item in raw:
            item = str(item or "").strip()
            if item and item not in channels:
                channels.append(item)
        return channels

    # ---------------------------------------------------------------- 恢复与调度

    def _restore_limits(self, downloaders: Optional[List[str]] = None):
        """
        将本插件限速过的种子恢复为不限速。

        用于停用/卸载插件时调用，保证不残留限速状态。
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
        """启动后台调度器（存在任务时）。"""
        if self._scheduler and self._scheduler.get_jobs():
            self._scheduler.print_jobs()
            self._scheduler.start()

    def _stop_scheduler(self):
        """停止并清理后台调度器。"""
        try:
            if getattr(self, "_scheduler", None):
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown(wait=False)
                self._scheduler = None
        except Exception as err:
            logger.error(f"{self.LOG_TAG}停止定时任务失败：{err}")

    # ---------------------------------------------------------------- 工具方法

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        """
        安全转换为整数：仅接受整数或整数字符串，拒绝小数与科学计数法，转换失败时返回默认值。

        :param value: 待转换值（可为数字或字符串）
        :param default: 转换失败时的默认值
        """
        if value is None or isinstance(value, bool):
            return default
        try:
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                # 整数值的浮点（如 30.0）视为合法整数，非整数值拒绝
                return int(value) if value.is_integer() else default
            text = str(value).strip()
            # 含小数点或科学计数法标记的字符串不是正整数
            if not text or any(c in text for c in ".eE"):
                return default
            return int(text)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _format_limit(limit: int) -> str:
        """格式化限速显示。"""
        return "不限速" if limit <= 0 else f"{limit} KB/s"
