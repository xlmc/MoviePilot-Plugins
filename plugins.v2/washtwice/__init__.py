# -*- coding: utf-8 -*-
"""洗版两次（MoviePilot V2）

功能：
    让一个订阅的每一集最多下载两次：
    第一次只下载本批候选里优先级最低的那一档，第二次只升级到本批候选里最高的一档，
    升级完成后把该集标记为「洗版完成」，之后不再为它下载任何版本。

    - 「优先级」就是宿主的 torrent.pri_order（由过滤模块按做种数/分辨率/字幕等规则算出，
      app/modules/filter/__init__.py），数值越大代表档位越高；
    - 电视剧按集计算，电影按整部计算（宿主会为电影写 current_priority，见
      app/chain/subscribe.py:__update_movie_download_priority）；
    - 只有开启了「洗版」的订阅才会做第二次（升级）和收尾（写 100），
      没开洗版的订阅只做「首发最低档」，之后一律放行。

工作方式：
    1. 监听宿主的资源选择事件 ChainEventType.ResourceSelection
       （app/chain/download/selection.py，在排序与下载之前发出，插件可以替换候选列表）；
    2. 只处理来源是订阅的批次（origin 形如 "Subscribe|{json}"，由
       app/chain/subscribe/policy.py:get_subscribe_source_keyword 生成），
       手动搜索、RSS、其它来源的下载一律放行；
    3. 用 origin 里的订阅 ID 读出订阅，按「每集阶段」过滤候选：
         阶段 0：该集还没有任何版本 —— 只保留本批优先级最低的候选；
         阶段 1：该集已有首发版本 —— 只保留本批优先级最高的候选；
         阶段 2：该集已升级完成 —— 全部拦掉，并写 episode_priority=100；
    4. 阶段不靠「选择时就记账」，而是每次用宿主自己维护的下载事实对账：
       电视剧看 subscribe.episode_priority[集号]、电影看 subscribe.current_priority
       （宿主在下载成功后写入，见 completion.py:__record_subscribe_download_facts），
       所以下载失败会自然退回上一阶段重试，不会被永久卡死。

设计说明：
    - 只过滤候选，不改宿主的搜索、过滤、下载逻辑，也不动下载器；
    - 写库范围最小：只写 episode_priority 与 current_priority 两个字段，保留其它分集原有值；
    - 试运行（默认开启）只记日志不写库，确认口径后再正式启用；
    - 状态保存在插件数据里，重启不丢；提供 /status、/reset、/restore 三个插件接口。
"""
import datetime
import json
import traceback
from typing import Any, Dict, List, Optional, Set, Tuple

from app.core.event import Event, eventmanager
from app.db.subscribe_oper import SubscribeOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import ChainEventType

# 宿主定义的「洗版完成」优先级：某集/整部达到该值即不再搜索更高优先级的版本
# （app/chain/subscribe.py:is_best_version_complete）
BEST_VERSION_DONE_PRIORITY = 100
# 宿主对「未设置优先级」的默认取值
UNSET_PRIORITY = 0
# 订阅下载的来源前缀（app/chain/subscribe/policy.py:get_subscribe_source_keyword）
SUBSCRIBE_SOURCE_PREFIX = "Subscribe|"
# 媒体类型字符串（app/schemas/types.py:MediaType）
MEDIA_TYPE_TV = "电视剧"
MEDIA_TYPE_MOVIE = "电影"
# 每集阶段
STAGE_NEW = 0
STAGE_LOW = 1
STAGE_DONE = 2

# 「不做档位限制」的哨兵值
ALL_PRIORITIES = "all"
# 阶段说明（详情页展示）
STAGE_TEXT = {
    STAGE_NEW: "待首发（只下最低档）",
    STAGE_LOW: "待升级（只下最高档）",
    STAGE_DONE: "已完成（不再洗版）",
}


class WashTwice(_PluginBase):
    """洗版两次插件。

    每集最多下载两次：首发最低档、升级最高档，之后不再洗版。
    """

    # 插件名称
    plugin_name = "洗版两次"
    # 插件描述
    plugin_desc = "每集只下载两次：首发只下本批最低档，只升级一次到最高档，之后不再洗版。"
    # 插件图标
    plugin_icon = "washtwice.svg"
    # 插件版本
    plugin_version = "2.0.0"
    # 插件作者
    plugin_author = "xlmc"
    # 作者主页
    author_url = "https://github.com/xlmc"
    # 插件配置项 ID 前缀
    plugin_config_prefix = "washtwice_"
    # 加载顺序
    plugin_order = 33
    # 可使用的用户级别
    auth_level = 1

    # 日志前缀
    LOG_TAG = "[WashTwice] "

    def __init__(self):
        """初始化运行时状态，配置项由 init_plugin 注入。"""
        super().__init__()
        # 是否启用
        self._enabled = False
        # 试运行：只记录不写库、不拦截
        self._dry_run = True
        # 是否处理电视剧
        self._handle_tv = True
        # 是否处理电影
        self._handle_movie = True
        # 首发是否只下最低档
        self._lowest_first = True
        # 升级完成后是否收尾（写 100，彻底停止该集洗版）
        self._cap_after_upgrade = True
        # 每集阶段：订阅 ID -> 集 key -> 状态
        self._state: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # 订阅标题缓存：订阅 ID -> 标题（详情页展示用）
        self._labels: Dict[str, str] = {}
        # 最近一次处理摘要
        self._last_run: Dict[str, Any] = {}

    def init_plugin(self, config: dict = None):
        """初始化插件：读取配置与上次状态。

        :param config: 插件配置字典，来自宿主的插件配置存储
        """
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._dry_run = bool(config.get("dry_run", True))
        self._handle_tv = bool(config.get("handle_tv", True))
        self._handle_movie = bool(config.get("handle_movie", True))
        self._lowest_first = bool(config.get("lowest_first", True))
        self._cap_after_upgrade = bool(config.get("cap_after_upgrade", True))

        # 恢复上次的每集阶段，重启后不用重新数一遍
        saved = self.get_data("state")
        self._state = saved if isinstance(saved, dict) else {}
        labels = self.get_data("labels")
        self._labels = labels if isinstance(labels, dict) else {}
        self._last_run = self.get_data("last_run") or {}

        # 把规范化后的配置写回，避免表单里残留空值/非法值
        if self._current_config() != config:
            self.update_config(self._current_config())

        logger.info(
            f"{self.LOG_TAG}插件{'已启用' if self._enabled else '未启用'}"
            f"{'（试运行）' if self._dry_run else ''}，已记录 {len(self._state)} 个订阅的洗版阶段"
        )

    def get_state(self) -> bool:
        """返回插件当前是否启用。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """没有远程命令，返回空列表。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """提供查看与还原状态的插件接口。"""
        return [
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "summary": "查看每集洗版阶段",
                "description": "返回所有被记录过的订阅与每集的当前阶段",
            },
            {
                "path": "/reset",
                "endpoint": self.api_reset,
                "methods": ["GET"],
                "summary": "清空洗版阶段记录",
                "description": "只清空插件记录，不改订阅数据；下次搜索会重新从「首发最低档」开始",
            },
            {
                "path": "/restore",
                "endpoint": self.api_restore,
                "methods": ["GET"],
                "summary": "还原被写成的洗版完成标记",
                "description": "把插件写过的 episode_priority / current_priority=100 还原成写入前的值",
            },
        ]

    def get_page(self) -> Optional[List[dict]]:
        """插件详情页：展示每集阶段与最近一次处理摘要。"""
        rows: List[dict] = []
        for subscribe_id, episodes in (self._state or {}).items():
            if not isinstance(episodes, dict):
                continue
            for key, record in episodes.items():
                if not isinstance(record, dict):
                    continue
                stage = self._as_int(record.get("stage"), STAGE_NEW)
                rows.append(
                    {
                        "title": self._labels.get(subscribe_id) or subscribe_id,
                        "episode": key,
                        "stage": STAGE_TEXT.get(stage, str(stage)),
                        "first": record.get("first") or "",
                        "target": record.get("target") or "",
                    }
                )
        rows.sort(key=lambda item: (item["title"], item["episode"]))

        page: List[dict] = [
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
                                    "type": "info" if self._enabled else "warning",
                                    "variant": "tonal",
                                    "title": "洗版两次",
                                    "text": self._page_summary(),
                                },
                            }
                        ],
                    }
                ],
            }
        ]

        if rows:
            page.append(
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
                                            {"title": "订阅", "key": "title"},
                                            {"title": "集", "key": "episode"},
                                            {"title": "阶段", "key": "stage"},
                                            {"title": "首发档", "key": "first"},
                                            {"title": "升级档", "key": "target"},
                                        ],
                                        "items": rows,
                                        "density": "compact",
                                        "hover": True,
                                    },
                                }
                            ],
                        }
                    ],
                }
            )

        page.append(
            {
                "component": "VAlert",
                "props": {
                    "type": "warning",
                    "variant": "tonal",
                    "text": "说明：插件只在订阅搜索的资源选择阶段过滤候选，不改下载器与订阅搜索逻辑。"
                            "每集第一次只保留本批最低档，第二次只保留本批最高档，之后写 100 停止该集洗版。"
                            "「试运行」下只记日志，不会拦截也不会写库。",
                },
            }
        )
        return page

    def stop_service(self):
        """本插件只监听事件，没有需要停止的服务。"""
        return

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """插件配置页面（Vuetify JSON 配置），返回 (表单, 默认值)。"""
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
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "dry_run",
                                            "label": "试运行（只记日志）",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "lowest_first",
                                            "label": "首发只下最低档",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "cap_after_upgrade",
                                            "label": "升级后停止洗版",
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
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "handle_tv",
                                            "label": "处理电视剧",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "handle_movie",
                                            "label": "处理电影",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "text": "「首发只下最低档」关闭时，第一次也放行全部候选；"
                                                    "「升级后停止洗版」只对开启了洗版的订阅生效，"
                                                    "会写入分集优先级 100 让该集不再参与洗版。",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], self._current_config()

    # ---- 事件入口 ----

    @eventmanager.register(ChainEventType.ResourceSelection)
    def on_resource_selection(self, event: Event):
        """资源选择事件：按「最低档 / 最高档 / 已完成」三态过滤候选。"""
        try:
            self._handle_event(event)
        except Exception as err:
            logger.error(f"{self.LOG_TAG}处理资源选择事件失败：{err}\n{traceback.format_exc()}")

    def _handle_event(self, event: Event):
        """处理一次资源选择事件。"""
        if not self._enabled:
            return
        data = getattr(event, "event_data", None)
        if not data:
            return
        origin = str(getattr(data, "origin", "") or "")
        if not origin.startswith(SUBSCRIBE_SOURCE_PREFIX):
            # 手动下载 / RSS / 其它来源一律放行
            return
        subscribe_id = self._parse_subscribe_id(origin)
        if subscribe_id is None:
            return
        subscribe = SubscribeOper().get(subscribe_id)
        if not subscribe:
            return
        is_movie = str(getattr(subscribe, "type", "") or "") == MEDIA_TYPE_MOVIE
        if is_movie and not self._handle_movie:
            return
        if not is_movie and not self._handle_tv:
            return

        contexts = list(getattr(data, "contexts", None) or [])
        if not contexts:
            return

        kept, notes = self._filter(subscribe, contexts, is_movie)
        self._remember(subscribe, is_movie, contexts, kept, notes)

        if len(kept) == len(contexts):
            return
        if self._dry_run:
            logger.info(
                f"{self.LOG_TAG}[试运行] {self._title(subscribe)} 本批 {len(contexts)} 个候选，"
                f"按规则只保留 {len(kept)} 个：{self._describe_priorities(contexts)}"
            )
            return

        data.updated = True
        data.updated_contexts = kept
        data.source = "洗版两次"
        logger.info(
            f"{self.LOG_TAG}{self._title(subscribe)} 本批 {len(contexts)} 个候选，"
            f"保留 {len(kept)} 个：{self._describe_priorities(kept)}"
        )

    # ---- 核心：候选过滤 ----

    def _filter(self, subscribe, contexts: List[Any], is_movie: bool) -> Tuple[List[Any], List[str]]:
        """按每集阶段过滤候选。

        :param subscribe: 订阅记录
        :param contexts: 本批候选上下文
        :param is_movie: 是否电影
        :return: (保留的上下文列表, 本轮的阶段变化说明)
        """
        subscribe_id = str(getattr(subscribe, "id", ""))
        state = self._state.setdefault(subscribe_id, {})
        season = self._as_int(getattr(subscribe, "season", None), 0)
        target = [1] if is_movie else self._target_episodes(subscribe)
        priorities = self._priorities(subscribe, target, is_movie)
        best_version = self._as_int(getattr(subscribe, "best_version", 0), 0) > 0
        # 只有开启了洗版的订阅才收尾：没开洗版的订阅宿主要靠「缺失集」重下，
        # 写 100 或永久拦截会让「删了以后再也下不回来」
        cap = self._cap_after_upgrade and best_version

        entries = self._entries(contexts, target, is_movie)
        priorities_by_episode = self._priorities_by_episode(entries)

        notes: List[str] = []
        # 1) 用宿主的下载事实推进阶段
        done = self._reconcile(state, priorities, target, season, is_movie)
        # 2) 已经没有更高档可升的集，直接收尾
        for episode in target:
            key = self._key(season, episode, is_movie)
            record = state.get(key) or {}
            if self._as_int(record.get("stage"), STAGE_NEW) != STAGE_LOW:
                continue
            first = self._as_int(record.get("first"), UNSET_PRIORITY)
            candidates = priorities_by_episode.get(episode) or []
            if candidates and max(candidates) <= first and episode not in done:
                record["stage"] = STAGE_DONE
                done.append(episode)
        # 3) 收尾：写 100，让宿主不再为这些集搜索
        if done:
            self._mark_done(subscribe, done, state, priorities, target, season, is_movie, cap, notes)

        # 4) 计算本批每集允许的档位
        allowed = self._allowed_priorities(priorities_by_episode, state, season, is_movie, cap)

        # 5) 过滤候选
        kept: List[Any] = []
        for entry in entries:
            episodes = entry.get("episodes")
            if episodes is None:
                # 拿不到集数信息（既没有 allowed_episodes 也没有 episode_list）：
                # 无法判断档位，原样放行，避免整包资源被误拦
                kept.append(entry["context"])
                continue
            if not episodes:
                # 候选不覆盖任何目标集（例如开始集数之外的集），直接丢弃
                continue
            if all(self._accept(entry["pri"], allowed.get(episode)) for episode in episodes):
                kept.append(entry["context"])

        # 6) 记录阶段 1 的升级目标：本批放行的最高档
        for episode in target:
            key = self._key(season, episode, is_movie)
            record = state.get(key) or {}
            if self._as_int(record.get("stage"), STAGE_NEW) != STAGE_LOW:
                continue
            tier = allowed.get(episode)
            if not isinstance(tier, int):
                continue
            if tier > self._as_int(record.get("first"), UNSET_PRIORITY):
                record["target"] = tier

        return kept, notes

    def _entries(self, contexts: List[Any], target: List[int], is_movie: bool) -> List[Dict[str, Any]]:
        """把候选上下文整理成「优先级 + 覆盖集数」。"""
        target_set = set(target)
        entries: List[Dict[str, Any]] = []
        for context in contexts:
            torrent = getattr(context, "torrent_info", None)
            priority = self._as_int(getattr(torrent, "pri_order", UNSET_PRIORITY), UNSET_PRIORITY)
            entries.append(
                {
                    "context": context,
                    "pri": priority,
                    "episodes": self._context_episodes(context, target_set, is_movie),
                }
            )
        return entries

    def _context_episodes(self, context, target_set: Set[int], is_movie: bool) -> Optional[Set[int]]:
        """候选覆盖的集号集合。

        返回空集表示候选不覆盖任何目标集（应丢弃）；返回 None 表示完全拿不到集数信息（放行）。
        """
        if is_movie:
            return {1}

        # 宿主在分集洗版时会用 allowed_episodes 限定本批可下载的集
        allowed = getattr(context, "allowed_episodes", None)
        if allowed:
            return self._intersect(allowed, target_set)

        meta = getattr(context, "meta_info", None)
        episode_list = getattr(meta, "episode_list", None) if meta else None
        if episode_list:
            return self._intersect(episode_list, target_set)
        return None

    @staticmethod
    def _intersect(values: Any, target_set: Set[int]) -> Set[int]:
        """把集号列表换算成目标集子集。"""
        episodes: Set[int] = set()
        for value in values:
            try:
                number = int(value)
            except (TypeError, ValueError):
                continue
            if number in target_set:
                episodes.add(number)
        return episodes

    @staticmethod
    def _priorities_by_episode(entries: List[Dict[str, Any]]) -> Dict[int, List[int]]:
        """本批候选中每个集号出现过的优先级。"""
        result: Dict[int, List[int]] = {}
        for entry in entries:
            episodes = entry.get("episodes")
            if episodes is None:
                continue
            for episode in episodes:
                result.setdefault(episode, []).append(entry["pri"])
        return result

    def _allowed_priorities(
            self,
            priorities_by_episode: Dict[int, List[int]],
            state: Dict[str, Dict[str, Any]],
            season: int,
            is_movie: bool,
            cap: bool,
    ) -> Dict[int, Optional[int]]:
        """每集本轮允许的优先级。

        None 表示拦掉该集的全部候选；ALL_PRIORITIES 表示不做档位限制。
        """
        allowed: Dict[int, Any] = {}
        for episode, candidates in priorities_by_episode.items():
            record = state.get(self._key(season, episode, is_movie)) or {}
            stage = self._as_int(record.get("stage"), STAGE_NEW)
            if stage >= STAGE_DONE:
                # 洗版订阅：彻底拦掉，保证每集只下两次；
                # 非洗版订阅：宿主靠缺失集重下，永久拦截会让删掉的文件再也下不回来
                allowed[episode] = None if cap else ALL_PRIORITIES
                continue
            if stage <= STAGE_NEW and self._lowest_first:
                allowed[episode] = min(candidates)
            else:
                allowed[episode] = max(candidates)
        return allowed

    @staticmethod
    def _accept(priority: int, allowed: Any) -> bool:
        """候选是否落在本轮允许的档位上。"""
        if allowed is ALL_PRIORITIES:
            return True
        if allowed is None:
            return False
        return priority == allowed

    def _reconcile(
            self,
            state: Dict[str, Dict[str, Any]],
            priorities: Dict[str, int],
            target: List[int],
            season: int,
            is_movie: bool,
    ) -> List[int]:
        """用宿主的下载事实推进每集阶段，返回本轮进入「已完成」的集。

        宿主的记录口径：
            - 电视剧：下载成功后 episode_priority[集号] = max(旧值, pri_order)
              （app/chain/subscribe.py:__record_subscribe_download_facts）
            - 电影：下载成功后 current_priority = max(pri_order)
              （app/chain/subscribe.py:__update_movie_download_priority）

        这样「下载失败」不会推进阶段：宿主的记录没有变化，下一轮仍按原阶段重试。
        """
        done: List[int] = []
        for episode in target:
            key = self._key(season, episode, is_movie)
            record = state.get(key)
            if not isinstance(record, dict):
                record = {"stage": STAGE_NEW, "first": UNSET_PRIORITY, "target": UNSET_PRIORITY, "prev": None}
                state[key] = record
            current = self._as_int(priorities.get(str(episode)), UNSET_PRIORITY)
            stage = self._as_int(record.get("stage"), STAGE_NEW)

            if current <= UNSET_PRIORITY:
                # 宿主侧没有该集的版本记录：回到阶段 0，允许重新走一次
                if stage != STAGE_NEW:
                    state[key] = {"stage": STAGE_NEW, "first": UNSET_PRIORITY,
                                  "target": UNSET_PRIORITY, "prev": None}
                continue

            if stage <= STAGE_NEW:
                record["stage"] = STAGE_LOW
                record["first"] = current
                continue

            if stage == STAGE_LOW:
                goal = self._as_int(record.get("target"), UNSET_PRIORITY)
                if goal and current >= goal:
                    record["stage"] = STAGE_DONE
                    done.append(episode)
                elif current > self._as_int(record.get("first"), UNSET_PRIORITY):
                    record["first"] = current
        return done

    def _mark_done(
            self,
            subscribe,
            episodes: List[int],
            state: Dict[str, Dict[str, Any]],
            priorities: Dict[str, int],
            target: List[int],
            season: int,
            is_movie: bool,
            cap: bool,
            notes: List[str],
    ):
        """把升级完成的集标记为洗版完成（写 100）。"""
        if not episodes:
            return
        label = self._title(subscribe)
        if not cap:
            notes.append(f"{label} 第 {sorted(episodes)} 集已完成一次升级")
            return

        if is_movie:
            current = self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY)
            if current >= BEST_VERSION_DONE_PRIORITY:
                return
            record = state.get(self._key(season, 1, True)) or {}
            record["prev"] = current
            state[self._key(season, 1, True)] = record
            self._update_subscribe(subscribe, {"current_priority": BEST_VERSION_DONE_PRIORITY})
            notes.append(f"{label} 整部洗版完成")
            return

        changed: List[int] = []
        for episode in episodes:
            key = self._key(season, episode, is_movie)
            record = state.get(key) or {}
            before = self._as_int(priorities.get(str(episode)), UNSET_PRIORITY)
            if before >= BEST_VERSION_DONE_PRIORITY:
                continue
            record["prev"] = before
            state[key] = record
            priorities[str(episode)] = BEST_VERSION_DONE_PRIORITY
            changed.append(episode)
        if not changed:
            return

        # 按宿主口径重算整部优先级：目标集范围内的最小值
        # （app/chain/subscribe.py:get_best_version_current_priority）
        new_priority = min(
            (self._as_int(priorities.get(str(episode)), UNSET_PRIORITY) for episode in target),
            default=BEST_VERSION_DONE_PRIORITY,
        )
        self._update_subscribe(
            subscribe,
            {"episode_priority": priorities, "current_priority": new_priority},
        )
        notes.append(f"{label} 第 {sorted(changed)} 集洗版完成")

    def _update_subscribe(self, subscribe, payload: Dict[str, Any]):
        """写回订阅；试运行时只记录日志。"""
        payload = dict(payload)
        payload.setdefault("last_update", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        title = self._title(subscribe)
        if self._dry_run:
            logger.info(f"{self.LOG_TAG}[试运行] {title} 将写入：{payload}")
            return
        try:
            SubscribeOper().update(subscribe.id, payload)
            logger.info(f"{self.LOG_TAG}{title} 已写入：{payload}")
        except Exception as err:
            logger.error(f"{self.LOG_TAG}{title} 写回订阅失败：{err}")

    # ---- 订阅数据口径（与宿主 app/chain/subscribe.py 保持一致） ----

    def _target_episodes(self, subscribe) -> List[int]:
        """订阅的目标集号范围（宿主口径：range(start_episode or 1, total_episode + 1)）。"""
        start = self._as_int(getattr(subscribe, "start_episode", None), 1) or 1
        total = self._as_int(getattr(subscribe, "total_episode", None), 0)
        if total < start:
            return []
        return list(range(start, total + 1))

    def _priorities(self, subscribe, target: List[int], is_movie: bool) -> Dict[str, int]:
        """宿主视角的「该集当前已有版本的优先级」。

        电视剧取 episode_priority，并在其为空时用 current_priority 回填目标集
        （app/chain/subscribe.py:get_episode_priority）；
        电影直接取 current_priority（宿主下载成功后写入的就是它）。
        """
        if is_movie:
            return {"1": self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY)}

        stored = getattr(subscribe, "episode_priority", None) or {}
        priorities: Dict[str, int] = {}
        for key, value in stored.items():
            try:
                priorities[str(int(key))] = self._as_int(value, UNSET_PRIORITY)
            except (TypeError, ValueError):
                continue

        if not priorities and not self._as_int(getattr(subscribe, "best_version_full", 0), 0):
            current = getattr(subscribe, "current_priority", None)
            if current is not None:
                priorities = {
                    str(episode): self._as_int(current, UNSET_PRIORITY) for episode in target
                }
        return priorities

    # ---- 状态存取 ----

    @staticmethod
    def _key(season: int, episode: int, is_movie: bool) -> str:
        """每集状态 key：电影固定 m-1，电视剧 S{季}E{集}。"""
        if is_movie:
            return "m-1"
        return f"s{season}e{episode}"

    @staticmethod
    def _parse_subscribe_id(origin: str) -> Optional[int]:
        """从订阅来源字符串里取订阅 ID。"""
        try:
            info = json.loads(origin.split("|", 1)[1])
        except (IndexError, ValueError, TypeError):
            return None
        if not isinstance(info, dict):
            return None
        try:
            return int(info.get("id"))
        except (TypeError, ValueError):
            return None

    def _remember(self, subscribe, is_movie: bool, contexts: List[Any], kept: List[Any], notes: List[str]):
        """记录最近一次处理摘要并落盘状态。"""
        subscribe_id = str(getattr(subscribe, "id", ""))
        self._labels[subscribe_id] = self._title(subscribe)
        self._last_run = {
            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "title": self._title(subscribe),
            "type": "电影" if is_movie else "电视剧",
            "total": len(contexts),
            "kept": len(kept),
            "notes": notes,
        }
        try:
            self.save_data("state", self._state)
            self.save_data("labels", self._labels)
            self.save_data("last_run", self._last_run)
        except Exception as err:
            logger.error(f"{self.LOG_TAG}保存状态失败：{err}")

    # ---- 插件接口 ----

    def api_status(self) -> Dict[str, Any]:
        """查看每集阶段。"""
        episodes: Dict[str, Dict[str, Any]] = {}
        for subscribe_id, records in (self._state or {}).items():
            if not isinstance(records, dict):
                continue
            label = self._labels.get(subscribe_id) or subscribe_id
            for key, record in records.items():
                if not isinstance(record, dict):
                    continue
                stage = self._as_int(record.get("stage"), STAGE_NEW)
                episodes.setdefault(label, {})[key] = {
                    "stage": stage,
                    "stage_text": STAGE_TEXT.get(stage, str(stage)),
                    "first": record.get("first"),
                    "target": record.get("target"),
                }
        return {
            "enabled": self._enabled,
            "dry_run": self._dry_run,
            "last_run": self._last_run,
            "episodes": episodes,
        }

    def api_reset(self) -> Dict[str, Any]:
        """清空插件记录（不改订阅数据）。"""
        count = sum(len(v) for v in (self._state or {}).values() if isinstance(v, dict))
        self._state = {}
        self._labels = {}
        self._last_run = {}
        self.save_data("state", self._state)
        self.save_data("labels", self._labels)
        self.save_data("last_run", self._last_run)
        logger.info(f"{self.LOG_TAG}已清空 {count} 条洗版阶段记录")
        return {"success": True, "cleared": count}

    def api_restore(self) -> Dict[str, Any]:
        """把插件写过的 100 还原成写入前的值。"""
        restored: List[str] = []
        for subscribe_id, records in (self._state or {}).items():
            if not isinstance(records, dict):
                continue
            try:
                subscribe = SubscribeOper().get(int(subscribe_id))
            except (TypeError, ValueError):
                subscribe = None
            if not subscribe:
                continue
            is_movie = str(getattr(subscribe, "type", "") or "") == MEDIA_TYPE_MOVIE
            if is_movie:
                record = records.get("m-1") or {}
                prev = record.get("prev")
                if prev is None:
                    continue
                if self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY) \
                        != BEST_VERSION_DONE_PRIORITY:
                    continue
                self._update_subscribe(subscribe, {"current_priority": self._as_int(prev, UNSET_PRIORITY)})
                restored.append(f"{self._title(subscribe)} 整部")
                continue

            stored = getattr(subscribe, "episode_priority", None) or {}
            priorities: Dict[str, int] = {}
            for key, value in stored.items():
                try:
                    priorities[str(int(key))] = self._as_int(value, UNSET_PRIORITY)
                except (TypeError, ValueError):
                    continue
            changed: List[int] = []
            for key, record in records.items():
                if not isinstance(record, dict):
                    continue
                prev = record.get("prev")
                if prev is None:
                    continue
                try:
                    episode = int(key.split("e")[-1])
                except (TypeError, ValueError):
                    continue
                if self._as_int(priorities.get(str(episode)), UNSET_PRIORITY) != BEST_VERSION_DONE_PRIORITY:
                    continue
                priorities[str(episode)] = self._as_int(prev, UNSET_PRIORITY)
                changed.append(episode)
            if not changed:
                continue
            target = self._target_episodes(subscribe)
            new_priority = min(
                (self._as_int(priorities.get(str(episode)), UNSET_PRIORITY) for episode in target),
                default=UNSET_PRIORITY,
            )
            self._update_subscribe(
                subscribe,
                {"episode_priority": priorities, "current_priority": new_priority},
            )
            restored.append(f"{self._title(subscribe)} 第 {sorted(changed)} 集")

        self._state = {}
        self._labels = {}
        self.save_data("state", self._state)
        self.save_data("labels", self._labels)
        logger.info(f"{self.LOG_TAG}已还原：{restored or '无'}")
        return {"success": True, "restored": restored}

    # ---- 小工具 ----

    def _page_summary(self) -> str:
        """详情页顶部摘要。"""
        if not self._enabled:
            return "插件未启用。"
        parts = ["已启用", "试运行（只记日志）" if self._dry_run else "正式生效"]
        last = self._last_run or {}
        if last:
            parts.append(
                f"最近一次 {last.get('time')}：{last.get('title')} "
                f"候选 {last.get('total')} → 保留 {last.get('kept')}"
            )
            for note in last.get("notes") or []:
                parts.append(note)
        else:
            parts.append("还没有处理过订阅下载")
        return "；".join(parts)

    @staticmethod
    def _describe_priorities(contexts: List[Any]) -> str:
        """把候选的优先级列出来，便于核对档位。"""
        values = []
        for context in contexts:
            torrent = getattr(context, "torrent_info", None)
            values.append(str(WashTwice._as_int(getattr(torrent, "pri_order", 0), 0)))
        return "[" + ", ".join(values) + "]"

    @staticmethod
    def _title(subscribe) -> str:
        """订阅的展示名。"""
        name = getattr(subscribe, "name", "") or ""
        year = getattr(subscribe, "year", "") or ""
        season = getattr(subscribe, "season", None)
        if str(getattr(subscribe, "type", "") or "") == MEDIA_TYPE_MOVIE:
            return f"{name} ({year})" if year else str(name)
        return f"{name} ({year}) 第 {season} 季" if year else f"{name} 第 {season} 季"

    def _current_config(self) -> Dict[str, Any]:
        """当前配置的规范化结果。"""
        return {
            "enabled": self._enabled,
            "dry_run": self._dry_run,
            "lowest_first": self._lowest_first,
            "cap_after_upgrade": self._cap_after_upgrade,
            "handle_tv": self._handle_tv,
            "handle_movie": self._handle_movie,
        }

    @staticmethod
    def _as_int(value: Any, default: int) -> int:
        """把配置/数据库里的值安全转成整数。"""
        if isinstance(value, bool):
            return int(value)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
