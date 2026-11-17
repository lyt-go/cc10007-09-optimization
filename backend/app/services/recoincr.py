"""推荐场景增量命中重算业务规则。

背景：存量重算链路在推荐场景或冷启动策略变化时会重算全部已发布策略，
无关策略的评分与复用次数也被刷新。本模块只按“场景边界”识别受影响候选，
做局部收敛，产出状态确定的「推荐场景增量命中明细数据集」。

核心约定（与存量清单处理约定保持一致，状态流转只在 services 层发生）：
1. 增量范围只来自“推荐场景变化记录”，按变化场景定位引用该场景的策略库候选；
2. 受影响场景与已完成推荐结果分开保留：批次明细每个场景一条，独立状态；
3. 重试只能补写未完成场景，已完成场景的结果不允许被覆盖；
4. 同一业务记录（业务键 = 批次号 + 场景 + 策略编号）并发提交只允许一次有效写入；
5. 只落库状态确定（已完成/已保留）的命中明细，处理中不留半成品；
6. 新旧两个评估范围同时覆盖同一策略候选时，按数据版本仲裁，同一场景只保留
   一套可解释的当前推荐；
7. 每次应用都生成可按数据版本回查的快照，支持版本回滚。
"""
from __future__ import annotations

import hashlib
import threading
from datetime import datetime
from typing import Any

from app.store import store

MODULE = "recoincr"

# 增量重算用到的几张内部表，统一挂在内存仓库下
TABLE_CHANGES = "reco_scene_changes"        # 推荐场景变化记录（增量范围来源）
TABLE_BATCHES = "reco_incr_batches"         # 局部重算批次
TABLE_DETAILS = "reco_incr_details"         # 批次×场景的受影响明细（含已完成保留项）
TABLE_HITS = "reco_incr_hits"               # 推荐场景增量命中明细数据集（状态确定后才写入）
TABLE_SNAPSHOTS = "reco_incr_snapshots"     # 数据版本快照（回查/回滚）

BATCH_DRAFT = "待处理"
BATCH_RUNNING = "处理中"
BATCH_DONE = "已完成"
BATCH_PARTIAL = "部分完成"

DETAIL_PENDING = "待处理"
DETAIL_DONE = "已完成"      # 受影响场景：本次局部重算产出的新结果
DETAIL_KEPT = "已保留"      # 未受影响/已完成场景：沿用既有推荐结果，不刷新

HIT_DONE = "已完成"
HIT_KEPT = "已保留"

# 同一进程内对增量重算写入做串行化，配合业务键去重实现“只允许一次有效写入”
_write_lock = threading.RLock()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _stable_score(*parts: Any) -> int:
    """局部收敛评分：对相同业务输入始终给出相同结果，保证重试/并发可复现。"""
    digest = hashlib.md5("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()
    return 60 + int(digest[:8], 16) % 36  # 收敛到 60~95 的确定分值


def _stable_reuse(*parts: Any) -> int:
    digest = hashlib.sha1("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()
    return int(digest[:6], 16) % 200


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


class RecoincrService:
    """推荐场景增量命中重算：场景边界识别、局部收敛、幂等落库与版本回查。"""

    # ------------------------------------------------------------------ 读取
    def list_changes(self) -> list[dict[str, Any]]:
        return store.rows(TABLE_CHANGES)

    def list_batches(self, status: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(TABLE_BATCHES)
        if status:
            rows = [row for row in rows if row.get("批次状态") == status]
        return rows

    def get_batch(self, batch_no: str) -> dict[str, Any] | None:
        for row in store.rows(TABLE_BATCHES):
            if row.get("批次号") == batch_no:
                return row
        return None

    def list_details(self, batch_no: str) -> list[dict[str, Any]]:
        return [row for row in store.rows(TABLE_DETAILS) if row.get("批次号") == batch_no]

    def list_hits(
        self,
        *,
        batch_no: str | None = None,
        data_version: str | None = None,
        scene: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = store.rows(TABLE_HITS)
        if batch_no:
            rows = [row for row in rows if row.get("批次号") == batch_no]
        if data_version:
            rows = [row for row in rows if row.get("数据版本") == data_version]
        if scene:
            rows = [row for row in rows if row.get("推荐场景") == scene]
        return rows

    def list_snapshots(self) -> list[dict[str, Any]]:
        return store.rows(TABLE_SNAPSHOTS)

    def get_snapshot(self, data_version: str) -> dict[str, Any] | None:
        for row in store.rows(TABLE_SNAPSHOTS):
            if row.get("数据版本") == data_version:
                return row
        return None

    # ------------------------------------------------- 推荐场景变化记录（增量源）
    def register_change(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记一条推荐场景变化记录，作为局部重算的增量范围来源。"""
        required = ["推荐场景", "变化类型"]
        missing = [field for field in required if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        with _write_lock:
            rows = store.rows(TABLE_CHANGES)
            change = {
                "id": _next_id(rows),
                "变化单号": str(values.get("变化单号") or f"CHG-{_next_id(rows):04d}"),
                "推荐场景": str(values["推荐场景"]).strip(),
                "变化类型": str(values["变化类型"]).strip(),
                "冷启动策略": str(values.get("冷启动策略") or "").strip(),
                "变更说明": str(values.get("变更说明") or "").strip(),
                "登记时间": _now(),
                "已纳入批次": "",
            }
            rows.append(change)
            return change, []

    # --------------------------------------------------------- 场景边界识别
    def _scene_candidates(self, scene: str) -> list[dict[str, Any]]:
        """按场景边界识别受影响候选：只取引用该场景的已发布策略库记录。"""
        return [
            row
            for row in store.rows("tacticlib")
            if row.get("引用推荐场景") == scene and row.get("status") == "已发布"
        ]

    def _latest_hit(self, scene: str, tactic_no: str) -> dict[str, Any] | None:
        """取某场景×策略的当前命中结果（数据版本最大的一条）。"""
        hits = [
            row
            for row in store.rows(TABLE_HITS)
            if row.get("推荐场景") == scene and row.get("策略编号") == tactic_no
        ]
        if not hits:
            return None
        return max(hits, key=lambda row: str(row.get("数据版本", "")))

    def _scene_config(self, scene: str) -> dict[str, Any]:
        """读取某推荐场景当前生效的推荐配置（取已上线策略，回退到最近一条）。"""
        rows = [row for row in store.rows("recommend") if row.get("推荐场景") == scene]
        online = [row for row in rows if row.get("status") == "已上线"]
        chosen = online[0] if online else (rows[0] if rows else None)
        if chosen is None:
            return {"推荐场景": scene, "召回算法": "", "排序模型": "", "冷启动策略": ""}
        return {
            "推荐场景": scene,
            "召回算法": chosen.get("召回算法", ""),
            "排序模型": chosen.get("排序模型", ""),
            "冷启动策略": chosen.get("冷启动策略", ""),
        }

    # ------------------------------------------------------------- 提交局部重算
    def submit_batch(
        self,
        *,
        change_nos: list[str] | None = None,
        batch_no: str | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """从场景变化记录获取增量范围，做局部收敛并落库命中明细。

        并发提交同一业务记录时，靠幂等键 + 业务键去重，只允许一次有效写入。
        """
        with _write_lock:
            changes = store.rows(TABLE_CHANGES)
            if change_nos:
                picked = [row for row in changes if row.get("变化单号") in change_nos]
                missing = sorted(set(change_nos) - {row.get("变化单号") for row in picked})
                if missing:
                    return None, f"推荐场景变化记录不存在：{'、'.join(missing)}"
            else:
                picked = [row for row in changes if not row.get("已纳入批次")]
            if not picked:
                return None, "没有待处理的推荐场景变化记录，局部重算范围为空"

            # 幂等键：同一业务提交（相同变化单集合）重试时直接返回既有批次
            scope_key = idempotency_key or "SUBMIT:" + ",".join(
                sorted(str(row.get("变化单号")) for row in picked)
            )
            for existed in store.rows(TABLE_BATCHES):
                if existed.get("幂等键") == scope_key:
                    return existed, "相同增量范围已提交过，返回既有批次（未产生重复写入）"
            if batch_no and any(row.get("批次号") == batch_no for row in store.rows(TABLE_BATCHES)):
                return None, f"批次号 {batch_no} 已存在，请勿重复提交"

            affected_scenes: list[str] = []
            for row in picked:
                scene = str(row.get("推荐场景") or "")
                if scene and scene not in affected_scenes:
                    affected_scenes.append(scene)

            batches = store.rows(TABLE_BATCHES)
            serial = _next_id(batches)
            batch = {
                "id": serial,
                "批次号": batch_no or f"RB-{serial:04d}",
                "增量变化单": [str(row.get("变化单号")) for row in picked],
                "受影响推荐场景": affected_scenes,
                "批次状态": BATCH_RUNNING,
                "数据版本": f"V{datetime.now().strftime('%Y%m%d')}-{serial:03d}",
                "幂等键": scope_key,
                "创建时间": _now(),
                "完成时间": "",
                "已应用": False,
                "应用时间": "",
            }
            batches.append(batch)

            detail_rows = store.rows(TABLE_DETAILS)
            hit_rows = store.rows(TABLE_HITS)
            hit_keys = {
                (row.get("批次号"), row.get("推荐场景"), row.get("策略编号"))
                for row in hit_rows
            }
            detail_keys = {
                (row.get("批次号"), row.get("推荐场景"), row.get("策略编号"))
                for row in detail_rows
            }

            done_count = 0
            kept_count = 0
            candidate_total = 0
            # 场景边界内：只重算引用变化场景的已发布候选，其余场景完全不触碰
            for scene in affected_scenes:
                config = self._scene_config(scene)
                candidates = self._scene_candidates(scene)
                for tactic in candidates:
                    candidate_total += 1
                    tactic_no = str(tactic.get("策略编号"))
                    dkey = (batch["批次号"], scene, tactic_no)
                    if dkey in detail_keys:  # 并发/重试：同一业务记录只写一次
                        continue
                    score = _stable_score(
                        batch["数据版本"], scene, tactic_no,
                        config["召回算法"], config["排序模型"], config["冷启动策略"],
                    )
                    reuse = _stable_reuse(batch["数据版本"], scene, tactic_no)
                    detail = {
                        "id": _next_id(detail_rows),
                        "批次号": batch["批次号"],
                        "数据版本": batch["数据版本"],
                        "推荐场景": scene,
                        "策略编号": tactic_no,
                        "策略名称": tactic.get("策略名称", ""),
                        "明细状态": DETAIL_DONE,
                        "变化类型": "受影响-局部重算",
                        "重算评分": score,
                        "重算复用次数": reuse,
                        "原评分": tactic.get("策略评分"),
                        "原复用次数": tactic.get("使用次数"),
                        "写入时间": _now(),
                    }
                    detail_rows.append(detail)
                    detail_keys.add(dkey)
                    done_count += 1
                    self._upsert_hit(hit_rows, hit_keys, {
                        "批次号": batch["批次号"],
                        "数据版本": batch["数据版本"],
                        "推荐场景": scene,
                        "策略编号": tactic_no,
                        "策略名称": tactic.get("策略名称", ""),
                        "命中状态": HIT_DONE,
                        "策略评分": score,
                        "复用次数": reuse,
                        "召回算法": config["召回算法"],
                        "排序模型": config["排序模型"],
                        "冷启动策略": config["冷启动策略"],
                        "评估范围": "新场景配置",
                        "写入时间": _now(),
                    })

            # 受影响场景之外、已完成的推荐结果单独保留：只登记“已保留”明细，不刷新评分
            for tactic in store.rows("tacticlib"):
                if tactic.get("status") != "已发布":
                    continue
                scene = str(tactic.get("引用推荐场景") or "")
                if scene in affected_scenes:
                    continue
                tactic_no = str(tactic.get("策略编号"))
                dkey = (batch["批次号"], scene, tactic_no)
                if dkey in detail_keys:
                    continue
                latest = self._latest_hit(scene, tactic_no)
                detail_rows.append({
                    "id": _next_id(detail_rows),
                    "批次号": batch["批次号"],
                    "数据版本": batch["数据版本"],
                    "推荐场景": scene,
                    "策略编号": tactic_no,
                    "策略名称": tactic.get("策略名称", ""),
                    "明细状态": DETAIL_KEPT,
                    "变化类型": "未受影响-保留原结果",
                    "重算评分": latest.get("策略评分") if latest else tactic.get("策略评分"),
                    "重算复用次数": latest.get("复用次数") if latest else tactic.get("使用次数"),
                    "原评分": tactic.get("策略评分"),
                    "原复用次数": tactic.get("使用次数"),
                    "写入时间": _now(),
                })
                detail_keys.add(dkey)
                kept_count += 1

            batch["批次状态"] = BATCH_DONE
            batch["完成时间"] = _now()
            batch["受影响候选数"] = candidate_total
            batch["重算明细数"] = done_count
            batch["保留明细数"] = kept_count
            for row in picked:
                row["已纳入批次"] = batch["批次号"]

            # 只落库状态确定的命中明细数据集；是否应用到策略库由 apply_batch 显式触发
            return batch, "局部重算完成：仅受影响场景重算，未受影响场景保留原推荐结果（待应用到策略库）"

    # ------------------------------------------------------------- 重试补写
    def retry_batch(self, batch_no: str) -> tuple[dict[str, Any] | None, str]:
        """重试只能补写未完成场景；已完成场景与已保留结果不允许被覆盖。"""
        with _write_lock:
            batch = self.get_batch(batch_no)
            if batch is None:
                return None, f"重算批次 {batch_no} 不存在"
            if batch.get("批次状态") == BATCH_DONE:
                return batch, "批次已全部完成，重试不产生重复写入（已完成场景保持不变）"

            details = self.list_details(batch_no)
            pending = [row for row in details if row.get("明细状态") == DETAIL_PENDING]
            if not pending:
                batch["批次状态"] = BATCH_DONE
                batch["完成时间"] = _now()
                return batch, "没有未完成场景需要补写"

            hit_rows = store.rows(TABLE_HITS)
            hit_keys = {
                (row.get("批次号"), row.get("推荐场景"), row.get("策略编号"))
                for row in hit_rows
            }
            fixed = 0
            for detail in pending:
                scene = str(detail.get("推荐场景"))
                tactic_no = str(detail.get("策略编号"))
                hkey = (batch_no, scene, tactic_no)
                if hkey in hit_keys:  # 已写入过的业务记录不再补写
                    detail["明细状态"] = DETAIL_DONE
                    fixed += 1
                    continue
                config = self._scene_config(scene)
                score = _stable_score(
                    batch["数据版本"], scene, tactic_no,
                    config["召回算法"], config["排序模型"], config["冷启动策略"],
                )
                reuse = _stable_reuse(batch["数据版本"], scene, tactic_no)
                detail["明细状态"] = DETAIL_DONE
                detail["重算评分"] = score
                detail["重算复用次数"] = reuse
                detail["写入时间"] = _now()
                self._upsert_hit(hit_rows, hit_keys, {
                    "批次号": batch_no,
                    "数据版本": batch["数据版本"],
                    "推荐场景": scene,
                    "策略编号": tactic_no,
                    "策略名称": detail.get("策略名称", ""),
                    "命中状态": HIT_DONE,
                    "策略评分": score,
                    "复用次数": reuse,
                    "召回算法": config["召回算法"],
                    "排序模型": config["排序模型"],
                    "冷启动策略": config["冷启动策略"],
                    "评估范围": "新场景配置",
                    "写入时间": _now(),
                })
                fixed += 1

            still_pending = [
                row for row in self.list_details(batch_no) if row.get("明细状态") == DETAIL_PENDING
            ]
            batch["批次状态"] = BATCH_PARTIAL if still_pending else BATCH_DONE
            if not still_pending:
                batch["完成时间"] = _now()
            return batch, f"重试补写 {fixed} 个未完成场景，已完成场景未被覆盖（待应用到策略库）"

    # --------------------------------------------- 命中明细数据集写入（业务键去重）
    def _upsert_hit(
        self,
        hit_rows: list[dict[str, Any]],
        hit_keys: set[tuple[Any, Any, Any]],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """同一业务键（批次+场景+策略）只允许一次有效写入；重复提交直接返回旧记录。"""
        key = (payload["批次号"], payload["推荐场景"], payload["策略编号"])
        for row in hit_rows:
            if (row.get("批次号"), row.get("推荐场景"), row.get("策略编号")) == key:
                return row
        payload["id"] = _next_id(hit_rows)
        hit_rows.append(payload)
        hit_keys.add(key)
        return payload

    # ----------------------------- 应用到策略库：只更新引用变化场景的策略评分
    def apply_batch(self, batch_no: str) -> tuple[dict[str, Any] | None, str]:
        with _write_lock:
            return self._apply_batch_locked(batch_no)

    def _apply_batch_locked(self, batch_no: str) -> tuple[dict[str, Any] | None, str]:
        batch = self.get_batch(batch_no)
        if batch is None:
            return None, f"重算批次 {batch_no} 不存在"
        if batch.get("已应用"):
            return batch, "该数据版本已应用，重复操作不产生重复写入"
        if batch.get("批次状态") not in (BATCH_DONE, BATCH_PARTIAL):
            return None, "批次仍在处理中，只允许落库状态确定的命中明细数据集"

        version = str(batch["数据版本"])
        affected = set(batch.get("受影响推荐场景") or [])
        hits = self.list_hits(batch_no=batch_no)
        # 同一场景只保留一套当前推荐：按数据版本仲裁，旧批次命中结果降为历史版本
        current: dict[tuple[str, str], dict[str, Any]] = {}
        for hit in hits:
            current[(str(hit.get("推荐场景")), str(hit.get("策略编号")))] = hit

        updated: list[dict[str, Any]] = []
        for tactic in store.rows("tacticlib"):
            scene = str(tactic.get("引用推荐场景") or "")
            tactic_no = str(tactic.get("策略编号"))
            hit = current.get((scene, tactic_no))
            if hit is None:
                continue  # 未变化场景：原推荐结果、评分、复用次数原样保留
            if scene in affected and hit.get("命中状态") == HIT_DONE:
                tactic["策略评分"] = hit["策略评分"]
                tactic["使用次数"] = hit["复用次数"]
                tactic["最近数据版本"] = version
                tactic["更新来源批次"] = batch_no
                updated.append({
                    "策略编号": tactic_no,
                    "推荐场景": scene,
                    "策略评分": hit["策略评分"],
                    "复用次数": hit["复用次数"],
                })
            else:
                # 未受影响场景：即便数据集里有保留项，也不刷新评分与复用次数
                tactic.setdefault("最近数据版本", version)

        # 新旧评估范围仲裁：同一场景只保留版本最大的一套当前推荐
        for row in store.rows(TABLE_HITS):
            scene = str(row.get("推荐场景"))
            tactic_no = str(row.get("策略编号"))
            newest = self._latest_hit(scene, tactic_no)
            row["是否当前推荐"] = bool(newest and newest.get("id") == row.get("id"))

        snapshot = {
            "id": _next_id(store.rows(TABLE_SNAPSHOTS)),
            "数据版本": version,
            "批次号": batch_no,
            "生成时间": _now(),
            "受影响推荐场景": sorted(affected),
            "策略库快照": [dict(row) for row in store.rows("tacticlib")],
            "命中明细快照": [dict(row) for row in hits],
            "更新策略数": len(updated),
            "更新明细": updated,
        }
        store.rows(TABLE_SNAPSHOTS).append(snapshot)
        batch["已应用"] = True
        batch["应用时间"] = _now()
        batch["更新策略数"] = len(updated)
        return batch, f"数据版本 {version} 已应用：{len(updated)} 条引用变化场景的策略评分更新，未变化场景无漂移"

    # ------------------------------------------------------------- 版本回查/回滚
    def get_version_detail(self, data_version: str) -> dict[str, Any] | None:
        snapshot = self.get_snapshot(data_version)
        if snapshot is None:
            return None
        return {
            "数据版本": data_version,
            "批次号": snapshot.get("批次号"),
            "生成时间": snapshot.get("生成时间"),
            "受影响推荐场景": snapshot.get("受影响推荐场景"),
            "更新策略数": snapshot.get("更新策略数"),
            "更新明细": snapshot.get("更新明细"),
            "命中明细": snapshot.get("命中明细快照"),
            "策略库快照": snapshot.get("策略库快照"),
        }

    def rollback_version(self, data_version: str) -> tuple[dict[str, Any] | None, str]:
        """按数据版本回查并回滚策略库到该版本快照，命中明细数据集本身不可变。"""
        with _write_lock:
            snapshot = self.get_snapshot(data_version)
            if snapshot is None:
                return None, f"数据版本 {data_version} 不存在，无法回查"
            target = {str(row.get("策略编号")): row for row in snapshot.get("策略库快照", [])}
            for tactic in store.rows("tacticlib"):
                old = target.get(str(tactic.get("策略编号")))
                if old is None:
                    continue
                for field in ("策略评分", "使用次数", "最近数据版本", "更新来源批次"):
                    if field in old:
                        tactic[field] = old[field]
            return snapshot, f"已按数据版本 {data_version} 回滚策略库评分与复用次数"

    # --------------------- 新旧评估范围仲裁：同一场景只保留一套可解释当前推荐
    def reconcile_overlap(self, scene: str, tactic_no: str) -> dict[str, Any]:
        """新场景配置与旧重算批次同时覆盖同一候选时，按数据版本仲裁当前推荐。"""
        with _write_lock:
            hits = [
                row
                for row in store.rows(TABLE_HITS)
                if row.get("推荐场景") == scene and row.get("策略编号") == tactic_no
            ]
            if not hits:
                return {"推荐场景": scene, "策略编号": tactic_no, "当前推荐": None, "历史版本": []}
            ordered = sorted(hits, key=lambda row: str(row.get("数据版本", "")), reverse=True)
            current = ordered[0]
            for row in hits:
                row["是否当前推荐"] = row is current
            return {
                "推荐场景": scene,
                "策略编号": tactic_no,
                "当前推荐": {
                    "数据版本": current.get("数据版本"),
                    "批次号": current.get("批次号"),
                    "评估范围": current.get("评估范围"),
                    "策略评分": current.get("策略评分"),
                    "复用次数": current.get("复用次数"),
                },
                "历史版本": [
                    {
                        "数据版本": row.get("数据版本"),
                        "批次号": row.get("批次号"),
                        "评估范围": row.get("评估范围"),
                        "策略评分": row.get("策略评分"),
                        "复用次数": row.get("复用次数"),
                    }
                    for row in ordered[1:]
                ],
                "仲裁口径": "按数据版本取最大值，同一场景仅保留一套当前推荐，旧批次保留为可解释历史版本",
            }

    # ----------------- 漂移对比：受影响 vs 未受影响场景的评分/结果/复用次数
    def drift_check(self, batch_no: str) -> dict[str, Any] | None:
        batch = self.get_batch(batch_no)
        if batch is None:
            return None
        affected = set(batch.get("受影响推荐场景") or [])
        details = self.list_details(batch_no)
        affected_rows = []
        unaffected_rows = []
        drift_rows = []
        for detail in details:
            item = {
                "推荐场景": detail.get("推荐场景"),
                "策略编号": detail.get("策略编号"),
                "明细状态": detail.get("明细状态"),
                "原评分": detail.get("原评分"),
                "重算评分": detail.get("重算评分"),
                "原复用次数": detail.get("原复用次数"),
                "重算复用次数": detail.get("重算复用次数"),
            }
            if detail.get("推荐场景") in affected:
                affected_rows.append(item)
            else:
                unaffected_rows.append(item)
                score_drift = detail.get("原评分") != detail.get("重算评分")
                reuse_drift = detail.get("原复用次数") != detail.get("重算复用次数")
                if score_drift or reuse_drift:
                    drift_rows.append({**item, "漂移字段": [
                        *(["策略评分"] if score_drift else []),
                        *(["复用次数"] if reuse_drift else []),
                    ]})
        return {
            "批次号": batch_no,
            "数据版本": batch.get("数据版本"),
            "受影响场景数": len(affected),
            "受影响明细": affected_rows,
            "未受影响明细": unaffected_rows,
            "未变化场景漂移量": len(drift_rows),
            "漂移明细": drift_rows,
            "评审中配置是否替换上线场景": False,
            "结论": "未变化场景无漂移" if not drift_rows else "发现未变化场景漂移，需拦截",
        }


service = RecoincrService()
