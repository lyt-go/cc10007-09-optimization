"""推荐场景增量命中重算接口。

沿用存量约定：列表返回 {items,total,page,size}，动作返回 {ok,message}，
业务判断全部收敛在 services/recoincr.py，路由层不做状态流转。
"""
from __future__ import annotations

from fastapi import APIRouter, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.recoincr import service

router = APIRouter(prefix="/api/recoincr", tags=["推荐场景增量重算"])


@router.get("/changes", response_model=PageResult[dict])
def list_changes(page: int = 1, size: int = 20) -> PageResult[dict]:
    """推荐场景变化记录：局部重算增量范围的来源。"""
    rows = service.list_changes()
    start = max(page - 1, 0) * size
    return PageResult(items=rows[start:start + size], total=len(rows), page=page, size=size)


@router.post("/changes", response_model=ActionResult)
def create_change(payload: EntryPayload) -> ActionResult:
    """登记推荐场景变化记录（场景新增/冷启动策略调整等）。"""
    entry, missing = service.register_change(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="推荐场景变化记录已登记", entry=entry)


@router.get("/batches", response_model=PageResult[dict])
def list_batches(
    status: str | None = Query(default=None, description="待处理、处理中、已完成、部分完成"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """局部重算批次清单。"""
    rows = service.list_batches(status=status)
    start = max(page - 1, 0) * size
    return PageResult(items=rows[start:start + size], total=len(rows), page=page, size=size)


@router.get("/batches/{batch_no}")
def get_batch(batch_no: str) -> dict:
    """批次详情：含受影响场景、明细与应用状态。"""
    batch = service.get_batch(batch_no)
    if batch is None:
        return {"ok": False, "message": f"重算批次 {batch_no} 不存在", "items": []}
    return {
        "batch": batch,
        "details": service.list_details(batch_no),
        "hits": service.list_hits(batch_no=batch_no),
        "drift": service.drift_check(batch_no),
    }


@router.post("/batches", response_model=ActionResult)
def submit_batch(payload: EntryPayload) -> ActionResult:
    """从推荐场景变化记录获取增量范围并执行局部收敛重算。

    入参：change_nos（变化单号数组，缺省取全部未纳入批次的变化记录）、
    batch_no（可选）、idempotency_key（并发提交幂等键，可选）。
    """
    values = payload.values
    change_nos = values.get("change_nos")
    if isinstance(change_nos, str):
        change_nos = [item.strip() for item in change_nos.split(",") if item.strip()]
    batch, message = service.submit_batch(
        change_nos=change_nos,
        batch_no=str(values.get("batch_no") or "").strip() or None,
        idempotency_key=str(values.get("idempotency_key") or "").strip() or None,
    )
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.post("/batches/{batch_no}/retry", response_model=ActionResult)
def retry_batch(batch_no: str) -> ActionResult:
    """重试补写：只补未完成场景，已完成场景与保留结果不被覆盖。"""
    batch, message = service.retry_batch(batch_no)
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.post("/batches/{batch_no}/apply", response_model=ActionResult)
def apply_batch(batch_no: str) -> ActionResult:
    """把增量命中明细数据集应用到策略库：仅更新引用变化场景的策略评分。"""
    batch, message = service.apply_batch(batch_no)
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.get("/hits", response_model=PageResult[dict])
def list_hits(
    batch_no: str | None = None,
    data_version: str | None = None,
    scene: str | None = None,
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """推荐场景增量命中明细数据集：只包含状态确定（已完成/已保留）的记录。"""
    rows = service.list_hits(batch_no=batch_no, data_version=data_version, scene=scene)
    start = max(page - 1, 0) * size
    return PageResult(items=rows[start:start + size], total=len(rows), page=page, size=size)


@router.get("/versions", response_model=PageResult[dict])
def list_versions(page: int = 1, size: int = 20) -> PageResult[dict]:
    """数据版本清单：策略命中增量重算按版本回查的入口。"""
    rows = service.list_snapshots()
    start = max(page - 1, 0) * size
    return PageResult(items=rows[start:start + size], total=len(rows), page=page, size=size)


@router.get("/versions/{data_version}")
def get_version(data_version: str) -> dict:
    """按数据版本回查当时的命中明细与策略库快照。"""
    detail = service.get_version_detail(data_version)
    if detail is None:
        return {"ok": False, "message": f"数据版本 {data_version} 不存在"}
    return detail


@router.post("/versions/{data_version}/rollback", response_model=ActionResult)
def rollback_version(data_version: str) -> ActionResult:
    """按数据版本回滚策略库评分与复用次数。"""
    snapshot, message = service.rollback_version(data_version)
    if snapshot is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=snapshot)


@router.get("/batches/{batch_no}/drift")
def drift_check(batch_no: str) -> dict:
    """对比受影响/未受影响场景的策略评分、推荐结果与复用次数，确认无漂移。"""
    result = service.drift_check(batch_no)
    if result is None:
        return {"ok": False, "message": f"重算批次 {batch_no} 不存在"}
    return result


@router.get("/overlap")
def reconcile_overlap(
    scene: str = Query(..., description="推荐场景"),
    tactic_no: str = Query(..., description="策略编号"),
) -> dict:
    """新旧评估范围覆盖同一候选时的版本仲裁视图。"""
    return service.reconcile_overlap(scene, tactic_no)
