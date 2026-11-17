"""推荐场景增量命中重算服务层单测（同进程，覆盖 HTTP 测试难构造的断点路径）。

运行：python -I tests/test_recoincr_service.py
"""
from __future__ import annotations

import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.recoincr import (  # noqa: E402
    BATCH_DONE,
    DETAIL_DONE,
    DETAIL_PENDING,
    service,
)
from app.store import store  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, condition: bool, extra: object = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {extra}")


def main() -> int:
    print("== S1. 场景边界识别：只取引用变化场景的已发布候选 ==")
    candidates = service._scene_candidates("智能推荐样例2")
    nos = sorted(c["策略编号"] for c in candidates)
    check("样例2受影响候选=TACT-0002/TACT-0004", nos == ["TACT-0002", "TACT-0004"], nos)
    check("样例3候选与样例2变化完全隔离",
          all(c["引用推荐场景"] == "智能推荐样例2" for c in candidates))

    print("== S2. 局部收敛结果确定性：同输入重复计算分值一致 ==")
    from app.services.recoincr import _stable_reuse, _stable_score
    s1 = _stable_score("V1", "场景", "T1", "a", "b", "c")
    s2 = _stable_score("V1", "场景", "T1", "a", "b", "c")
    r1 = _stable_reuse("V1", "场景", "T1")
    r2 = _stable_reuse("V1", "场景", "T1")
    check("评分收敛可复现", s1 == s2 and isinstance(s1, int) and 60 <= s1 <= 95, s1)
    check("复用次数收敛可复现", r1 == r2 and isinstance(r1, int))

    print("== S3. 提交局部重算：受影响重算 + 未受影响保留 ==")
    change, missing = service.register_change({
        "推荐场景": "智能推荐样例2", "变化类型": "冷启动策略调整", "变化单号": "CHG-S3",
    })
    check("变化单登记成功", not missing and change["变化单号"] == "CHG-S3")
    batch, msg = service.submit_batch(change_nos=["CHG-S3"], idempotency_key="S3-KEY")
    check("批次已完成且状态确定", batch is not None and batch["批次状态"] == BATCH_DONE, msg)
    check("提交阶段尚未应用到策略库", batch["已应用"] is False)
    check("仅样例2进入受影响范围", batch["受影响推荐场景"] == ["智能推荐样例2"])
    details = service.list_details(batch["批次号"])
    done = [d for d in details if d["明细状态"] == DETAIL_DONE]
    kept = [d for d in details if d["明细状态"] == "已保留"]
    check("受影响明细2条", len(done) == 2 and {d["策略编号"] for d in done} == {"TACT-0002", "TACT-0004"})
    check("未受影响明细保留2条", len(kept) == 2 and {d["推荐场景"] for d in kept} == {"智能推荐样例1", "智能推荐样例3"})
    check("保留明细评分等于原评分（无漂移）", all(d["重算评分"] == d["原评分"] for d in kept))

    print("== S3.5 显式应用到策略库：仅引用变化场景的候选被更新 ==")
    untouched = next(t for t in store.rows("tacticlib") if t["策略编号"] == "TACT-0003")
    score_before, reuse_before = untouched["策略评分"], untouched["使用次数"]
    applied, apply_msg = service.apply_batch(batch["批次号"])
    check("应用成功并固化版本快照", applied is not None and applied["已应用"] is True, apply_msg)
    check("仅2条引用变化场景的策略被更新", applied["更新策略数"] == 2, applied)
    untouched_after = next(t for t in store.rows("tacticlib") if t["策略编号"] == "TACT-0003")
    check("未变化场景评分与复用次数未漂移",
          untouched_after["策略评分"] == score_before and untouched_after["使用次数"] == reuse_before)
    again_apply, again_msg = service.apply_batch(batch["批次号"])
    check("重复应用幂等不产生重复写入", again_apply["更新策略数"] == 2 and "已应用" in again_msg)

    print("== S4. 幂等键：同一业务记录重复提交只允许一次有效写入 ==")
    hits_before = len(store.rows("reco_incr_hits"))
    again, _ = service.submit_batch(change_nos=["CHG-S3"], idempotency_key="S3-KEY")
    check("重复提交返回同批次", again["批次号"] == batch["批次号"])
    check("命中数据集无新增", len(store.rows("reco_incr_hits")) == hits_before)

    print("== S5. 并发提交：20 线程同幂等键，仅一个批次、业务键无重复 ==")
    service.register_change({"推荐场景": "智能推荐样例1", "变化类型": "场景配置新增", "变化单号": "CHG-S5"})
    results: list[dict] = []
    barrier = threading.Barrier(20)

    def worker() -> None:
        barrier.wait()
        b, _ = service.submit_batch(change_nos=["CHG-S5"], idempotency_key="S5-CONC")
        results.append(b)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("20 个并发提交全部返回同一批次", len({b["批次号"] for b in results}) == 1)
    conc_no = results[0]["批次号"]
    conc_hits = service.list_hits(batch_no=conc_no)
    keys = [(h["推荐场景"], h["策略编号"]) for h in conc_hits]
    check("同场景×策略只存在一条有效命中", len(keys) == len(set(keys)), keys)

    print("== S6. 断点补写：部分完成批次重试只补未完成场景 ==")
    partial_no = "RB-S6-PARTIAL"
    partial_version = "V20261117-S6"
    store.rows("reco_incr_batches").append({
        "id": 9001, "批次号": partial_no, "增量变化单": ["CHG-S6"],
        "受影响推荐场景": ["智能推荐样例2"], "批次状态": "部分完成",
        "数据版本": partial_version, "幂等键": "S6-PARTIAL",
        "创建时间": "2026-11-17 10:00:00", "完成时间": "", "已应用": False,
    })
    store.rows("reco_incr_details").append({
        "id": 9001, "批次号": partial_no, "数据版本": partial_version,
        "推荐场景": "智能推荐样例2", "策略编号": "TACT-0002", "策略名称": "购物车凑单推荐策略",
        "明细状态": DETAIL_PENDING, "变化类型": "受影响-局部重算",
        "重算评分": None, "重算复用次数": None, "原评分": 77, "原复用次数": 96, "写入时间": "",
    })
    store.rows("reco_incr_details").append({
        "id": 9002, "批次号": partial_no, "数据版本": partial_version,
        "推荐场景": "智能推荐样例2", "策略编号": "TACT-0004", "策略名称": "新客冷启动引导策略",
        "明细状态": DETAIL_DONE, "变化类型": "受影响-局部重算",
        "重算评分": 71, "重算复用次数": 58, "原评分": 69, "原复用次数": 54,
        "写入时间": "2026-11-17 10:01:00",
    })
    fixed_batch, message = service.retry_batch(partial_no)
    check("重试补写成功", fixed_batch is not None and "补写" in message, message)
    check("补写后批次收敛为已完成", fixed_batch["批次状态"] == BATCH_DONE)
    details6 = {d["策略编号"]: d for d in service.list_details(partial_no)}
    check("待处理明细被补写为已完成且评分确定",
          details6["TACT-0002"]["明细状态"] == DETAIL_DONE
          and isinstance(details6["TACT-0002"]["重算评分"], int))
    check("已完成明细未被重试覆盖",
          details6["TACT-0004"]["重算评分"] == 71 and details6["TACT-0004"]["重算复用次数"] == 58)
    hit0002 = [h for h in service.list_hits(batch_no=partial_no) if h["策略编号"] == "TACT-0002"]
    check("补写命中只产生一条确定记录", len(hit0002) == 1 and hit0002[0]["命中状态"] == "已完成")
    # 再次重试：无未完成场景，不产生新写入
    hits_before6 = len(store.rows("reco_incr_hits"))
    service.retry_batch(partial_no)
    check("二次重试无新增写入", len(store.rows("reco_incr_hits")) == hits_before6)

    print("== S7. 版本回查与回滚 ==")
    version = batch["数据版本"]
    # S6 的断点批次只做了重试补写、从未应用，验证“未应用不产生版本快照”的门禁
    check("未应用批次没有自己的版本快照", service.get_version_detail(partial_version) is None)
    detail = service.get_version_detail(version)
    check("版本详情含策略库快照与命中明细", detail is not None and "策略库快照" in detail and "命中明细" in detail)
    target_tactic = next(t for t in store.rows("tacticlib") if t["策略编号"] == "TACT-0002")
    snap, message = service.rollback_version(version)
    check("回滚执行成功", snap is not None and version in message, message)

    print("== S8. 新旧评估范围仲裁：同一场景一套当前推荐 ==")
    overlap = service.reconcile_overlap("智能推荐样例2", "TACT-0002")
    versions = [overlap["当前推荐"]["数据版本"]] + [h["数据版本"] for h in overlap["历史版本"]]
    check("当前推荐为数据版本最大者", versions == sorted(versions, reverse=True), versions)
    check("历史版本保留且可解释", all("评估范围" in h for h in overlap["历史版本"]))

    print("== S9. 漂移核对：未变化场景零漂移 ==")
    drift = service.drift_check(batch["批次号"])
    check("未变化场景漂移量为0", drift["未变化场景漂移量"] == 0, drift)
    check("评审中配置不会替换上线场景", drift["评审中配置是否替换上线场景"] is False)
    check("结论明确", drift["结论"] == "未变化场景无漂移")

    print("== S10. 异常输入 ==")
    none_batch, none_msg = service.submit_batch(change_nos=["CHG-NO-SUCH"])
    check("不存在变化单被拦截", none_batch is None and "不存在" in none_msg)
    no_retry, no_msg = service.retry_batch("RB-NO-SUCH")
    check("不存在批次重试被拦截", no_retry is None and "不存在" in no_msg)
    bad_change, missing_fields = service.register_change({"推荐场景": ""})
    check("变化单缺必填字段被拦截", bad_change is None and "推荐场景" in missing_fields)
    dup_no, dup_msg = service.submit_batch(change_nos=["CHG-S3"], batch_no="RB-DUP-1", idempotency_key="DUP-1")
    check("首次显式批次号提交成功", dup_no is not None, dup_msg)
    dup_again, dup_msg2 = service.submit_batch(change_nos=["CHG-S3"], batch_no="RB-DUP-1", idempotency_key="DUP-2")
    check("同名批次号重复提交被拦截", dup_again is None and "已存在" in dup_msg2, dup_msg2)
    # 处理中/未完成批次不允许应用：构造一个待处理批次验证状态门禁
    store.rows("reco_incr_batches").append({
        "id": 9002, "批次号": "RB-S10-RUNNING", "增量变化单": [], "受影响推荐场景": [],
        "批次状态": "处理中", "数据版本": "V20261117-S10", "幂等键": "S10-RUN",
        "创建时间": "", "完成时间": "", "已应用": False,
    })
    running_apply, running_msg = service.apply_batch("RB-S10-RUNNING")
    check("处理中批次禁止应用（只留状态确定数据集）", running_apply is None and "状态确定" in running_msg, running_msg)

    print(f"\n汇总：PASS={PASS} FAIL={FAIL}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
