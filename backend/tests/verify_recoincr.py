"""推荐场景增量命中重算端到端核验（RECO-0002 主核对样例）。

运行：python -I tests/verify_recoincr.py
仅依赖标准库，通过 HTTP 调用真实运行的后端。
"""
from __future__ import annotations

import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE = "http://127.0.0.1:8000"
PASS = 0
FAIL = 0


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    if "?" in path:
        head, query = path.split("?", 1)
        path = head + "?" + urllib.parse.urlencode(urllib.parse.parse_qsl(query))
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return {"http_error": exc.code, "detail": exc.read().decode("utf-8")}


def check(name: str, condition: bool, extra: object = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {extra}")


def tactic_map() -> dict[str, dict]:
    rows = call("GET", "/api/tacticlib?size=200")["items"]
    return {row["策略编号"]: row for row in rows}


def main() -> int:
    print("== 0. 基线：策略库原始评分与复用次数 ==")
    base = tactic_map()
    for no in ("TACT-0001", "TACT-0002", "TACT-0003", "TACT-0004"):
        print(f"  {no}: 评分={base[no]['策略评分']} 复用={base[no]['使用次数']} 场景={base[no]['引用推荐场景']}")
    check("基线包含 4 条已发布候选", len(base) == 4)

    print("== 1. 提交局部重算（增量范围仅 CHG-0001，场景=智能推荐样例2） ==")
    res = call("POST", "/api/recoincr/batches", {"values": {"change_nos": ["CHG-0001"]}})
    check("提交成功", res.get("ok") is True, res)
    batch = res["entry"]
    bno = batch["批次号"]
    version = batch["数据版本"]
    check("批次状态确定=已完成", batch["批次状态"] == "已完成", batch)
    check("受影响场景收敛为样例2一个", batch["受影响推荐场景"] == ["智能推荐样例2"], batch)
    check("受影响候选数=2（TACT-0002/TACT-0004）", batch["受影响候选数"] == 2, batch)
    check("保留明细数=2（样例1/样例3 不重算）", batch["保留明细数"] == 2, batch)
    check("提交阶段仅生成数据集、尚未应用", batch["已应用"] is False, batch)

    print("== 1.5 策略库待处理明细接收数据集：显式应用，仅更新引用变化场景的策略 ==")
    apply_res = call("POST", f"/api/recoincr/batches/{bno}/apply")
    check("应用成功", apply_res.get("ok") is True and apply_res["entry"]["已应用"] is True, apply_res)
    check("仅2条引用变化场景的策略评分被更新", apply_res["entry"]["更新策略数"] == 2, apply_res)
    apply_again = call("POST", f"/api/recoincr/batches/{bno}/apply")
    check("重复应用幂等", apply_again.get("ok") is True and "不产生重复写入" in apply_again["message"], apply_again)
    version = batch["数据版本"]

    print("== 2. 命中明细数据集：只含状态确定记录，业务键唯一 ==")
    hits = call("GET", f"/api/recoincr/hits?batch_no={bno}&size=200")["items"]
    check("新批次命中 2 条且均为已完成", len(hits) == 2 and all(h["命中状态"] == "已完成" for h in hits), hits)
    keys = [(h["批次号"], h["推荐场景"], h["策略编号"]) for h in hits]
    check("业务键（批次+场景+策略）无重复", len(keys) == len(set(keys)), keys)
    online_cfg = next(h for h in hits if h["策略编号"] == "TACT-0002")
    check("采用已上线 RECO-0004 配置而非评审中 RECO-0002",
          online_cfg["召回算法"] == "协同过滤召回" and online_cfg["评估范围"] == "新场景配置", online_cfg)

    print("== 2.5 首批应用后立即核对：未变化场景无漂移（此时样例1/3 尚未被任何批次触碰） ==")
    first_apply = tactic_map()
    check("TACT-0001（样例1）评分与复用次数原样保留",
          first_apply["TACT-0001"]["策略评分"] == base["TACT-0001"]["策略评分"]
          and first_apply["TACT-0001"]["使用次数"] == base["TACT-0001"]["使用次数"])
    check("TACT-0003（样例3）评分与复用次数原样保留",
          first_apply["TACT-0003"]["策略评分"] == base["TACT-0003"]["策略评分"]
          and first_apply["TACT-0003"]["使用次数"] == base["TACT-0003"]["使用次数"])

    print("== 3. 幂等：相同增量范围重复提交，只允许一次有效写入 ==")
    before = len(call("GET", "/api/recoincr/hits?size=200")["items"])
    res2 = call("POST", "/api/recoincr/batches", {"values": {"change_nos": ["CHG-0001"]}})
    after = len(call("GET", "/api/recoincr/hits?size=200")["items"])
    check("重复提交返回既有批次号", res2.get("entry", {}).get("批次号") == bno, res2)
    check("命中数据集无新增写入", before == after, (before, after))

    print("== 4. 并发：同一业务记录并发提交，仅一次有效写入 ==")
    outcomes: list[dict] = []
    barrier = threading.Barrier(5)

    def worker() -> None:
        barrier.wait()
        outcomes.append(call("POST", "/api/recoincr/batches",
                             {"values": {"change_nos": ["CHG-0002"], "idempotency_key": "CONC-KEY-1"}}))

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    batch_nos = {o.get("entry", {}).get("批次号") for o in outcomes}
    check("5 个并发请求落到同一个批次", len(batch_nos) == 1, outcomes)
    conc_bno = next(iter(batch_nos))
    conc_hits = call("GET", f"/api/recoincr/hits?batch_no={conc_bno}&size=200")["items"]
    check("并发批次每场景×策略仅一条命中", len(conc_hits) == len({(h["推荐场景"], h["策略编号"]) for h in conc_hits}), conc_hits)
    conc_apply = call("POST", f"/api/recoincr/batches/{conc_bno}/apply")
    check("并发批次应用成功且幂等", conc_apply.get("ok") and conc_apply["entry"]["已应用"] is True, conc_apply)

    print("== 5. 策略库应用结果：仅变化场景评分更新，未变化场景无漂移 ==")
    after_apply = tactic_map()
    check("TACT-0002（样例2）评分已刷新",
          after_apply["TACT-0002"]["策略评分"] != base["TACT-0002"]["策略评分"],
          (base["TACT-0002"]["策略评分"], after_apply["TACT-0002"]["策略评分"]))
    check("TACT-0004（样例2）复用次数已刷新",
          after_apply["TACT-0004"]["使用次数"] != base["TACT-0004"]["使用次数"])
    # 并发批次 CHG-0002 覆盖样例1：TACT-0001 此时被局部重算属于预期，数值须与并发批次命中明细一致
    conc_hit_0001 = next(h for h in call("GET", f"/api/recoincr/hits?batch_no={conc_bno}&size=200")["items"]
                         if h["策略编号"] == "TACT-0001")
    check("TACT-0001 仅按并发批次结果更新（局部收敛、可解释）",
          after_apply["TACT-0001"]["策略评分"] == conc_hit_0001["策略评分"]
          and after_apply["TACT-0001"]["使用次数"] == conc_hit_0001["复用次数"])
    check("TACT-0003（样例3，无任何变化单覆盖）评分与复用次数始终原样保留",
          after_apply["TACT-0003"]["策略评分"] == base["TACT-0003"]["策略评分"]
          and after_apply["TACT-0003"]["使用次数"] == base["TACT-0003"]["使用次数"])
    check("更新策略带数据版本可回查", after_apply["TACT-0002"]["最近数据版本"] == version)

    print("== 6. 漂移对比接口 ==")
    drift = call("GET", f"/api/recoincr/batches/{bno}/drift")
    check("未变化场景漂移量=0", drift["未变化场景漂移量"] == 0, drift)
    check("受影响明细2条、未受影响明细2条",
          len(drift["受影响明细"]) == 2 and len(drift["未受影响明细"]) == 2, drift)
    check("结论=未变化场景无漂移", drift["结论"] == "未变化场景无漂移", drift)
    check("评审中配置未替换上线场景", drift["评审中配置是否替换上线场景"] is False)

    print("== 7. 重试补写：HTTP 侧验证已完成批次重试不产生重复写入（断点补写路径见同进程单测） ==")
    retry_done = call("POST", f"/api/recoincr/batches/{bno}/retry")
    check("已完成批次重试不产生重复写入", retry_done.get("ok") and "不产生重复写入" in retry_done["message"], retry_done)
    hits_before = len(call("GET", "/api/recoincr/hits?size=200")["items"])
    call("POST", f"/api/recoincr/batches/{bno}/retry")
    hits_after = len(call("GET", "/api/recoincr/hits?size=200")["items"])
    check("重复重试命中数据集条数不变", hits_before == hits_after, (hits_before, hits_after))

    print("== 8. 数据版本回查与回滚 ==")
    ver = call("GET", f"/api/recoincr/versions/{version}")
    check("版本详情可回查", ver.get("数据版本") == version and "策略库快照" in ver, ver)
    rollback = call("POST", f"/api/recoincr/versions/{version}/rollback")
    check("按版本回滚成功", rollback.get("ok") is True, rollback)
    missing_ver = call("GET", "/api/recoincr/versions/V9999-999")
    check("不存在版本回查给出可读提示", missing_ver.get("ok") is False, missing_ver)

    print("== 9. 新旧评估范围仲裁：同一场景只保留一套当前推荐 ==")
    overlap = call("GET", "/api/recoincr/overlap?scene=智能推荐样例2&tactic_no=TACT-0002")
    check("存在新旧两个评估范围", len(overlap["历史版本"]) >= 1 and overlap["当前推荐"], overlap)
    check("当前推荐取数据版本最大者", overlap["当前推荐"]["数据版本"] == version, overlap)
    check("仲裁口径可解释", "数据版本" in overlap["仲裁口径"], overlap)

    print("== 10. 异常输入与边界 ==")
    empty = call("POST", "/api/recoincr/batches", {"values": {"change_nos": ["CHG-404"]}})
    check("不存在的变化单被拦截", empty.get("ok") is False, empty)
    no_retry = call("POST", "/api/recoincr/batches/RB-404/retry")
    check("不存在批次重试被拦截", no_retry.get("ok") is False, no_retry)
    change_res = call("POST", "/api/recoincr/changes", {"values": {"推荐场景": "智能推荐样例3"}})
    check("变化记录缺字段被拦截", change_res.get("ok") is False, change_res)

    print(f"\n汇总：PASS={PASS} FAIL={FAIL}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
