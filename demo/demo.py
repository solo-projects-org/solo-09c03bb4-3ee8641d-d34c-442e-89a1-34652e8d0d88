#!/usr/bin/env python3
"""End-to-end demonstration for the water-quality collection platform.

Runnable with an isolated interpreter:

    python -I demo/demo.py

Scenarios:
  1. Mid-flight correction  -> stale cursor 409, run fails, nothing partial is
     published; re-collection publishes the new revision atomically.
  2. Transient read failure -> current page is retried, run still succeeds.
  3. Two web processes collecting the same day -> only one runs; plus an
     end-to-end stale-revision check proving a late lower revision can never
     overwrite a newer published revision, plus a direct DB-guard unit check.
  4. Review-team withdrawal -> withdraw the seen revision with a reason; old
     observations disappear from reads (reason + withdrawn revision remain);
     stale-form/optimistic conflicts are refused; same-or-older in-flight
     collections are fenced out even when late; only a strictly higher
     revision passing full-day validation restores the day; reads across both
     web processes come from one committed state; plus direct DB unit checks
     for the fence and rollback-on-write-failure.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "web"))  # db.py is stdlib-only, safe under -I

RESULTS: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    RESULTS.append((ok, label))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {label}" + (f" -- {detail}" if detail else ""))
    if not ok:
        raise AssertionError(label)


def request(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode())
        except Exception:
            return exc.code, {}


def wait_run(base: str, run_id: int, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, run = request("GET", f"{base}/api/runs/{run_id}")
        if status == 200 and run["status"] != "running":
            return run
        time.sleep(0.4)
    raise TimeoutError(f"run {run_id} did not finish in {timeout}s")


def wait_progress(base: str, run_id: int, min_attempts: int, timeout: float = 15.0) -> None:
    """Block until the run has made at least ``min_attempts`` page attempts."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, run = request("GET", f"{base}/api/runs/{run_id}")
        if int(run.get("page_attempts") or 0) >= min_attempts:
            return
        time.sleep(0.1)
    raise TimeoutError(f"run {run_id} did not reach {min_attempts} page attempts")


def reset_day(station: str, section: str, date: str) -> None:
    request("POST", f"{station}/admin/reset", {"section": section, "date": date})


def published_or_none(web: str, section: str, date: str) -> dict | None:
    status, body = request("GET", f"{web}/api/published?section={section}&date={date}")
    return body if status == 200 else None


def scenario_mid_correction(station: str, web: str, section: str, date: str) -> None:
    print("\n=== 场景 1：采集中途县站更正记录（旧游标失效 / 整日发布）===")
    reset_day(station, section, date)

    # Capture a cursor minted at revision 1, before any correction happens.
    status, page = request("GET", f"{station}/observations?section={section}&date={date}")
    old_cursor = page["next_cursor"]
    check(status == 200 and page["revision"] == 1 and old_cursor, "取得修订号 1 的分页游标")

    status, started = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    check(status == 202, "启动采集任务", json.dumps(started))
    run_id = started["run_id"]

    # Fire the correction as soon as page 1 is accepted: page 2's cursor was
    # minted at revision 1 and the station now rejects it with 409. This is
    # deterministic regardless of the configured page latency.
    wait_progress(web, run_id, min_attempts=1)
    status, corr = request(
        "POST", f"{station}/admin/correct",
        {"section": section, "date": date, "time": "08:00", "nh3n": 9.999},
    )
    check(status == 200 and corr["revision"] == 2, "县站更正后修订号递增到 2", json.dumps(corr))

    run = wait_run(web, run_id)
    check(run["status"] == "failed", "混入旧修订页的采集任务失败", run.get("error", ""))
    check("revision" in run["error"].lower() or "cursor" in run["error"].lower(),
          "失败原因为修订变化/旧游标", run["error"])
    check(published_or_none(web, section, date) is None,
          "失败后没有任何部分数据被公布")

    # Direct proof that the old cursor is now dead at the source.
    status, _ = request("GET", f"{station}/observations?section={section}&date={date}&cursor={old_cursor}")
    check(status == 409, "复用更正前游标返回 409 stale_cursor")

    # Re-collect: full day at revision 2 gets published atomically.
    status, started2 = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    check(status == 202, "重新启动采集")
    run2 = wait_run(web, started2["run_id"])
    check(run2["status"] == "success" and run2["revision"] == 2,
          "重新采集成功并按修订号 2 发布", json.dumps(run2))

    published = published_or_none(web, section, date)
    check(published is not None and published["revision"] == 2, "已发布版本修订号为 2")
    check(len(published["rows"]) == 24, "收齐总行数 24 行")
    times = [r["time"] for r in published["rows"]]
    check(len(set(times)) == 24 and all(t.endswith(":00") for t in times),
          "当日时刻唯一且为整点 UTC 时刻")
    row8 = next(r for r in published["rows"] if r["time"] == "08:00")
    check(row8["nh3n"] == 9.999, "更正后的数值被发布", json.dumps(row8))
    check(all(r["source_page"] >= 1 and r["revision"] == 2 for r in published["rows"]),
          "每行都记录来源页与修订号")

    # Same revision again: idempotent success, still revision 2.
    status, started3 = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    run3 = wait_run(web, started3["run_id"])
    check(run3["status"] == "success" and run3["revision"] == 2,
          "同修订号重复采集幂等成功")


def scenario_read_failure(station: str, web: str, section: str, date: str) -> None:
    print("\n=== 场景 2：页读取失败，仅重试当前页后成功 ===")
    reset_day(station, section, date)
    request("POST", f"{station}/admin/fail-next",
            {"section": section, "date": date})

    # The injected failure surfaces directly at the station endpoint.
    status, _ = request("GET", f"{station}/observations?section={section}&date={date}")
    check(status == 500, "县站确实对下一次读取返回 500")

    # Re-arm: the probe above consumed the one-shot failure.
    request("POST", f"{station}/admin/fail-next",
            {"section": section, "date": date})

    status, started = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    run = wait_run(web, started["run_id"])
    check(run["status"] == "success", "重试后整日采集成功", run.get("error", ""))
    check(run["page_attempts"] >= 4,
          "3 个页面中至少 1 页发生重试（尝试次数 > 页数）",
          f"page_attempts={run['page_attempts']}")
    check(len(published_or_none(web, section, date)["rows"]) == 24, "发布 24 行完整数据")


def scenario_concurrency(station: str, web1: str, web2: str, section: str, date: str) -> None:
    print("\n=== 场景 3：两个 Web 进程并发采集同日 + 陈旧修订防护 ===")
    reset_day(station, section, date)
    request("POST", f"{station}/admin/latency", {"delay_ms": 1200})
    try:
        s1, r1 = request("POST", f"{web1}/api/collect", {"section": section, "date": date})
        s2, r2 = request("POST", f"{web2}/api/collect", {"section": section, "date": date})
        statuses = sorted([s1, s2])
        check(statuses == [202, 409],
              "并发同日采集：一个受理、一个被唯一活动任务约束拒绝",
              f"web1={s1} web2={s2} detail={r2.get('detail')}")

        # While the winner runs, no partial day is visible anywhere.
        time.sleep(0.8)
        check(published_or_none(web1, section, date) is None
              and published_or_none(web2, section, date) is None,
              "采集进行中两进程均看不到未完成版本")

        winner_base = web1 if s1 == 202 else web2
        run = wait_run(winner_base, r1["run_id"] if s1 == 202 else r2["run_id"])
        check(run["status"] == "success", "胜出任务完整发布", json.dumps(run))
        check(published_or_none(web1, section, date)["revision"] == 1
              and published_or_none(web2, section, date)["revision"] == 1,
              "两个进程读取同一 SQLite，均看到修订号 1 的完整版本")
    finally:
        request("POST", f"{station}/admin/latency", {"delay_ms": 350})

    # End-to-end stale guard: publish rev 2, then make the station serve rev 1
    # again and confirm the late lower revision cannot overwrite the newer one.
    request("POST", f"{station}/admin/correct",
            {"section": section, "date": date, "time": "10:00", "nh3n": 7.777})
    _, started = request("POST", f"{web1}/api/collect", {"section": section, "date": date})
    run = wait_run(web1, started["run_id"])
    check(run["status"] == "success" and run["revision"] == 2, "采集并发布修订号 2")

    reset_day(station, section, date)  # station goes back to revision 1
    _, started = request("POST", f"{web2}/api/collect", {"section": section, "date": date})
    run = wait_run(web2, started["run_id"])
    check(run["status"] == "failed", "迟到的旧修订采集被拒绝")
    check("refusing to publish revision 1" in run["error"], "拒绝原因是陈旧修订防护", run["error"])
    published = published_or_none(web1, section, date)
    check(published["revision"] == 2, "已发布的较新修订号 2 未被覆盖")

    # Direct DB-layer check (the exact cross-process transaction guard).
    from app import db  # noqa: E402  (path inserted at top of file)

    tmp = ROOT / "data" / f"guard-{os.getpid()}.db"
    tmp.parent.mkdir(exist_ok=True)
    db.init_db(str(tmp))
    rid = db.start_run(str(tmp), section, date, "guard-test")
    db.publish_version(str(tmp), section, date, 2,
                       [{"time": "00:00", "nh3n": 1.0, "source_page": 1}], "guard-test", rid)
    rid2 = db.start_run(str(tmp), section, date, "guard-test")
    blocked = False
    try:
        db.publish_version(str(tmp), section, date, 1,
                           [{"time": "00:00", "nh3n": 0.5, "source_page": 1}],
                           "guard-test", rid2)
    except db.StaleRevisionError:
        blocked = True
    check(blocked, "DB 事务直接验证：旧修订无法覆盖新修订")
    kept = db.get_published(str(tmp), section, date)
    check(kept["revision"] == 2 and kept["rows"][0]["nh3n"] == 1.0,
          "防护触发后原有数据原样保留")
    tmp.unlink(missing_ok=True)
    for suffix in ("-wal", "-shm"):
        Path(str(tmp) + suffix).unlink(missing_ok=True)


def scenario_withdrawal(station: str, web1: str, web2: str, section: str, date: str,
                        interleave_date: str) -> None:
    print("\n=== 场景 4：复核组撤回污染修订（原因撤回 / 乐观并发 / 修订围栏 / 高修订恢复）===")
    reset_day(station, section, date)
    request("POST", f"{station}/admin/latency", {"delay_ms": 0})

    # --- baseline: revision 1 fully published -------------------------------
    _, started = request("POST", f"{web1}/api/collect", {"section": section, "date": date})
    run = wait_run(web1, started["run_id"])
    check(run["status"] == "success" and run["revision"] == 1, "基线：修订号 1 整日发布")

    # --- withdrawing a day that has no published version -> 404 -------------
    s404, body404 = request(
        "POST", f"{web1}/api/withdraw",
        {"section": section, "date": "2099-01-01", "expected_revision": 1, "reason": "无此日"},
    )
    check(s404 == 404, "无已发布版本时撤回返回 404", json.dumps(body404))

    # --- old page submits a revision that is no longer current -> 409 --------
    # (simulates the form carrying a stale expected_revision)
    s, conflict = request(
        "POST", f"{web1}/api/withdraw",
        {"section": section, "date": date, "expected_revision": 99,
         "reason": "旧页面重复提交"},
    )
    check(s == 409 and conflict["detail"]["error"] == "revision_conflict"
          and conflict["detail"]["current_revision"] == 1,
          "旧修订号撤回被乐观并发拒绝，新档不受影响", json.dumps(conflict))
    still = published_or_none(web1, section, date)
    check(still["status"] == "published" and len(still["rows"]) == 24,
          "冲突拒绝后观测原样保留")

    # --- valid withdrawal of the seen revision ------------------------------
    s, w = request(
        "POST", f"{web2}/api/withdraw",
        {"section": section, "date": date, "expected_revision": 1,
         "reason": "污染复核：仪器故障数据混入，县站尚未更正，先阻止下游取用"},
    )
    check(s == 200 and w["status"] == "withdrawn" and w["withdrawn_revision"] == 1,
          "按所见修订号撤回成功", json.dumps(w))

    # --- reads on BOTH instances: reason visible, zero old observations -----
    for base in (web1, web2):
        p = published_or_none(base, section, date)
        check(p is not None and p["status"] == "withdrawn"
              and p["withdrawn_revision"] == 1 and p["rows"] == []
              and "仪器故障" in p["reason"],
              f"{base} 读取：显示原因与被撤回修订，不返回旧观测")
    s, audit = request("GET", f"{web1}/api/withdrawals?section={section}&date={date}")
    check(s == 200 and len(audit["withdrawals"]) == 1
          and audit["withdrawals"][0]["superseded_at"] is None,
          "撤回审计记录生效中")

    # --- repeated withdraw cannot touch anything (already withdrawn) --------
    s, again = request(
        "POST", f"{web1}/api/withdraw",
        {"section": section, "date": date, "expected_revision": 1, "reason": "重复撤回"},
    )
    check(s == 409 and again["detail"]["error"] == "already_withdrawn",
          "重复撤回被拒绝", json.dumps(again))

    # --- same-revision in-flight collection, late -> fenced out -------------
    _, started = request("POST", f"{web1}/api/collect", {"section": section, "date": date})
    run = wait_run(web1, started["run_id"])
    check(run["status"] == "failed" and "withdrawn" in run["error"],
          "同修订在途采集迟到后被撤回围栏拒绝", run["error"])
    check(published_or_none(web2, section, date)["status"] == "withdrawn",
          "围栏拒绝后仍为撤回状态")

    # --- station still serves rev 1 (county not corrected): also blocked ----
    s, meta = request("GET", f"{station}/metadata?section={section}&date={date}")
    check(meta["revision"] == 1, "县站尚未更正（仍为修订号 1）")
    _, started = request("POST", f"{web2}/api/collect", {"section": section, "date": date})
    run = wait_run(web2, started["run_id"])
    check(run["status"] == "failed", "旧修订再次采集仍被阻止")

    # --- interleaving: slow in-flight collection crossed by withdrawal ------
    # A dedicated day in a month no other scenario uses, so it can never carry
    # leftover state. Note resetting the station does not clear the local
    # SQLite withdrawal fence on any other day.
    idate = interleave_date
    reset_day(station, section, idate)
    request("POST", f"{station}/admin/latency", {"delay_ms": 0})
    _, started = request("POST", f"{web1}/api/collect", {"section": section, "date": idate})
    wait_run(web1, started["run_id"])
    request("POST", f"{station}/admin/latency", {"delay_ms": 900})
    _, inflight = request("POST", f"{web1}/api/collect", {"section": section, "date": idate})
    # Wait until page 1 is accepted so the collector is mid-flight (holding
    # the day's data in memory) but still has pages 2..N to fetch.
    wait_progress(web1, inflight["run_id"], min_attempts=1)
    s, w2 = request(
        "POST", f"{web2}/api/withdraw",
        {"section": section, "date": idate, "expected_revision": 1,
         "reason": "采集与撤回交错：立即阻断下游"},
    )
    check(s == 200, "交错中撤回立即生效")
    # Both processes must report the same committed withdrawn snapshot.
    p1 = published_or_none(web1, section, idate)
    p2 = published_or_none(web2, section, idate)
    check(p1["status"] == p2["status"] == "withdrawn"
          and p1["rows"] == p2["rows"] == []
          and p1["withdrawn_revision"] == p2["withdrawn_revision"] == 1,
          "两 Web 实例读到同一已提交的撤回状态（状态与观测一致）")
    run = wait_run(web1, inflight["run_id"])
    check(run["status"] == "failed" and "withdrawn" in run["error"],
          "交错中同修订采集发布时被围栏挡住", run["error"])
    request("POST", f"{station}/admin/latency", {"delay_ms": 0})

    # --- only a strictly higher revision, fully validated, restores ---------
    s, corr = request(
        "POST", f"{station}/admin/correct",
        {"section": section, "date": idate, "time": "08:00", "nh3n": 6.321},
    )
    check(corr["revision"] == 2, "县站更正后修订号升到 2")
    _, started = request("POST", f"{web2}/api/collect", {"section": section, "date": idate})
    run = wait_run(web2, started["run_id"])
    check(run["status"] == "success" and run["revision"] == 2 and run["rows_collected"] == 24,
          "更高修订通过整日校验后恢复可用", json.dumps(run))
    for base in (web1, web2):
        p = published_or_none(base, section, idate)
        check(p["status"] == "published" and p["revision"] == 2 and len(p["rows"]) == 24,
              f"{base} 读取：恢复后返回修订号 2 的完整观测")
    s, audit = request("GET", f"{web1}/api/withdrawals?section={section}&date={idate}")
    latest = audit["withdrawals"][0]
    check(latest["superseded_at"] is not None and latest["superseded_by_revision"] == 2,
          "撤回记录被更高修订标记为已取代", json.dumps(latest))

    # --- a late rev-1 collector after restore must still lose ---------------
    reset_day(station, section, idate)  # station back to revision 1
    _, started = request("POST", f"{web1}/api/collect", {"section": section, "date": idate})
    run = wait_run(web1, started["run_id"])
    check(run["status"] == "failed", "恢复后的迟到旧修订采集仍被拒绝")
    check(published_or_none(web2, section, idate)["revision"] == 2,
          "恢复后的修订号 2 版本原样保留")

    # --- direct DB-layer unit checks (stdlib only, python -I safe) ----------
    from app import db  # noqa: E402  (path inserted at top of file)

    tmp = ROOT / "data" / f"withdraw-guard-{os.getpid()}.db"
    tmp.parent.mkdir(exist_ok=True)
    one_row = [{"time": "00:00", "nh3n": 1.0, "source_page": 1}]
    db.init_db(str(tmp))
    rid = db.start_run(str(tmp), section, date, "guard-test")
    db.publish_version(str(tmp), section, date, 1, one_row, "guard-test", rid)

    # optimistic conflict never mutates data.
    conflicted = False
    try:
        db.withdraw_published(str(tmp), section, date, 7, "stale form", "guard-test")
    except db.RevisionConflictError:
        conflicted = True
    check(conflicted, "DB 单元：期望修订不符时报冲突")
    kept = db.get_published(str(tmp), section, date)
    check(kept["status"] == "published" and len(kept["rows"]) == 1,
          "DB 单元：冲突后数据原样保留")

    db.withdraw_published(str(tmp), section, date, 1, "fault", "guard-test")
    rid = db.start_run(str(tmp), section, date, "guard-test")
    fenced = False
    try:
        db.publish_version(str(tmp), section, date, 1, one_row, "guard-test", rid)
    except db.WithdrawnRevisionError:
        fenced = True
    check(fenced, "DB 单元：撤回后同修订发布被围栏拒绝")
    db.fail_run(str(tmp), rid, "fenced")
    rid = db.start_run(str(tmp), section, date, "guard-test")
    db.publish_version(str(tmp), section, date, 2, one_row, "guard-test", rid)
    restored = db.get_published(str(tmp), section, date)
    check(restored["status"] == "published" and restored["revision"] == 2,
          "DB 单元：更高修订解除围栏并恢复")
    check(db.list_withdrawals(str(tmp), section, date)[0]["superseded_by_revision"] == 2,
          "DB 单元：围栏解除时撤回记录被标记取代")

    # Rollback-on-write-failure: hold an IMMEDIATE lock elsewhere so a second
    # writer cannot acquire its write lock; the failed write must roll back
    # cleanly and the committed state must survive.
    holder = db.get_conn(str(tmp))
    holder.execute("BEGIN IMMEDIATE")
    blocked_conn = db.get_conn(str(tmp))
    blocked_conn.execute("PRAGMA busy_timeout=400")
    import sqlite3 as _sqlite3  # noqa: E402
    locked = False
    try:
        blocked_conn.execute("BEGIN IMMEDIATE")
        blocked_conn.execute(
            "DELETE FROM published WHERE section=? AND date=?", (section, date)
        )
        blocked_conn.execute("COMMIT")
    except _sqlite3.OperationalError:
        # BEGIN IMMEDIATE itself may have failed (no transaction active), so
        # roll back only if one is open -- mirrors the app's own helper.
        locked = True
        db.rollback_safely(blocked_conn)
    db.rollback_safely(holder)
    holder.close()
    blocked_conn.close()
    check(locked, "DB 单元：写入拿不到锁时失败")
    survivor = db.get_published(str(tmp), section, date)
    check(survivor["status"] == "published" and survivor["revision"] == 2
          and len(survivor["rows"]) == 1,
          "DB 单元：写入失败后原状态完整保留")
    tmp.unlink(missing_ok=True)
    for suffix in ("-wal", "-shm"):
        Path(str(tmp) + suffix).unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Water-quality platform demo")
    parser.add_argument("--station", default="http://localhost:8001")
    parser.add_argument("--web1", default="http://localhost:8011")
    parser.add_argument("--web2", default="http://localhost:8012")
    parser.add_argument("--section",
                        help="defaults to a unique per-invocation section so "
                             "repeated demo runs never share local DB state")
    parser.add_argument("--date1")
    parser.add_argument("--date2")
    parser.add_argument("--date3")
    parser.add_argument("--date4")
    parser.add_argument("--interleave-date")
    parser.add_argument("--run-tag",
                        default=f"{time.strftime('%H%M%S')}-{os.getpid()}",
                        help="drives the unique section/dates for this run")
    args = parser.parse_args()

    # Isolation strategy: every invocation uses a *unique section name* (the
    # station lazily synthesizes data for any section, and every local DB row
    # is keyed by section+date). Combined with spread-out dates this makes it
    # impossible for one run -- even a crashed/interrupted one -- to leave
    # state that a later run could read on the shared persistent SQLite volume.
    if not args.section:
        args.section = f"DEMO-{args.run_tag}"

    # With a unique section, fixed human-readable dates are safe and clear.
    args.date1 = args.date1 or "2026-11-13"
    args.date2 = args.date2 or "2026-11-12"
    args.date3 = args.date3 or "2026-11-11"
    args.date4 = args.date4 or "2026-11-10"
    # Separate day for the interleaving subtest (still under the unique section).
    args.interleave_date = args.interleave_date or "2026-11-09"

    print(f"station={args.station} web1={args.web1} web2={args.web2} "
          f"section={args.section} "
          f"dates={args.date1}/{args.date2}/{args.date3}/{args.date4} "
          f"interleave={args.interleave_date}")
    try:
        scenario_mid_correction(args.station, args.web1, args.section, args.date1)
        scenario_read_failure(args.station, args.web1, args.section, args.date2)
        scenario_concurrency(args.station, args.web1, args.web2, args.section, args.date3)
        scenario_withdrawal(args.station, args.web1, args.web2, args.section, args.date4,
                            args.interleave_date)
    except Exception as exc:
        print(f"\n演示中断: {exc!r}")
        return 1

    passed = sum(1 for ok, _ in RESULTS if ok)
    print(f"\n==== 结果: {passed}/{len(RESULTS)} 项检查通过 ====")
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
