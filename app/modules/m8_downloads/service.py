"""目录下载编排：分片生成、租约、容量账、取消与回收。

不变量：
- 创建即锁定批次（run_id 与内容摘要写死），之后新批次不影响已建任务。
- 容量账全部由库表推导，重启不丢；活跃任务按预留计，收尾任务按在租片段计。
- 取消/失败只立即回收未被领取的片段；已领取片段留到租约到期再回收。
- 生成游标以片段表为准，中断后从已落盘片段续跑，重试不重复落片段。
"""

from __future__ import annotations

import base64
import uuid
from datetime import datetime

from app import config, errors, store
from app.modules.m7_migration import csv_out
from app.modules.m8_downloads import fragments

ACTIVE_STATUSES = ("generating", "ready")
CLOSED_STATUSES = ("cancelled", "failed")

TASK_STATUS_LABELS = {
    "generating": "生成中",
    "ready": "待领取",
    "cancelled": "已取消",
    "failed": "生成失败",
}

FRAGMENT_STATUS_LABELS = {
    "persisted": "待领取",
    "claimed": "已领取",
    "reclaimed": "已回收",
}


class GenerationInterrupted(Exception):
    """测试注入的中断：任务保持 generating，已落盘片段可续跑。"""


# ---------- 基础 ----------

def _moment(now: datetime | None) -> datetime:
    return now if now is not None else config.now_utc()


def _iso(moment: datetime) -> str:
    return fragments.iso(moment)


def _render_content(run: dict) -> bytes:
    return csv_out.fields_csv(run.get("diff") or []).encode("utf-8")


def _require_task(task_id: str) -> dict:
    task = store.download_task(task_id)
    if task is None:
        raise errors.NotFound("下载任务不存在", detail={"task_id": task_id})
    return task


def _validate_fragment_bytes(value) -> int:
    if value is None:
        return config.DOWNLOAD_FRAGMENT_DEFAULT
    if isinstance(value, bool) or not isinstance(value, int):
        raise errors.BadInput("fragment_bytes 必须是整数",
                              detail={"value": repr(value)})
    if value < 1:
        raise errors.BadInput("fragment_bytes 至少 1 字节", detail={"value": value})
    if value > config.DOWNLOAD_FRAGMENT_MAX:
        raise errors.BadInput("fragment_bytes 超过上限",
                              detail={"value": value,
                                      "max": config.DOWNLOAD_FRAGMENT_MAX})
    return value


# ---------- 回收 ----------

def _reclaim(fragment: dict, reason: str, stamp: str) -> dict:
    """终态回收：释放字节、丢掉内容、记下原因；不碰在租片段。"""
    fragment["status"] = "reclaimed"
    fragment["content"] = None
    fragment["lease_token"] = None
    fragment["lease_expires_at"] = None
    fragment["reclaimed_at"] = stamp
    fragment["reclaim_reason"] = reason
    return fragment


def _release(fragment: dict) -> dict:
    """活跃任务上的租约到期：片段回到待领取，内容保留，字节仍算在预留里。"""
    fragment["status"] = "persisted"
    fragment["lease_token"] = None
    fragment["lease_expires_at"] = None
    fragment["claimed_at"] = None
    return fragment


def _sweep(moment: datetime) -> int:
    """把到期的租约收尾：活跃任务释放回待领取，收尾任务按到期回收。"""
    stamp = _iso(moment)
    expired = store.list_expired_claims(stamp)
    for item in expired:
        if item["task_status"] in CLOSED_STATUSES:
            store.save_download_fragment(_reclaim(item, "LEASE_EXPIRED", stamp))
        else:
            store.save_download_fragment(_release(item))
    return len(expired)


def sweep(now: datetime | None = None) -> int:
    with store.locked():
        return _sweep(_moment(now))


# ---------- 占用账 ----------

def _held_bytes(row: dict) -> int:
    if row["status"] in ACTIVE_STATUSES:
        return int(row["total_bytes"])
    return int(row["claimed_bytes"])


def occupancy(now: datetime | None = None) -> dict:
    with store.locked():
        _sweep(_moment(now))
        rows = store.download_occupancy_rows()
    tasks = [
        {"task_id": row["task_id"], "status": row["status"],
         "held_bytes": _held_bytes(row)}
        for row in rows
    ]
    occupied = sum(item["held_bytes"] for item in tasks)
    capacity = config.DOWNLOAD_CAPACITY_BYTES
    return {
        "capacity_bytes": capacity,
        "occupied_bytes": occupied,
        "available_bytes": max(0, capacity - occupied),
        "tasks": tasks,
    }


# ---------- 视图 ----------

def fragment_view(fragment: dict) -> dict:
    return {
        "seq": fragment["seq"],
        "byte_start": fragment["byte_start"],
        "byte_end": fragment["byte_end"],
        "bytes": fragment["byte_end"] - fragment["byte_start"],
        "sha256": fragment["sha256"],
        "status": fragment["status"],
        "status_label": FRAGMENT_STATUS_LABELS.get(fragment["status"],
                                                   fragment["status"]),
        "lease_expires_at": fragment.get("lease_expires_at"),
        "claimed_at": fragment.get("claimed_at"),
        "reclaimed_at": fragment.get("reclaimed_at"),
        "reclaim_reason": fragment.get("reclaim_reason"),
    }


def task_view(task: dict, with_fragments: bool = False) -> dict:
    view = {
        "task_id": task["task_id"],
        "run_id": task["run_id"],
        "status": task["status"],
        "status_label": TASK_STATUS_LABELS.get(task["status"], task["status"]),
        "fragment_bytes": task["fragment_bytes"],
        "total_bytes": task["total_bytes"],
        "generated_bytes": task["generated_bytes"],
        "fragment_count": task["fragment_count"],
        "created_at": task["created_at"],
        "updated_at": task["updated_at"],
        "finished_at": task.get("finished_at"),
        "error": task.get("error"),
    }
    if with_fragments:
        view["fragments"] = [
            fragment_view(row) for row in store.list_download_fragments(task["task_id"])
        ]
    return view


def list_tasks(now: datetime | None = None) -> list[dict]:
    with store.locked():
        _sweep(_moment(now))
        tasks = store.list_download_tasks()
        held = {row["task_id"]: _held_bytes(row)
                for row in store.download_occupancy_rows()}
    views = []
    for task in tasks:
        view = task_view(task)
        view["held_bytes"] = held.get(task["task_id"], 0)
        views.append(view)
    return views


def task_detail(task_id: str, now: datetime | None = None) -> dict:
    with store.locked():
        _sweep(_moment(now))
        task = _require_task(task_id)
        held = {row["task_id"]: _held_bytes(row)
                for row in store.download_occupancy_rows()}
    view = task_view(task, with_fragments=True)
    view["held_bytes"] = held.get(task["task_id"], 0)
    return view


# ---------- 创建 ----------

def create_task(idem_key: str | None = None, fragment_bytes=None,
                now: datetime | None = None) -> dict:
    moment = _moment(now)
    size = _validate_fragment_bytes(fragment_bytes)
    key = (idem_key or "").strip()
    if len(key) > 128:
        raise errors.BadInput("idem_key 最长 128 字符", detail={"length": len(key)})
    if not key:
        key = "auto-" + uuid.uuid4().hex

    with store.locked():
        existing = store.download_task_by_idem(key)
        if existing is not None:
            if existing["fragment_bytes"] != size:
                raise errors.Conflict(
                    "同一幂等键的 fragment_bytes 不一致",
                    detail={"idem_key": key, "task_id": existing["task_id"]},
                )
            view = task_view(existing)
            view["reused"] = True
            return view

        run = store.latest_run()
        if run is None:
            raise errors.NotFound("还没有批次，先在版本历史页跑一次")
        content = _render_content(run)
        total = len(content)

        account = occupancy(moment)
        if account["occupied_bytes"] + total > config.DOWNLOAD_CAPACITY_BYTES:
            raise errors.CapacityExceeded(
                "容量不足，下载任务已拒绝",
                detail={"capacity_bytes": config.DOWNLOAD_CAPACITY_BYTES,
                        "occupied_bytes": account["occupied_bytes"],
                        "requested_bytes": total,
                        "reason": "CAPACITY_EXCEEDED"},
            )

        stamp = _iso(moment)
        task = {
            "task_id": "dl-" + fragments.sha256_bytes(key.encode("utf-8"))[:12],
            "idem_key": key,
            "run_id": run["run_id"],
            "status": "generating",
            "fragment_bytes": size,
            "total_bytes": total,
            "generated_bytes": 0,
            "fragment_count": 0,
            "content_sha256": fragments.sha256_bytes(content),
            "created_at": stamp,
            "updated_at": stamp,
            "finished_at": None,
            "error": None,
        }
        store.save_download_task(task)
        view = task_view(task)
        view["reused"] = False
        return view


# ---------- 生成 ----------

def _persisted_cursor(task_id: str, content: bytes) -> tuple[int, int]:
    """以片段表为准的续跑游标：(已生成字节, 下一个序号)，并校验已落盘内容。"""
    rows = store.list_download_fragments(task_id, with_content=True)
    generated = 0
    for row in rows:
        blob = row["content"] or b""
        start, end = int(row["byte_start"]), int(row["byte_end"])
        if content[start:end] != blob or fragments.sha256_bytes(blob) != row["sha256"]:
            raise errors.Conflict("已落盘片段与锁定批次内容不一致",
                                  detail={"task_id": task_id, "seq": row["seq"]})
        generated += end - start
    next_seq = (int(rows[-1]["seq"]) + 1) if rows else 0
    return generated, next_seq


def _fail_task(task: dict, error: str, moment: datetime) -> None:
    """生成失败收尾：未领取的片段立即回收，在租片段留到到期。"""
    stamp = _iso(moment)
    task["status"] = "failed"
    task["error"] = error
    task["updated_at"] = stamp
    task["finished_at"] = stamp
    for row in store.list_download_fragments(task["task_id"]):
        if row["status"] == "claimed" and row["lease_expires_at"] > stamp:
            continue
        if row["status"] == "claimed":
            store.save_download_fragment(_reclaim(row, "LEASE_EXPIRED", stamp))
        else:
            store.save_download_fragment(_reclaim(row, "GEN_FAILED", stamp))
    store.save_download_task(task)


def generate(task_id: str, now: datetime | None = None,
             fail_after: int | None = None) -> dict:
    moment = _moment(now)
    with store.locked():
        task = _require_task(task_id)
        if task["status"] == "cancelled":
            raise errors.TaskCancelled("任务已取消，不再生成",
                                       detail={"task_id": task_id})
        if task["status"] == "failed":
            raise errors.Conflict("任务已失败，不能续跑",
                                  detail={"task_id": task_id, "error": task["error"]})
        if task["status"] == "ready":
            return task_view(task)

        run = store.run_by_id(task["run_id"])
        if run is None:
            raise errors.Conflict("锁定批次不存在",
                                  detail={"task_id": task_id, "run_id": task["run_id"]})
        try:
            content = _render_content(run)
            if fragments.sha256_bytes(content) != task["content_sha256"]:
                raise errors.Conflict("锁定批次内容已变化",
                                      detail={"task_id": task_id})
            generated, seq = _persisted_cursor(task_id, content)
            task["generated_bytes"] = generated
            produced = 0
            while generated < task["total_bytes"]:
                start, end = fragments.slice_bounds(
                    generated, generated + task["fragment_bytes"], task["total_bytes"])
                blob = content[start:end]
                row = {
                    "task_id": task_id, "seq": seq, "byte_start": start,
                    "byte_end": end, "sha256": fragments.sha256_bytes(blob),
                    "status": "persisted", "content": blob,
                    "lease_token": None, "lease_expires_at": None,
                    "claimed_at": None, "reclaimed_at": None, "reclaim_reason": None,
                }
                generated = end
                task["generated_bytes"] = generated
                task["fragment_count"] = seq + 1
                task["updated_at"] = _iso(moment)
                store.save_download_fragment_and_task(row, task)
                seq += 1
                produced += 1
                if fail_after is not None and produced >= fail_after:
                    raise GenerationInterrupted(f"生成中断于第 {seq} 片")
            task["status"] = "ready"
            task["finished_at"] = _iso(moment)
            task["updated_at"] = _iso(moment)
            store.save_download_task(task)
        except GenerationInterrupted:
            raise
        except Exception as exc:
            _fail_task(task, f"{type(exc).__name__}: {exc}", moment)
            raise
        return task_view(task)


# ---------- 领取 / 续租 / 续传 ----------

def _require_fragment(task_id: str, seq: int) -> dict:
    fragment = store.download_fragment(task_id, seq)
    if fragment is None:
        raise errors.NotFound("片段不存在", detail={"task_id": task_id, "seq": seq})
    return fragment


def _deliver(fragment: dict, task: dict) -> dict:
    view = fragment_view(fragment)
    view["task_id"] = task["task_id"]
    view["run_id"] = task["run_id"]
    view["lease_token"] = fragment["lease_token"]
    blob = fragment.get("content") or b""
    view["content_b64"] = base64.b64encode(blob).decode("ascii")
    return view


def claim(task_id: str, seq: int | None = None, now: datetime | None = None) -> dict:
    moment = _moment(now)
    stamp = _iso(moment)
    with store.locked():
        task = _require_task(task_id)
        if task["status"] == "cancelled":
            raise errors.TaskCancelled("任务已取消，不再发放新片段",
                                       detail={"task_id": task_id})
        if task["status"] == "failed":
            raise errors.Conflict("任务已失败，不能领取",
                                  detail={"task_id": task_id, "reason": "GEN_FAILED"})
        _sweep(moment)

        if seq is None:
            waiting = [row for row in store.list_download_fragments(task_id)
                       if row["status"] == "persisted"]
            if not waiting:
                raise errors.Conflict("没有可领取的片段",
                                      detail={"task_id": task_id,
                                              "status": task["status"]})
            seq = waiting[0]["seq"]
        fragment = _require_fragment(task_id, seq)

        if fragment["status"] == "reclaimed":
            raise errors.Conflict("片段已回收",
                                  detail={"task_id": task_id, "seq": seq,
                                          "reason": fragment["reclaim_reason"]})
        if fragment["status"] == "claimed":
            if fragment["lease_expires_at"] > stamp:
                raise errors.Conflict(
                    "片段已被领取",
                    detail={"task_id": task_id, "seq": seq,
                            "lease_expires_at": fragment["lease_expires_at"]})
            fragment = _release(fragment)

        fragment["status"] = "claimed"
        fragment["lease_token"] = fragments.new_lease_token()
        fragment["lease_expires_at"] = fragments.lease_expiry(
            moment, config.DOWNLOAD_LEASE_SECONDS)
        fragment["claimed_at"] = stamp
        store.save_download_fragment(fragment)
        return _deliver(fragment, task)


def renew(task_id: str, seq: int, lease_token: str,
          now: datetime | None = None) -> dict:
    moment = _moment(now)
    stamp = _iso(moment)
    with store.locked():
        task = _require_task(task_id)
        fragment = _require_fragment(task_id, seq)
        _check_lease(fragment, lease_token, stamp)
        if task["status"] in CLOSED_STATUSES:
            raise errors.TaskCancelled("任务已取消，租约不再续期",
                                       detail={"task_id": task_id, "seq": seq})
        fragment["lease_expires_at"] = fragments.lease_expiry(
            moment, config.DOWNLOAD_LEASE_SECONDS)
        store.save_download_fragment(fragment)
        view = fragment_view(fragment)
        view["task_id"] = task_id
        return view


def resume(task_id: str, seq: int, lease_token: str,
           now: datetime | None = None) -> dict:
    """续传：凭证有效即按原批次内容重发，取消后也服务到租约到期。"""
    moment = _moment(now)
    stamp = _iso(moment)
    with store.locked():
        task = _require_task(task_id)
        fragment = _require_fragment(task_id, seq)
        _check_lease(fragment, lease_token, stamp)
        return _deliver(fragment, task)


def _check_lease(fragment: dict, lease_token: str, stamp: str) -> None:
    """过期凭证一律拒绝；回收过的片段按到期处理。"""
    if fragment["status"] == "reclaimed":
        raise errors.LeaseExpired("租约已到期，片段已回收",
                                  detail={"seq": fragment["seq"],
                                          "reason": fragment["reclaim_reason"]})
    if fragment["status"] != "claimed":
        raise errors.LeaseExpired("租约已到期",
                                  detail={"seq": fragment["seq"]})
    if not lease_token or fragment["lease_token"] != lease_token:
        raise errors.Conflict("租约凭证不匹配",
                              detail={"seq": fragment["seq"]})
    if fragment["lease_expires_at"] <= stamp:
        raise errors.LeaseExpired("租约已到期，凭证失效",
                                  detail={"seq": fragment["seq"],
                                          "lease_expires_at": fragment["lease_expires_at"]})


# ---------- 取消 ----------

def cancel(task_id: str, now: datetime | None = None) -> dict:
    moment = _moment(now)
    stamp = _iso(moment)
    with store.locked():
        task = _require_task(task_id)
        if task["status"] in CLOSED_STATUSES:
            view = task_view(task)
            view["already"] = True
            return view
        task["status"] = "cancelled"
        task["updated_at"] = stamp
        task["finished_at"] = stamp
        for row in store.list_download_fragments(task_id):
            if row["status"] == "claimed" and row["lease_expires_at"] > stamp:
                continue  # 已领取片段按原批次留到租约到期
            if row["status"] == "claimed":
                store.save_download_fragment(_reclaim(row, "LEASE_EXPIRED", stamp))
            else:
                store.save_download_fragment(_reclaim(row, "TASK_CANCELLED", stamp))
        store.save_download_task(task)
        return task_view(task)
