"""M8 目录下载：分片交付、租约、容量账、取消与回收。"""

from __future__ import annotations

import base64
from datetime import timedelta

import pytest

from app import config, errors, store
from app.modules.m7_migration import service as migration
from app.modules.m8_downloads import service as downloads

FRAG = 200


def _catalog_bytes() -> bytes:
    return migration.fields_csv_text(store.latest_run()).encode("utf-8")


def _make(fragment_bytes=FRAG, idem_key="k-1", now=None):
    task = downloads.create_task(idem_key=idem_key, fragment_bytes=fragment_bytes, now=now)
    return downloads.generate(task["task_id"], now=now)


def test_create_locks_batch_and_reserves(client):
    run = store.latest_run()
    task = downloads.create_task(idem_key="k-lock", fragment_bytes=FRAG)
    assert task["run_id"] == run["run_id"]
    assert task["status"] == "generating"
    assert task["total_bytes"] == len(_catalog_bytes())
    account = downloads.occupancy()
    assert account["occupied_bytes"] == task["total_bytes"]
    assert account["available_bytes"] == account["capacity_bytes"] - task["total_bytes"]


def test_create_idempotent_retry(client):
    first = downloads.create_task(idem_key="k-same", fragment_bytes=FRAG)
    again = downloads.create_task(idem_key="k-same", fragment_bytes=FRAG)
    assert again["task_id"] == first["task_id"]
    assert again["reused"] is True
    assert store.count_download_tasks() == 1
    with pytest.raises(errors.Conflict):
        downloads.create_task(idem_key="k-same", fragment_bytes=FRAG + 1)
    assert store.count_download_tasks() == 1


def test_fragment_bytes_validation(client):
    with pytest.raises(errors.BadInput):
        downloads.create_task(idem_key="k-bad", fragment_bytes=0)
    with pytest.raises(errors.BadInput):
        downloads.create_task(idem_key="k-bad", fragment_bytes=True)
    with pytest.raises(errors.BadInput):
        downloads.create_task(idem_key="k-bad",
                              fragment_bytes=config.DOWNLOAD_FRAGMENT_MAX + 1)
    assert store.count_download_tasks() == 0


def test_capacity_exceeded_explicit(client, monkeypatch):
    total = len(_catalog_bytes())
    monkeypatch.setattr(config, "DOWNLOAD_CAPACITY_BYTES", total - 1)
    with pytest.raises(errors.CapacityExceeded) as caught:
        downloads.create_task(idem_key="k-fat", fragment_bytes=FRAG)
    detail = caught.value.detail
    assert detail["reason"] == "CAPACITY_EXCEEDED"
    assert detail["requested_bytes"] == total
    assert detail["occupied_bytes"] == 0
    assert store.count_download_tasks() == 0


def test_generate_resumes_after_interruption(client):
    task = downloads.create_task(idem_key="k-int", fragment_bytes=FRAG)
    with pytest.raises(downloads.GenerationInterrupted):
        downloads.generate(task["task_id"], fail_after=3)
    partial = store.download_task(task["task_id"])
    assert partial["status"] == "generating"
    assert partial["fragment_count"] == 3
    assert partial["generated_bytes"] == 3 * FRAG

    done = downloads.generate(task["task_id"])
    assert done["status"] == "ready"
    rows = store.list_download_fragments(task["task_id"])
    assert [row["seq"] for row in rows] == list(range(len(rows)))
    assert len(rows) == done["fragment_count"]
    blob = b"".join(
        row["content"] for row in
        store.list_download_fragments(task["task_id"], with_content=True)
    )
    assert blob == _catalog_bytes()


def test_generate_retry_does_not_duplicate(client):
    task = _make()
    assert downloads.generate(task["task_id"])["status"] == "ready"
    assert downloads.generate(task["task_id"])["fragment_count"] == task["fragment_count"]
    assert len(store.list_download_fragments(task["task_id"])) == task["fragment_count"]


def test_claim_and_resume_deliver_locked_slice(client):
    task = _make()
    lease = downloads.claim(task["task_id"], 0)
    content = base64.b64decode(lease["content_b64"])
    assert content == _catalog_bytes()[:FRAG]
    assert lease["lease_token"]
    again = downloads.resume(task["task_id"], 0, lease["lease_token"])
    assert again["content_b64"] == lease["content_b64"]
    with pytest.raises(errors.Conflict):
        downloads.resume(task["task_id"], 0, "wrong-token")
    with pytest.raises(errors.Conflict):
        downloads.claim(task["task_id"], 0)


def test_claim_picks_next_waiting_fragment(client):
    task = _make()
    first = downloads.claim(task["task_id"])
    second = downloads.claim(task["task_id"])
    assert (first["seq"], second["seq"]) == (0, 1)


def test_claim_after_cancel_rejected(client):
    task = _make()
    downloads.cancel(task["task_id"])
    with pytest.raises(errors.TaskCancelled):
        downloads.claim(task["task_id"])


def test_cancel_releases_capacity_but_keeps_claims(client, monkeypatch):
    t0 = config.now_utc()
    total = len(_catalog_bytes())
    monkeypatch.setattr(config, "DOWNLOAD_CAPACITY_BYTES", total)
    first = _make(idem_key="k-a", now=t0)
    lease = downloads.claim(first["task_id"], 0, now=t0)

    with pytest.raises(errors.CapacityExceeded):
        downloads.create_task(idem_key="k-b", fragment_bytes=FRAG, now=t0)

    downloads.cancel(first["task_id"], now=t0 + timedelta(seconds=5))
    account = downloads.occupancy(now=t0 + timedelta(seconds=5))
    assert account["occupied_bytes"] == FRAG  # 只剩在租片段
    with pytest.raises(errors.CapacityExceeded):
        downloads.create_task(idem_key="k-b", fragment_bytes=FRAG, now=t0)

    later = t0 + timedelta(seconds=config.DOWNLOAD_LEASE_SECONDS + 1)
    downloads.sweep(now=later)
    assert downloads.occupancy(now=later)["occupied_bytes"] == 0
    second = downloads.create_task(idem_key="k-b", fragment_bytes=FRAG, now=later)
    assert second["run_id"] == first["run_id"]


def test_claimed_fragment_survives_cancel_until_lease_expiry(client):
    t0 = config.now_utc()
    task = _make(idem_key="k-keep", now=t0)
    lease = downloads.claim(task["task_id"], 1, now=t0)
    downloads.cancel(task["task_id"], now=t0 + timedelta(seconds=5))

    kept = store.download_fragment(task["task_id"], 1)
    assert kept["status"] == "claimed"
    gone = store.download_fragment(task["task_id"], 2)
    assert (gone["status"], gone["reclaim_reason"]) == ("reclaimed", "TASK_CANCELLED")
    assert gone["content"] is None

    still = downloads.resume(task["task_id"], 1, lease["lease_token"],
                             now=t0 + timedelta(seconds=10))
    assert base64.b64decode(still["content_b64"]) == _catalog_bytes()[FRAG:2 * FRAG]

    later = t0 + timedelta(seconds=config.DOWNLOAD_LEASE_SECONDS + 1)
    with pytest.raises(errors.LeaseExpired):
        downloads.resume(task["task_id"], 1, lease["lease_token"], now=later)
    downloads.sweep(now=later)
    recycled = store.download_fragment(task["task_id"], 1)
    assert (recycled["status"], recycled["reclaim_reason"]) == ("reclaimed", "LEASE_EXPIRED")


def test_renew_extends_and_rejects_expired_credential(client):
    t0 = config.now_utc()
    task = _make(idem_key="k-renew", now=t0)
    lease = downloads.claim(task["task_id"], 0, now=t0)
    renewed = downloads.renew(task["task_id"], 0, lease["lease_token"],
                              now=t0 + timedelta(seconds=30))
    assert renewed["lease_expires_at"] > lease["lease_expires_at"]

    # 续租与取消并发：过期凭证必须被拒
    downloads.cancel(task["task_id"], now=t0 + timedelta(seconds=40))
    from datetime import datetime

    expiry = datetime.fromisoformat(renewed["lease_expires_at"])
    with pytest.raises(errors.LeaseExpired):
        downloads.renew(task["task_id"], 0, lease["lease_token"],
                        now=expiry + timedelta(seconds=1))


def test_renew_on_cancelled_task_rejected(client):
    t0 = config.now_utc()
    task = _make(idem_key="k-renew2", now=t0)
    lease = downloads.claim(task["task_id"], 0, now=t0)
    downloads.cancel(task["task_id"], now=t0 + timedelta(seconds=5))
    with pytest.raises(errors.TaskCancelled):
        downloads.renew(task["task_id"], 0, lease["lease_token"],
                        now=t0 + timedelta(seconds=10))


def test_failure_cleanup_keeps_active_leases(client, monkeypatch):
    t0 = config.now_utc()
    task = downloads.create_task(idem_key="k-fail", fragment_bytes=FRAG, now=t0)
    with pytest.raises(downloads.GenerationInterrupted):
        downloads.generate(task["task_id"], now=t0, fail_after=2)
    lease = downloads.claim(task["task_id"], 0, now=t0)

    def boom(_run):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(downloads, "_render_content", boom)
    with pytest.raises(RuntimeError):
        downloads.generate(task["task_id"], now=t0 + timedelta(seconds=1))
    failed = store.download_task(task["task_id"])
    assert failed["status"] == "failed"

    # 在租片段不被失败清理误删，未领取的按 GEN_FAILED 回收
    kept = store.download_fragment(task["task_id"], 0)
    assert kept["status"] == "claimed"
    recycled = store.download_fragment(task["task_id"], 1)
    assert (recycled["status"], recycled["reclaim_reason"]) == ("reclaimed", "GEN_FAILED")
    assert downloads.occupancy(now=t0)["occupied_bytes"] == FRAG
    still = downloads.resume(task["task_id"], 0, lease["lease_token"],
                             now=t0 + timedelta(seconds=2))
    assert still["seq"] == 0


def test_locked_batch_ignores_newer_run(client):
    t0 = config.now_utc()
    run_one = store.latest_run()
    task = _make(idem_key="k-pin", now=t0)
    lease = downloads.claim(task["task_id"], 0, now=t0)

    client.post("/api/runs", json={"params": {"wave": "two"}})
    assert store.latest_run()["run_id"] != run_one["run_id"]

    still = downloads.resume(task["task_id"], 0, lease["lease_token"],
                             now=t0 + timedelta(seconds=1))
    expected = migration.fields_csv_text(run_one).encode("utf-8")[:FRAG]
    assert base64.b64decode(still["content_b64"]) == expected
    assert store.download_task(task["task_id"])["run_id"] == run_one["run_id"]


def test_occupancy_survives_restart(client):
    response = client.post("/api/downloads", json={"idem_key": "k-boot", "fragment_bytes": FRAG})
    assert response.status_code == 200
    task_id = response.json()["task_id"]
    client.post(f"/api/downloads/{task_id}/generate")
    before = client.get("/api/downloads").json()["occupancy"]
    assert before["occupied_bytes"] == len(_catalog_bytes())

    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as rebooted:
        after = rebooted.get("/api/downloads").json()["occupancy"]
    assert after == before


def test_api_flow_and_page(client):
    created = client.post("/api/downloads", json={"idem_key": "k-api", "fragment_bytes": FRAG})
    assert created.status_code == 200
    task_id = created.json()["task_id"]

    generated = client.post(f"/api/downloads/{task_id}/generate")
    assert generated.json()["status"] == "ready"

    lease = client.post(f"/api/downloads/{task_id}/claim", json={}).json()
    assert lease["seq"] == 0
    renewed = client.post(f"/api/downloads/{task_id}/renew",
                          json={"seq": 0, "lease_token": lease["lease_token"]})
    assert renewed.status_code == 200
    resumed = client.post(f"/api/downloads/{task_id}/resume",
                          json={"seq": 0, "lease_token": lease["lease_token"]})
    assert resumed.status_code == 200

    cancelled = client.post(f"/api/downloads/{task_id}/cancel")
    assert cancelled.json()["status"] == "cancelled"
    rejected = client.post(f"/api/downloads/{task_id}/claim", json={})
    assert rejected.status_code == 409
    assert rejected.json()["error"] == "TASK_CANCELLED"

    page = client.get("/downloads")
    assert page.status_code == 200
    assert "目录下载" in page.text
    assert "任务取消回收" in page.text  # 回收原因可见
    assert task_id in page.text


def test_api_capacity_rejection_body(client, monkeypatch):
    total = len(_catalog_bytes())
    monkeypatch.setattr(config, "DOWNLOAD_CAPACITY_BYTES", total)
    client.post("/api/downloads", json={"idem_key": "k-1", "fragment_bytes": FRAG})
    rejected = client.post("/api/downloads", json={"idem_key": "k-2", "fragment_bytes": FRAG})
    assert rejected.status_code == 409
    body = rejected.json()
    assert body["error"] == "CAPACITY_EXCEEDED"
    assert body["detail"]["requested_bytes"] == total


def test_empty_state(empty_client):
    page = empty_client.get("/downloads")
    assert page.status_code == 200
    assert "还没有下载任务" in page.text
    created = empty_client.post("/api/downloads", json={"fragment_bytes": FRAG})
    assert created.status_code == 404


def test_unknown_keys_rejected(client):
    response = client.post("/api/downloads", json={"fragment_bytes": FRAG, "nope": 1})
    assert response.status_code == 400
    assert response.json()["detail"]["keys"] == ["nope"]
