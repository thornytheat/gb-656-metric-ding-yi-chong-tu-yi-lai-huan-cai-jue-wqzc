"""M8 入口：目录下载页与分片交付接口。"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app import errors, web
from app.modules.m8_downloads import service

router = APIRouter()

CREATE_KEYS = {"idem_key", "fragment_bytes"}
CLAIM_KEYS = {"seq"}
LEASE_KEYS = {"seq", "lease_token"}


def _reject_unknown(body: dict, allowed: set[str]) -> None:
    unknown = set(body) - allowed
    if unknown:
        raise errors.BadInput("出现未知参数", detail={"keys": sorted(unknown)})


def _seq_of(body: dict) -> int | None:
    value = body.get("seq")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise errors.BadInput("seq 必须是非负整数", detail={"value": repr(value)})
    return value


def _token_of(body: dict) -> str:
    token = body.get("lease_token")
    if not isinstance(token, str) or not token.strip():
        raise errors.BadInput("需要 lease_token", detail={"field": "lease_token"})
    return token.strip()


@router.get("/downloads", response_class=HTMLResponse)
def downloads_page(request: Request):
    tasks = service.list_tasks()
    detailed = [service.task_detail(item["task_id"]) for item in tasks]
    return web.render(
        request,
        "downloads.html",
        "downloads",
        account=service.occupancy(),
        tasks=detailed,
    )


@router.post("/api/downloads")
def create_download(payload: dict | None = None):
    body = payload or {}
    _reject_unknown(body, CREATE_KEYS)
    return service.create_task(
        idem_key=body.get("idem_key"),
        fragment_bytes=body.get("fragment_bytes"),
    )


@router.get("/api/downloads")
def list_downloads():
    return {"occupancy": service.occupancy(), "tasks": service.list_tasks()}


@router.get("/api/downloads/{task_id}")
def download_detail(task_id: str):
    return service.task_detail(task_id)


@router.post("/api/downloads/{task_id}/generate")
def generate_download(task_id: str):
    return service.generate(task_id)


@router.post("/api/downloads/{task_id}/claim")
def claim_fragment(task_id: str, payload: dict | None = None):
    body = payload or {}
    _reject_unknown(body, CLAIM_KEYS)
    return service.claim(task_id, seq=_seq_of(body))


@router.post("/api/downloads/{task_id}/renew")
def renew_lease(task_id: str, payload: dict | None = None):
    body = payload or {}
    _reject_unknown(body, LEASE_KEYS)
    seq = _seq_of(body)
    if seq is None:
        raise errors.BadInput("需要 seq", detail={"field": "seq"})
    return service.renew(task_id, seq, _token_of(body))


@router.post("/api/downloads/{task_id}/resume")
def resume_fragment(task_id: str, payload: dict | None = None):
    body = payload or {}
    _reject_unknown(body, LEASE_KEYS)
    seq = _seq_of(body)
    if seq is None:
        raise errors.BadInput("需要 seq", detail={"field": "seq"})
    return service.resume(task_id, seq, _token_of(body))


@router.post("/api/downloads/{task_id}/cancel")
def cancel_download(task_id: str):
    return service.cancel(task_id)
