"""
refresher.py
Background refresh manager.

Runs AAP fetches in a daemon thread so the UI stays responsive.
One refresh job per host_key at a time — duplicate requests are ignored.

Usage:
    from refresher import start_refresh, get_status

    start_refresh("prod-us")          # fire and forget
    status = get_status("prod-us")    # poll from UI
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Dict

from aap_client import get_client
from services import ee_service, project_service, template_service, org_service, cred_service


# ── Job state ─────────────────────────────────────────────────
# { host_key: { "status": "idle|refreshing|done|error",
#               "started_at": float,
#               "finished_at": float | None,
#               "error": str | None } }
_jobs: Dict[str, dict] = {}
_lock = threading.Lock()


def _make_job(status: str = "idle") -> dict:
    return {
        "status":      status,
        "started_at":  time.time(),
        "finished_at": None,
        "error":       None,
    }


def get_status(host_key: str, scope: str = "all") -> dict:
    """Returns the current refresh job state for a host+scope."""
    job_key = f"{host_key}:{scope}"
    with _lock:
        job = _jobs.get(job_key, {"status": "idle"})
        return dict(job)


def is_refreshing(host_key: str) -> bool:
    with _lock:
        # True if ANY scope is currently refreshing for this host
        return any(
            v.get("status") == "refreshing"
            for k, v in _jobs.items()
            if k.startswith(host_key + ":")
        )


def start_refresh(host_key: str, force: bool = True, scope: str = "all") -> bool:
    """
    Starts a background refresh for host_key + scope.
    Returns True if a new job was started, False if one was already running.
    Scoped refreshes (ee, credentials) run independently of each other.
    """
    job_key = f"{host_key}:{scope}"
    with _lock:
        if _jobs.get(job_key, {}).get("status") == "refreshing":
            return False  # already in progress for this scope
        _jobs[job_key] = _make_job("refreshing")

    thread = threading.Thread(
        target=_run_refresh,
        args=(job_key, host_key, force, scope),
        daemon=True,
        name=f"refresh-{host_key}-{scope}",
    )
    thread.start()
    return True


def _run_refresh(job_key: str, host_key: str, force: bool, scope: str = "all") -> None:
    """Worker: fetches resources for a host and writes to DB cache.
    scope: 'ee' = EE resources only, 'credentials' = creds only, 'all' = everything.
    """
    try:
        client = get_client(host_key)
        if scope in ("ee", "all"):
            ee_service.get_all(client, host_key, force=force)
            org_service.get_with_ee(client, host_key, force=force)
            project_service.get_with_ee(client, host_key, force=force)
            template_service.get_with_ee(client, host_key, force=force)
        if scope in ("credentials", "all"):
            cred_service.get_all(client, host_key, force=force)

        with _lock:
            _jobs[job_key]["status"]      = "done"
            _jobs[job_key]["finished_at"] = time.time()

    except Exception as exc:
        with _lock:
            _jobs[job_key]["status"]      = "error"
            _jobs[job_key]["finished_at"] = time.time()
            _jobs[job_key]["error"]       = str(exc)
