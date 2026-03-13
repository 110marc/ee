"""
services.py
Business logic layer — one service class per AAP resource.

Cache-aside pattern:
  1. Check SQLite for a fresh entry
  2. If fresh  -> return cached data (no AAP call)
  3. If stale/missing -> fetch from AAP, write to DB, return fresh data
"""

from __future__ import annotations
import fnmatch
import logging
from aap_client import AAPClient
import database as db
import config

log = logging.getLogger(__name__)


def _is_excluded(name: str, patterns: list) -> bool:
    """Returns True if name matches any glob pattern in the list."""
    return any(fnmatch.fnmatch(name, p) for p in patterns)


def _extract_ee(record: dict, ee_field: str, summary_field: str) -> dict | None:
    """
    Extract EE id + name from an AAP resource record.

    Args:
        record:        raw AAP API record
        ee_field:      top-level field holding the EE id
                       e.g. "execution_environment" or "default_environment"
        summary_field: key inside summary_fields that holds {"id":..,"name":..}
                       e.g. "execution_environment" or "default_environment"

    Resource-specific calling convention (from AAP API behaviour):
        job_templates  → ee_field="execution_environment",  summary_field="execution_environment"
        projects       → ee_field="default_environment",    summary_field="default_environment"
        organizations  → ee_field="default_environment",    summary_field="default_environment"

    Name resolution order (highest wins):
        1. summary_fields > <summary_field> > name   (most reliable)
        2. top-level ee dict > name                  (fallback)
        3. "EE #<id>"                                (bare int or no name)
    """
    record_id = record.get("id")

    # 1. Best source: summary_fields — always present and correctly named
    sf  = record.get("summary_fields", {})
    sf_ee = sf.get(summary_field, {})
    sf_id   = sf_ee.get("id")
    sf_name = sf_ee.get("name")

    if sf_id and sf_name:
        return {"id": sf_id, "name": sf_name}

    # 2. Fall back to top-level ee field for the id
    ee = record.get(ee_field)

    if ee is None:
        if sf_id:
            # Have id from summary_fields but no name — use it with fallback label
            log.debug("EE id=%s found in summary_fields but no name, record id=%s", sf_id, record_id)
            return {"id": sf_id, "name": f"EE #{sf_id}"}
        return None

    if isinstance(ee, int):
        ee_id = ee
    elif isinstance(ee, dict):
        ee_id = ee.get("id") or sf_id
    else:
        log.warning("Unexpected %s type %s for record id=%s: %r",
                    ee_field, type(ee).__name__, record_id, ee)
        return None

    if not ee_id:
        return None

    name = sf_name or (ee.get("name") if isinstance(ee, dict) else None) or f"EE #{ee_id}"
    return {"id": ee_id, "name": name}


class ExecutionEnvironmentService:
    RESOURCE = "execution_environments"
    ENDPOINT = "/api/v2/execution_environments/"

    def get_all(self, client: AAPClient, host_key: str, force: bool = False, serve_stale: bool = False) -> tuple[list[dict], str]:
        if not force:
            cached, fetched_at = db.get_cached(host_key, self.RESOURCE, ignore_ttl=serve_stale)
            if cached is not None:
                return cached, fetched_at.isoformat()
            return [], None  # cache cold, not forcing — do not hit AAP

        print(f"[AAP FETCH] {self.RESOURCE} host={host_key} force={force}", flush=True)
        raw    = client.get_all(self.ENDPOINT)
        shaped = [self._shape(e) for e in raw]
        db.set_cached(host_key, self.RESOURCE, shaped)
        _, fetched_at = db.get_cached(host_key, self.RESOURCE)
        return shaped, fetched_at.isoformat()

    @staticmethod
    def _shape(e: dict) -> dict:
        org = e.get("organization")
        if isinstance(org, dict):
            org_name = org.get("name", "Global")
        elif isinstance(org, int):
            org_name = f"Org #{org}"
        else:
            org_name = "Global"
        return {
            "id":    e.get("id"),
            "name":  e.get("name", ""),
            "image": e.get("image", ""),
            "pull":  e.get("pull", ""),
            "org":   org_name,
        }


class ProjectService:
    RESOURCE = "projects"
    ENDPOINT = "/api/v2/projects/"

    def get_with_ee(self, client: AAPClient, host_key: str, force: bool = False, serve_stale: bool = False) -> tuple[list[dict], int, str]:
        if not force:
            cached, fetched_at = db.get_cached(host_key, self.RESOURCE, ignore_ttl=serve_stale)
            if cached is not None:
                return cached["assigned"], cached["total"], fetched_at.isoformat()
            return [], 0, None  # cache cold, not forcing — do not hit AAP

        print(f"[AAP FETCH] {self.RESOURCE} host={host_key} force={force}", flush=True)
        raw      = client.get_all(self.ENDPOINT)
        total    = len(raw)
        assigned = []

        excluded = 0
        for p in raw:
            name = p.get("name", "")
            if _is_excluded(name, config.EXCLUDE_PROJECTS):
                excluded += 1
                continue
            ee = _extract_ee(p, "default_environment", "default_environment")
            if ee is None:
                continue
            sf_org = p.get("summary_fields", {}).get("organization", {})
            assigned.append({
                "id":       p.get("id"),
                "name":     name,
                "org_id":   sf_org.get("id"),
                "org_name": sf_org.get("name", ""),
                "ee_id":    ee["id"],
                "ee_name":  ee["name"],
            })

        log.info("Projects: %d total, %d excluded, %d with EE assigned", total, excluded, len(assigned))
        db.set_cached(host_key, self.RESOURCE, {"assigned": assigned, "total": total})
        _, fetched_at = db.get_cached(host_key, self.RESOURCE)
        return assigned, total, fetched_at.isoformat()


class JobTemplateService:
    RESOURCE = "job_templates"
    ENDPOINT = "/api/v2/job_templates/"

    def get_with_ee(self, client: AAPClient, host_key: str, force: bool = False, serve_stale: bool = False) -> tuple[list[dict], int, str]:
        if not force:
            cached, fetched_at = db.get_cached(host_key, self.RESOURCE, ignore_ttl=serve_stale)
            if cached is not None:
                return cached["assigned"], cached["total"], fetched_at.isoformat()
            return [], 0, None  # cache cold, not forcing — do not hit AAP

        print(f"[AAP FETCH] {self.RESOURCE} host={host_key} force={force}", flush=True)
        raw      = client.get_all(self.ENDPOINT)
        total    = len(raw)
        assigned = []

        excluded = 0
        for t in raw:
            name = t.get("name", "")
            if _is_excluded(name, config.EXCLUDE_TEMPLATES):
                excluded += 1
                continue
            ee = _extract_ee(t, "execution_environment", "execution_environment")
            if ee is None:
                continue
            sf_org = t.get("summary_fields", {}).get("organization", {})
            assigned.append({
                "id":       t.get("id"),
                "name":     name,
                "org_id":   sf_org.get("id"),
                "org_name": sf_org.get("name", ""),
                "ee_id":    ee["id"],
                "ee_name":  ee["name"],
            })

        log.info("Job templates: %d total, %d excluded, %d with EE assigned", total, excluded, len(assigned))
        db.set_cached(host_key, self.RESOURCE, {"assigned": assigned, "total": total})
        _, fetched_at = db.get_cached(host_key, self.RESOURCE)
        return assigned, total, fetched_at.isoformat()



class OrganizationService:
    RESOURCE = "organizations"
    ENDPOINT = "/api/v2/organizations/"

    def get_with_ee(self, client: AAPClient, host_key: str, force: bool = False, serve_stale: bool = False) -> tuple[list[dict], int, str]:
        if not force:
            cached, fetched_at = db.get_cached(host_key, self.RESOURCE, ignore_ttl=serve_stale)
            if cached is not None:
                return cached["assigned"], cached["total"], fetched_at.isoformat()
            return [], 0, None  # cache cold, not forcing — do not hit AAP

        print(f"[AAP FETCH] {self.RESOURCE} host={host_key} force={force}", flush=True)
        raw      = client.get_all(self.ENDPOINT)
        total    = len(raw)
        assigned = []

        excluded = 0
        for o in raw:
            name = o.get("name", "")
            if _is_excluded(name, config.EXCLUDE_ORGS):
                excluded += 1
                continue
            ee = _extract_ee(o, "default_environment", "default_environment")
            if ee is None:
                continue
            assigned.append({
                "id":      o.get("id"),
                "name":    name,
                "ee_id":   ee["id"],
                "ee_name": ee["name"],
            })

        log.info("Organizations: %d total, %d excluded, %d with EE assigned", total, excluded, len(assigned))
        db.set_cached(host_key, self.RESOURCE, {"assigned": assigned, "total": total})
        _, fetched_at = db.get_cached(host_key, self.RESOURCE)
        return assigned, total, fetched_at.isoformat()


class CredentialService:
    RESOURCE = "credentials"
    ENDPOINT = "/api/v2/credentials/"

    def get_all(self, client: AAPClient, host_key: str, force: bool = False, serve_stale: bool = False) -> tuple[list[dict], str]:
        if not force:
            cached, fetched_at = db.get_cached(host_key, self.RESOURCE, ignore_ttl=serve_stale)
            if cached is not None:
                return cached, fetched_at.isoformat()
            return [], None  # cache cold, not forcing — do not hit AAP

        print(f"[AAP FETCH] {self.RESOURCE} host={host_key} force={force}", flush=True)
        raw    = client.get_all(self.ENDPOINT)
        shaped = [self._shape(c) for c in raw]
        log.info("Credentials: %d total", len(shaped))
        db.set_cached(host_key, self.RESOURCE, shaped)
        _, fetched_at = db.get_cached(host_key, self.RESOURCE)
        return shaped, fetched_at.isoformat()

    @staticmethod
    def _shape(c: dict) -> dict:
        sf       = c.get("summary_fields", {})
        sf_org   = sf.get("organization", {})
        sf_ctype = sf.get("credential_type", {})

        # org — None means it's a global/default credential not tied to any org
        org_id   = sf_org.get("id")
        org_name = sf_org.get("name", "") if sf_org else ""

        # credential type name and kind
        ctype_name = sf_ctype.get("name", "") or c.get("kind", "")
        ctype_id   = sf_ctype.get("id")

        # username — not all credential types have one
        username = c.get("inputs", {}).get("username", "") if isinstance(c.get("inputs"), dict) else ""

        # usage counts from related counts if available
        related_counts = sf.get("related_counts", {}) or {}
        usage_count    = (
            related_counts.get("job_templates", 0) +
            related_counts.get("workflows", 0)
        )

        return {
            "id":          c.get("id"),
            "name":        c.get("name", ""),
            "description": c.get("description", ""),
            "ctype_id":    ctype_id,
            "ctype_name":  ctype_name,
            "username":    username,
            "org_id":      org_id,
            "org_name":    org_name,
            "has_org":     org_id is not None,
            "usage_count": usage_count,
        }

# Stateless singletons
ee_service       = ExecutionEnvironmentService()
project_service  = ProjectService()
template_service = JobTemplateService()
org_service      = OrganizationService()
cred_service     = CredentialService()
