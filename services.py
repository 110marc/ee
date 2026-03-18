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


class _ProjectsFullService:
    """Fetches all projects with created_by + scm_url for the Projects page."""
    RESOURCE = "projects_full"
    ENDPOINT = "/api/v2/projects/"

    def get_all(self, client, host_key: str, force: bool = False, serve_stale: bool = False):
        if not force:
            cached, fetched_at = db.get_cached(host_key, self.RESOURCE, ignore_ttl=serve_stale)
            if cached is not None:
                return cached, fetched_at.isoformat()
            return [], None

        raw    = client.get_all(self.ENDPOINT)
        shaped = [self._shape(p) for p in raw]
        db.set_cached(host_key, self.RESOURCE, shaped)
        _, fetched_at = db.get_cached(host_key, self.RESOURCE)
        return shaped, fetched_at.isoformat()

    @staticmethod
    def _shape(p: dict) -> dict:
        sf         = p.get("summary_fields", {})
        sf_org     = sf.get("organization", {})
        sf_ee      = sf.get("default_environment", {})
        sf_created = sf.get("created_by", {})
        return {
            "id":          p.get("id"),
            "name":        p.get("name", ""),
            "description": p.get("description", ""),
            "scm_type":    p.get("scm_type", ""),
            "scm_url":     p.get("scm_url", ""),
            "scm_branch":  p.get("scm_branch", ""),
            "status":      p.get("status", ""),
            "org_id":      sf_org.get("id"),
            "org_name":    sf_org.get("name", ""),
            "ee_id":       sf_ee.get("id"),
            "ee_name":     sf_ee.get("name", ""),
            "created_by":  sf_created.get("username", ""),
        }


projects_full_service = _ProjectsFullService()


class JobTemplateService:
    RESOURCE = "job_templates"
    ENDPOINT = "/api/v2/job_templates/"

    def get_with_ee(self, client: AAPClient, host_key: str, force: bool = False, serve_stale: bool = False) -> tuple[list[dict], int, str]:
        if not force:
            cached, fetched_at = db.get_cached(host_key, self.RESOURCE, ignore_ttl=serve_stale)
            if cached is not None:
                return cached["assigned"], cached["total"], fetched_at.isoformat()
            return [], 0, None  # cache cold, not forcing — do not hit AAP

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


# ── LDAP service ───────────────────────────────────────────────
# Setting categories for friendly display
_LDAP_CATEGORIES = {
    "Connection": [
        "SERVER_URI", "START_TLS", "CONNECTION_OPTIONS",
    ],
    "Bind": [
        "BIND_DN", "BIND_PASSWORD",
    ],
    "User Search": [
        "USER_SEARCH", "USER_DN_TEMPLATE", "USER_ATTR_MAP",
        "USER_FLAGS_BY_GROUP",
    ],
    "Group Search": [
        "GROUP_SEARCH", "GROUP_TYPE", "GROUP_TYPE_PARAMS",
        "REQUIRE_GROUP", "DENY_GROUP",
    ],
    "Sync": [
        "ALWAYS_UPDATE_USER", "MIRROR_GROUPS",
        "ORGANIZATION_MAP", "TEAM_MAP",
    ],
}

# Keys to always hide (sensitive or noise)
_LDAP_HIDDEN = {"BIND_PASSWORD"}

# Keys that contain org/team mapping (rendered separately)
_LDAP_MAP_KEYS = {"ORGANIZATION_MAP", "TEAM_MAP"}


class _LdapService:
    RESOURCE = "ldap_settings"

    def get_all(self, client, host_key: str, force: bool = False, serve_stale: bool = False):
        """
        Fetches /api/v2/settings/ldap/ — single endpoint with all AUTH_LDAP_* keys.
        Shapes into categorised settings + org/team maps.
        """
        if not force:
            cached, fetched_at = db.get_cached(host_key, self.RESOURCE, ignore_ttl=serve_stale)
            if cached is not None:
                return cached, fetched_at.isoformat()
            return {}, None  # cache cold

        raw    = client.get("/api/v2/settings/ldap/")
        result = self._shape(raw)
        db.set_cached(host_key, self.RESOURCE, result)
        _, fetched_at = db.get_cached(host_key, self.RESOURCE)
        return result, fetched_at.isoformat()

    @staticmethod
    def _shape(raw: dict) -> dict:
        """
        Groups all AUTH_LDAP_* keys into categories.
        Pulls org_map and team_map out separately for the gap-analysis table.
        Strips the AUTH_LDAP_ prefix for display.
        """
        prefix   = "AUTH_LDAP_"
        org_map  = raw.get(f"{prefix}ORGANIZATION_MAP", {}) or {}
        team_map = raw.get(f"{prefix}TEAM_MAP", {}) or {}

        # Build category buckets
        categories = {}
        seen = set()
        for cat, keys in _LDAP_CATEGORIES.items():
            rows = []
            for key in keys:
                full_key = f"{prefix}{key}"
                val = raw.get(full_key)
                seen.add(full_key)
                if key in _LDAP_HIDDEN:
                    rows.append({"key": key, "value": "••••••••", "hidden": True})
                elif key in _LDAP_MAP_KEYS:
                    continue   # rendered separately
                elif val is not None and val != "" and val != [] and val != {}:
                    rows.append({"key": key, "value": val, "hidden": False})
                else:
                    rows.append({"key": key, "value": None, "hidden": False})
            if rows:
                categories[cat] = rows

        # Catch any AUTH_LDAP_* keys NOT in our categories (future-proof)
        extras = []
        for full_key, val in raw.items():
            if full_key.startswith(prefix) and full_key not in seen:
                key = full_key[len(prefix):]
                if key not in _LDAP_MAP_KEYS and key not in _LDAP_HIDDEN:
                    if val is not None and val != "" and val != [] and val != {}:
                        extras.append({"key": key, "value": val, "hidden": False})
        if extras:
            categories["Other"] = extras

        return {
            "categories": categories,
            "org_map":    org_map,
            "team_map":   team_map,
            "org_count":  len(org_map),
            "team_count": len(team_map),
            "configured": bool(raw.get(f"{prefix}SERVER_URI")),
        }


ldap_service = _LdapService()


# ── SSL Certificate service ────────────────────────────────────
import ssl as _ssl
import socket as _socket
from datetime import datetime as _dt, timezone as _tz
from cryptography import x509 as _x509
from cryptography.hazmat.backends import default_backend as _default_backend
from cryptography.x509.oid import NameOID as _NameOID, ExtensionOID as _ExtOID

class _SSLService:
    """
    Checks TLS certs by opening a raw SSL socket to host:443.
    Uses binary_form=True to get the DER cert bytes regardless of
    verify_mode, then parses with the cryptography library.
    This correctly handles self-signed and internal CA certs.
    """
    TIMEOUT = 10

    def get_all(self, host_configs: list[dict]) -> list[dict]:
        return [self._check(hc) for hc in host_configs]

    def _check(self, hc: dict) -> dict:
        url      = hc.get("url", "")
        hostname = url.replace("https://", "").replace("http://", "").split("/")[0].split(":")[0]

        base = {
            "host_key":   hc["host_key"],
            "host_label": hc["label"],
            "env_key":    hc["env_key"],
            "env_label":  hc["env_label"],
            "url":        url,
            "hostname":   hostname,
        }

        try:
            ctx = _ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode    = _ssl.CERT_NONE   # accept self-signed / expired certs

            with _socket.create_connection((hostname, 443), timeout=self.TIMEOUT) as sock:
                with ctx.wrap_socket(sock, server_hostname=hostname) as ssock:
                    # binary_form=True always returns DER bytes even with CERT_NONE
                    der = ssock.getpeercert(binary_form=True)

            # Parse with cryptography — works on any cert regardless of trust
            cert    = _x509.load_der_x509_certificate(der, _default_backend())
            now     = _dt.now(_tz.utc)
            expires = cert.not_valid_after_utc
            issued  = cert.not_valid_before_utc
            days    = (expires - now).days

            # CN
            cn_attrs = cert.subject.get_attributes_for_oid(_NameOID.COMMON_NAME)
            cn       = cn_attrs[0].value if cn_attrs else ""

            # SANs
            try:
                san_ext  = cert.extensions.get_extension_for_oid(_ExtOID.SUBJECT_ALTERNATIVE_NAME)
                sans     = san_ext.value.get_values_for_type(_x509.DNSName)
            except Exception:
                sans = []

            # Issuer
            i_cn_attrs  = cert.issuer.get_attributes_for_oid(_NameOID.COMMON_NAME)
            i_org_attrs = cert.issuer.get_attributes_for_oid(_NameOID.ORGANIZATION_NAME)
            issuer_cn   = i_cn_attrs[0].value  if i_cn_attrs  else ""
            issuer_org  = i_org_attrs[0].value if i_org_attrs else ""

            # Status thresholds
            if days < 0:
                status = "expired"
            elif days < 30:
                status = "critical"
            elif days < 90:
                status = "warning"
            else:
                status = "ok"

            return {
                **base,
                "ok":        True,
                "status":    status,
                "days_left": days,
                "expires":   expires.strftime("%b %d %H:%M:%S %Y GMT"),
                "issued":    issued.strftime("%b %d %H:%M:%S %Y GMT"),
                "cn":        cn,
                "sans":      list(sans),
                "issuer_cn": issuer_cn,
                "issuer_org":issuer_org,
                "serial":    str(cert.serial_number),
            }

        except _socket.timeout:
            return {**base, "ok": False, "status": "error", "error": "Connection timed out"}
        except ConnectionRefusedError:
            return {**base, "ok": False, "status": "error", "error": "Connection refused"}
        except Exception as e:
            return {**base, "ok": False, "status": "error", "error": str(e)}


ssl_service = _SSLService()
