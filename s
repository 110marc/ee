"""
routes.py
Flask route definitions.

?host=<key>    selects the AAP host (default: config.DEFAULT_HOST)
?force=true    bypasses cache and fetches fresh data from AAP
"""

from __future__ import annotations

from flask import Blueprint, jsonify, request
from aap_client import get_client, list_environments
from services import ee_service, project_service, template_service, org_service, cred_service
import database as db
import database as db
import refresher
import config

ee_bp        = Blueprint("execution_environments", __name__)
projects_bp  = Blueprint("projects",               __name__)
templates_bp = Blueprint("job_templates",          __name__)
summary_bp   = Blueprint("summary",                __name__)
hosts_bp     = Blueprint("hosts",                  __name__)
cache_bp     = Blueprint("cache",                  __name__)
refresh_bp   = Blueprint("refresh",                __name__)
orgs_bp      = Blueprint("organizations",           __name__)
creds_bp     = Blueprint("credentials",              __name__)


def _resolve_client():
    host_key = request.args.get("host") or config.DEFAULT_HOST
    return get_client(host_key), host_key


def _force() -> bool:
    return request.args.get("force", "").lower() in ("true", "1", "yes")


# ── /api/hosts ────────────────────────────────────────────────

# ── /api/health ───────────────────────────────────────────────
@hosts_bp.route("/api/health")
def health():
    """Used by the portal to check if this module is reachable."""
    return jsonify({"success": True, "service": "ee_dashboard"})

@hosts_bp.route("/api/hosts")
def get_hosts():
    """Returns the full environment -> hosts tree (no credentials)."""
    try:
        return jsonify({
            "success":      True,
            "default_env":  config.DEFAULT_ENV,
            "default_host": config.DEFAULT_HOST,
            "environments": list_environments(),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "environments": []}), 500


# ── /api/refresh  POST = start, GET status ────────────────────
@refresh_bp.route("/api/refresh", methods=["POST"])
def start_refresh():
    """
    Starts a background AAP refresh for the given host.
    ?scope=ee          — refresh EE resources only (ees, orgs, projects, templates)
    ?scope=credentials — refresh credentials only
    ?scope=all         — refresh everything (default)
    Returns immediately — poll /api/refresh/status for completion.
    """
    host_key = request.args.get("host")  or config.DEFAULT_HOST
    scope    = request.args.get("scope", "all").lower()
    force    = request.args.get("force", "true").lower() in ("true", "1", "yes")
    if scope not in ("ee", "credentials", "all"):
        scope = "all"
    print(f"[ROUTE /api/refresh] full_url={request.url} host={host_key} scope={scope} force={force}", flush=True)
    try:
        get_client(host_key)  # validate host exists
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400

    started = refresher.start_refresh(host_key, force=force, scope=scope)
    return jsonify({
        "success":  True,
        "host_key": host_key,
        "scope":    scope,
        "started":  started,
        "status":   "refreshing" if started else "already_refreshing",
    })


@refresh_bp.route("/api/refresh/status")
def refresh_status():
    """Returns the current background refresh state for a host+scope."""
    host_key = request.args.get("host")  or config.DEFAULT_HOST
    scope    = request.args.get("scope", "all").lower()
    status   = refresher.get_status(host_key, scope=scope)
    return jsonify({"success": True, "host_key": host_key, "scope": scope, **status})


# ── /api/cache ────────────────────────────────────────────────
@cache_bp.route("/api/cache", methods=["GET"])
def get_cache_status():
    return jsonify({
        "success":     True,
        "ttl_minutes": config.CACHE_TTL_MINUTES,
        "db_path":     config.DB_PATH,
        "entries":     db.cache_status(),
    })


@cache_bp.route("/api/cache", methods=["DELETE"])
def clear_cache():
    host_key = request.args.get("host")
    resource = request.args.get("resource")
    if host_key:
        db.invalidate(host_key, resource)
        msg = f"Cache cleared for host={host_key}" + (f" resource={resource}" if resource else "")
    else:
        db.invalidate_all()
        msg = "Entire cache cleared"
    return jsonify({"success": True, "message": msg})




# ── /api/credentials ──────────────────────────────────────────
@creds_bp.route("/api/credentials")
def get_credentials():
    try:
        client, host_key        = _resolve_client()
        creds, synced_at        = cred_service.get_all(client, host_key, force=_force(), serve_stale=True)
        org_creds     = [c for c in creds if c["has_org"]]
        global_creds  = [c for c in creds if not c["has_org"]]
        return jsonify({
            "success":    True,
            "host":       client.label,
            "synced_at":  synced_at,
            "count":      len(creds),
            "org_count":  len(org_creds),
            "global_count": len(global_creds),
            "results":    creds,
        })
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ── /api/organizations ────────────────────────────────────────
@orgs_bp.route("/api/organizations")
def get_organizations():
    try:
        client, host_key  = _resolve_client()
        orgs, total_o, synced_at = org_service.get_with_ee(client, host_key, force=_force(), serve_stale=True)
        return jsonify({
            "success": True, "host": client.label, "env": client.env_label,
            "synced_at": synced_at, "count": len(orgs), "total": total_o, "results": orgs,
        })
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ── /api/execution_environments ───────────────────────────────
@ee_bp.route("/api/execution_environments")
def get_execution_environments():
    try:
        client, host_key = _resolve_client()
        ees, synced_at   = ee_service.get_all(client, host_key, force=_force())
        return jsonify({
            "success": True, "host": client.label, "env": client.env_label,
            "synced_at": synced_at, "count": len(ees), "results": ees,
        })
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── /api/projects ─────────────────────────────────────────────
@projects_bp.route("/api/projects")
def get_projects():
    try:
        client, host_key           = _resolve_client()
        projects, total, synced_at = project_service.get_with_ee(client, host_key, force=_force())
        return jsonify({
            "success": True, "host": client.label, "env": client.env_label,
            "synced_at": synced_at, "total": total,
            "count": len(projects), "results": projects,
        })
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── /api/job_templates ────────────────────────────────────────
@templates_bp.route("/api/job_templates")
def get_job_templates():
    try:
        client, host_key            = _resolve_client()
        templates, total, synced_at = template_service.get_with_ee(client, host_key, force=_force())
        return jsonify({
            "success": True, "host": client.label, "env": client.env_label,
            "synced_at": synced_at, "total": total,
            "count": len(templates), "results": templates,
        })
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── /api/summary ──────────────────────────────────────────────
@summary_bp.route("/api/summary")
def get_summary():
    """
    Serves data from DB cache only — never hits AAP directly.
    Returns empty arrays if nothing is cached yet.
    Pass ?force=true only via the refresher, not the UI summary call.
    """
    try:
        client, host_key = _resolve_client()

        # Read from cache only — ignore_ttl=True so stale data is still shown.
        # If nothing cached yet, return empty immediately (no AAP call).
        ees,       synced_ees   = db.get_cached(host_key, "execution_environments", ignore_ttl=True)
        orgs_raw,  synced_orgs  = db.get_cached(host_key, "organizations",          ignore_ttl=True)
        proj_raw,  synced_p     = db.get_cached(host_key, "projects",               ignore_ttl=True)
        tmpl_raw,  synced_t     = db.get_cached(host_key, "job_templates",          ignore_ttl=True)
        creds_data, synced_creds = db.get_cached(host_key, "credentials",            ignore_ttl=True)

        ees       = ees if isinstance(ees, list) else []
        orgs_raw  = orgs_raw  or {"assigned": [], "total": 0}
        proj_raw  = proj_raw  or {"assigned": [], "total": 0}
        tmpl_raw  = tmpl_raw  or {"assigned": [], "total": 0}

        orgs      = orgs_raw["assigned"]
        total_o   = orgs_raw["total"]
        projects  = proj_raw["assigned"]
        total_p   = proj_raw["total"]
        templates  = tmpl_raw["assigned"]
        total_t    = tmpl_raw["total"]
        creds      = creds_data if isinstance(creds_data, list) else []
        org_creds  = [c for c in creds if c.get("has_org")]
        global_creds = [c for c in creds if not c.get("has_org")]

        # Use the oldest sync time across all resources
        # synced_* are datetime objects or None from db.get_cached
        sync_times = [s for s in [synced_ees, synced_orgs, synced_p, synced_t, synced_creds] if s is not None]
        synced_at  = min(sync_times).isoformat() if sync_times else None

        return jsonify({
            "success":   True,
            "host":      client.label,
            "host_key":  client.host_key,
            "env":       client.env_label,
            "env_key":   client.env_key,
            "synced_at": synced_at,
            "cached":    True,
            "refreshing": refresher.is_refreshing(host_key),
            "ees":        ees,
            "orgs":       orgs,
            "projects":   projects,
            "templates":  templates,
            "credentials": creds,
            "stats": {
                "total_ees":         len(ees),
                "total_orgs":        total_o,
                "orgs_with_ee":      len(orgs),
                "total_projects":    total_p,
                "projects_with_ee":  len(projects),
                "total_templates":   total_t,
                "templates_with_ee": len(templates),
                "total_credentials": len(creds),
                "org_credentials":   len(org_creds),
                "global_credentials": len(global_creds),
            },
        })
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── /api/summary/env ──────────────────────────────────────────
@summary_bp.route("/api/summary/env")
def get_env_summary():
    """
    Aggregates cached data across ALL hosts in an environment.
    No AAP calls — reads from DB only.
    """
    try:
        env_key = request.args.get("env") or config.DEFAULT_ENV
        env     = config.ENVIRONMENTS.get(env_key)
        if not env:
            return jsonify({"success": False, "error": f"Unknown environment: {env_key}"}), 400

        totals = {
            "total_ees":         0,
            "total_orgs":        0,
            "orgs_with_ee":      0,
            "total_projects":    0,
            "projects_with_ee":  0,
            "total_templates":   0,
            "templates_with_ee": 0,
            "total_credentials": 0,
            "org_credentials":   0,
            "global_credentials":0,
        }
        host_summaries = []
        oldest_sync    = None

        for host_key, host_cfg in env["hosts"].items():
            # Read from DB cache only — never hit AAP
            ees_data,   synced_ees  = db.get_cached(host_key, "execution_environments", ignore_ttl=True)
            orgs_data,  synced_orgs = db.get_cached(host_key, "organizations",          ignore_ttl=True)
            proj_data,  _           = db.get_cached(host_key, "projects",               ignore_ttl=True)
            tmpl_data,  _           = db.get_cached(host_key, "job_templates",          ignore_ttl=True)
            creds_data, _           = db.get_cached(host_key, "credentials",            ignore_ttl=True)

            ees       = ees_data  if isinstance(ees_data, list)  else []
            orgs_raw  = orgs_data if isinstance(orgs_data, dict) else {"assigned": [], "total": 0}
            proj_raw  = proj_data if isinstance(proj_data, dict) else {"assigned": [], "total": 0}
            tmpl_raw  = tmpl_data if isinstance(tmpl_data, dict) else {"assigned": [], "total": 0}

            orgs      = orgs_raw["assigned"]
            total_o   = orgs_raw["total"]
            projects  = proj_raw["assigned"]
            total_p   = proj_raw["total"]
            templates = tmpl_raw["assigned"]
            total_t   = tmpl_raw["total"]
            creds     = creds_data if isinstance(creds_data, list) else []
            org_creds    = [c for c in creds if c.get("has_org")]
            global_creds = [c for c in creds if not c.get("has_org")]

            has_data = bool(ees or orgs or projects or templates or creds)

            totals["total_ees"]          += len(ees)
            totals["total_orgs"]         += total_o
            totals["orgs_with_ee"]       += len(orgs)
            totals["total_projects"]     += total_p
            totals["projects_with_ee"]   += len(projects)
            totals["total_templates"]    += total_t
            totals["templates_with_ee"]  += len(templates)
            totals["total_credentials"]  += len(creds)
            totals["org_credentials"]    += len(org_creds)
            totals["global_credentials"] += len(global_creds)

            if synced_ees and (oldest_sync is None or synced_ees < oldest_sync):
                oldest_sync = synced_ees

            host_summaries.append({
                "host_key": host_key,
                "label":    host_cfg.get("label", host_key),
                "has_data": has_data,
                "stats": {
                    "total_ees":          len(ees),
                    "total_orgs":         total_o,
                    "orgs_with_ee":       len(orgs),
                    "total_projects":     total_p,
                    "projects_with_ee":   len(projects),
                    "total_templates":    total_t,
                    "templates_with_ee":  len(templates),
                    "total_credentials":  len(creds),
                    "org_credentials":    len(org_creds),
                    "global_credentials": len(global_creds),
                },
            })

        return jsonify({
            "success":   True,
            "env_key":   env_key,
            "env_label": env["label"],
            "synced_at": oldest_sync.isoformat() if oldest_sync else None,
            "totals":    totals,
            "hosts":     host_summaries,
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ── /api/debug  (temporary — remove in production) ───────────
debug_bp = Blueprint("debug", __name__)

@debug_bp.route("/api/debug/projects")
def debug_projects():
    """
    Returns raw execution_environment field from first 10 projects.
    Use this to see exactly what AAP is returning.
    """
    try:
        client, host_key = _resolve_client()
        raw = client.get_all("/api/v2/projects/")
        sample = []
        for p in raw[:10]:
            ee = p.get("execution_environment")
            sample.append({
                "id":     p.get("id"),
                "name":   p.get("name"),
                "ee_raw": ee,
                "ee_type": type(ee).__name__,
            })
        return jsonify({"success": True, "count": len(raw), "sample": sample})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@debug_bp.route("/api/debug/templates")
def debug_templates():
    """
    Returns raw execution_environment field from first 10 job templates.
    """
    try:
        client, host_key = _resolve_client()
        raw = client.get_all("/api/v2/job_templates/")
        sample = []
        for t in raw[:10]:
            ee = t.get("execution_environment")
            sample.append({
                "id":     t.get("id"),
                "name":   t.get("name"),
                "ee_raw": ee,
                "ee_type": type(ee).__name__,
            })
        return jsonify({"success": True, "count": len(raw), "sample": sample})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
