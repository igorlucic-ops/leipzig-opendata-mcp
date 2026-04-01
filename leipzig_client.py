"""
HTTP client for Leipzig Open Data Portal (CKAN API).
"""
from __future__ import annotations

import asyncio
import logging
import re

import httpx

logger = logging.getLogger("leipzig-mcp")

BASE_URL = "https://opendata.leipzig.de/api/3/action"
PORTAL_URL = "https://opendata.leipzig.de"
HEADERS = {
    "User-Agent": "leipzig-opendata-mcp/1.0 (Intric AI assistant; opendata@leipzig.de)",
    "Accept": "application/json",
}
TIMEOUT = 20.0

UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)

OGC_FORMATS = {"WFS", "WMS", "WMS_SRVC", "WFS_SRVC"}
IMAGE_FORMATS = {"JPG", "PNG", "JPEG"}


async def ckan_get(action: str, params: dict | None = None) -> dict:
    """Call a CKAN action endpoint. Returns parsed JSON result or raises."""
    url = f"{BASE_URL}/{action}"
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            async with httpx.AsyncClient(headers=HEADERS, timeout=TIMEOUT) as client:
                resp = await client.get(url, params=params or {})
                if resp.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"Server error {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                resp.raise_for_status()
                data = resp.json()
                if not data.get("success"):
                    raise ValueError(f"CKAN error: {data.get('error', {})}")
                return data.get("result", {})
        except (httpx.TimeoutException, httpx.HTTPStatusError) as exc:
            last_exc = exc
            if attempt == 0:
                if isinstance(exc, httpx.HTTPStatusError) and 400 <= exc.response.status_code < 500:
                    raise
                logger.warning("Retrying %s after error: %s", action, exc)
                await asyncio.sleep(2)
            else:
                raise
    raise last_exc  # type: ignore[misc]


def _access_note(resource: dict) -> str:
    fmt = (resource.get("format") or "").upper()
    if resource.get("datastore_active"):
        return "Query via API (DataStore)"
    if fmt in OGC_FORMATS:
        return "OGC service endpoint — use URL with GIS client"
    if fmt == "GTFS":
        return "GTFS archive — download and parse locally"
    if fmt in IMAGE_FORMATS:
        return "Image file — download via URL"
    return "Download via URL"


def normalize_resource(raw: dict) -> dict:
    ds_active = bool(raw.get("datastore_active", False))
    return {
        "id": raw.get("id", ""),
        "name": raw.get("name", ""),
        "description": raw.get("description", ""),
        "format": raw.get("format", ""),
        "url": raw.get("url", ""),
        "mimetype": raw.get("mimetype"),
        "size": raw.get("size"),
        "datastore_active": ds_active,
        "queryable": ds_active,
        "last_modified": raw.get("last_modified"),
        "access_note": _access_note(raw),
    }


def normalize_dataset(raw: dict) -> dict:
    name = raw.get("name", "")
    org_raw = raw.get("organization") or {}
    resources = [normalize_resource(r) for r in (raw.get("resources") or [])]
    tags = [t["display_name"] if isinstance(t, dict) else t for t in (raw.get("tags") or [])]
    groups = [
        {"name": g.get("name", ""), "title": g.get("title", g.get("display_name", ""))}
        for g in (raw.get("groups") or [])
    ]

    extras_raw = raw.get("extras") or []
    if isinstance(extras_raw, list):
        extras = {e["key"]: e["value"] for e in extras_raw if isinstance(e, dict) and "key" in e}
    elif isinstance(extras_raw, dict):
        extras = extras_raw
    else:
        extras = {}

    return {
        "id": raw.get("id", ""),
        "name": name,
        "title": raw.get("title", ""),
        "notes": raw.get("notes", ""),
        "organization": {
            "name": org_raw.get("name", ""),
            "title": org_raw.get("title", ""),
            "description": org_raw.get("description", ""),
        },
        "groups": groups,
        "tags": tags,
        "license_id": raw.get("license_id") or "",
        "license_title": raw.get("license_title") or "",
        "metadata_created": raw.get("metadata_created", ""),
        "metadata_modified": raw.get("metadata_modified", ""),
        "maintainer": raw.get("maintainer"),
        "maintainer_email": raw.get("maintainer_email"),
        "dataset_url": f"{PORTAL_URL}/dataset/{name}",
        "resources": resources,
        "extras": extras,
        "raw_ckan_url": f"{BASE_URL}/package_show?id={name}",
    }


def normalize_dataset_summary(raw: dict) -> dict:
    name = raw.get("name", "")
    org = raw.get("organization") or {}
    resources = raw.get("resources") or []
    formats = sorted({(r.get("format") or "").upper() for r in resources if r.get("format")})
    tags = [t["display_name"] if isinstance(t, dict) else t for t in (raw.get("tags") or [])]
    groups = [
        g.get("title", g.get("display_name", ""))
        for g in (raw.get("groups") or [])
    ]

    return {
        "id": raw.get("id", ""),
        "name": name,
        "title": raw.get("title", ""),
        "organization": org.get("title", ""),
        "groups": groups,
        "formats": formats,
        "tags": tags,
        "metadata_modified": raw.get("metadata_modified", ""),
        "dataset_url": f"{PORTAL_URL}/dataset/{name}",
    }


def is_valid_uuid(s: str) -> bool:
    return bool(UUID_RE.match(s))
