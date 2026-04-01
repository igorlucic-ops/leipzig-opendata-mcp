"""
Tool implementations for Leipzig Open Data MCP Server.
"""
from __future__ import annotations

import logging

from leipzig_client import (
    ckan_get,
    normalize_dataset,
    normalize_dataset_summary,
    normalize_resource,
    is_valid_uuid,
    BASE_URL,
)

logger = logging.getLogger("leipzig-mcp")


async def search_datasets(
    q: str = "",
    fq: str = "",
    rows: int = 20,
    start: int = 0,
    sort: str = "score desc",
    facets: bool = False,
) -> dict:
    """
    Search Leipzig open data catalog by keyword, category, format, or tag.

    USE THIS WHEN:
    - User wants to find datasets about a topic (e.g. "Verkehr", "Einwohner")
    - User wants datasets by category, organization, format, or tag
    - User asks "what data is available about X?"

    THEN CALL:
    → Found results? → call get_dataset with the dataset name/slug
    → No results? → try broader terms, or call list_groups / list_tags to explore

    DO NOT USE WHEN:
    - You already have a specific dataset ID or slug → use get_dataset directly

    Parameters:
    - q: Free-text query (German or English). Empty returns all.
    - fq: CKAN filter query e.g. "organization:amt-fuer-umweltschutz"
           or "groups:soci" or "res_format:CSV" or "tags:Verkehr"
    - rows: Max results (hard cap: 20)
    - start: Offset for pagination
    - sort: "score desc", "metadata_modified desc", or "name asc"
    - facets: If True, include facet counts
    """
    try:
        rows = min(rows, 20)
        params: dict = {
            "q": q,
            "rows": rows,
            "start": start,
            "sort": sort,
        }
        if fq:
            params["fq"] = fq
        if facets:
            params["facet.field"] = '["tags","res_format","organization","groups"]'
            params["facet"] = "true"

        logger.info("search_datasets q=%r fq=%r rows=%d start=%d", q, fq, rows, start)
        result = await ckan_get("package_search", params)

        datasets = [normalize_dataset_summary(d) for d in (result.get("results") or [])]
        total = result.get("count", 0)

        response: dict = {
            "total_count": total,
            "returned": len(datasets),
            "start": start,
            "datasets": datasets,
            "facets": result.get("search_facets", {}) if facets else {},
            "search_note": None,
        }

        # Fallback: if multi-word query returns 0, retry with first word
        if total == 0 and q and " " in q.strip():
            first_word = q.strip().split()[0]
            params["q"] = first_word
            logger.info("Retrying search with first word: %r", first_word)
            result2 = await ckan_get("package_search", params)
            datasets2 = [normalize_dataset_summary(d) for d in (result2.get("results") or [])]
            total2 = result2.get("count", 0)
            response = {
                "total_count": total2,
                "returned": len(datasets2),
                "start": start,
                "datasets": datasets2,
                "facets": result2.get("search_facets", {}) if facets else {},
                "search_note": f"No results for '{q}' — showing results for '{first_word}'",
            }

        return response
    except Exception as e:
        logger.warning("search_datasets error: %s", e)
        return {"error": str(e), "total_count": 0, "datasets": []}


async def get_dataset(dataset_id: str) -> dict:
    """
    Fetch full metadata for a single Leipzig dataset by slug or UUID.

    USE THIS WHEN:
    - You have a specific dataset name or ID from search results
    - You want full description, resources, license info

    DO NOT USE WHEN:
    - You don't know the dataset ID → use search_datasets first

    Parameters:
    - dataset_id: Dataset slug (e.g. "wochenmaerkte") or UUID
    """
    try:
        logger.info("get_dataset id=%r", dataset_id)
        result = await ckan_get("package_show", {"id": dataset_id})
        return normalize_dataset(result)
    except Exception as e:
        msg = str(e)
        if "Not Found" in msg or "not found" in msg.lower():
            return {"error": "Dataset not found", "id": dataset_id}
        logger.warning("get_dataset error: %s", e)
        return {"error": msg, "id": dataset_id}


async def list_resources(dataset_id: str) -> dict:
    """
    List all resources for a dataset with formats, URLs, and access notes.

    USE THIS WHEN:
    - You want a clean summary of files/services for a dataset
    - You need to decide which resource to query or download

    DO NOT USE WHEN:
    - You already called get_dataset — resources are included there

    Parameters:
    - dataset_id: Dataset slug or UUID
    """
    try:
        logger.info("list_resources id=%r", dataset_id)
        result = await ckan_get("package_show", {"id": dataset_id})
        resources = [normalize_resource(r) for r in (result.get("resources") or [])]
        # Add size_bytes alias
        for res in resources:
            res["size_bytes"] = res.pop("size", None)
            # Remove fields not needed in this view
            res.pop("mimetype", None)
            res.pop("last_modified", None)

        return {
            "dataset_name": result.get("name", ""),
            "dataset_title": result.get("title", ""),
            "resource_count": len(resources),
            "resources": resources,
        }
    except Exception as e:
        msg = str(e)
        if "Not Found" in msg or "not found" in msg.lower():
            return {"error": "Dataset not found", "id": dataset_id}
        logger.warning("list_resources error: %s", e)
        return {"error": msg, "id": dataset_id}


async def query_datastore(
    resource_id: str,
    limit: int = 20,
    offset: int = 0,
    filters: dict | None = None,
    q: str = "",
    fields: list[str] | None = None,
    sort: str = "",
) -> dict:
    """
    Query a DataStore-backed resource for tabular data rows.

    USE THIS WHEN:
    - A resource has datastore_active=true AND you want to read/filter rows

    DO NOT USE WHEN:
    - datastore_active is false or unknown
    - Resource is WFS/WMS/GTFS/CityGML/image — these are NOT DataStore-backed

    Parameters:
    - resource_id: Resource UUID (get from get_dataset or list_resources)
    - limit: Max rows (hard cap: 50)
    - offset: Row offset for pagination
    - filters: Dict of field=value filters e.g. {"Tage": "Mittwoch"}
    - q: Full-text search within the resource
    - fields: Specific fields to return (empty = all)
    - sort: Sort string e.g. "year desc"
    """
    try:
        if not is_valid_uuid(resource_id):
            return {
                "error": "Invalid resource_id format. Resource IDs are UUIDs. Get them from get_dataset or list_resources.",
                "resource_id": resource_id,
            }

        limit = min(limit, 50)
        params: dict = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
        }
        if filters:
            import json
            params["filters"] = json.dumps(filters)
        if q:
            params["q"] = q
        if fields:
            params["fields"] = ",".join(fields)
        if sort:
            params["sort"] = sort

        logger.info("query_datastore resource=%r limit=%d offset=%d", resource_id, limit, offset)
        result = await ckan_get("datastore_search", params)

        ds_url = f"{BASE_URL}/datastore_search?resource_id={resource_id}&limit={limit}&offset={offset}"

        return {
            "resource_id": resource_id,
            "total": result.get("total", 0),
            "returned": len(result.get("records", [])),
            "offset": offset,
            "fields": result.get("fields", []),
            "records": result.get("records", []),
            "datastore_url": ds_url,
        }
    except Exception as e:
        msg = str(e)
        if "not found" in msg.lower() or "Resource not found" in msg:
            return {
                "error": "Resource is not DataStore-backed. Download via URL instead.",
                "resource_id": resource_id,
            }
        logger.warning("query_datastore error: %s", e)
        return {"error": msg, "resource_id": resource_id}


async def list_organizations() -> dict:
    """
    List all publishing organizations on Leipzig's open data portal.

    USE THIS WHEN:
    - User asks who publishes data, wants to browse by city department
    - User needs an organization slug for search_datasets(fq="organization:<slug>")

    DO NOT USE WHEN:
    - You already know the organization slug
    """
    try:
        logger.info("list_organizations")
        result = await ckan_get("organization_list", {"all_fields": "true"})
        orgs = sorted(
            [
                {
                    "name": o.get("name", ""),
                    "title": o.get("title", o.get("display_name", "")),
                    "description": o.get("description", ""),
                    "package_count": o.get("package_count", 0),
                }
                for o in result
            ],
            key=lambda x: x["package_count"],
            reverse=True,
        )
        return {"count": len(orgs), "organizations": orgs}
    except Exception as e:
        logger.warning("list_organizations error: %s", e)
        return {"error": str(e)}


async def list_groups() -> dict:
    """
    List all thematic categories (CKAN groups) on the portal.

    USE THIS WHEN:
    - User wants to explore data by topic/category
    - User needs a group slug for search_datasets(fq="groups:<slug>")

    DO NOT USE WHEN:
    - You already know the group slug
    """
    try:
        logger.info("list_groups")
        result = await ckan_get("group_list", {"all_fields": "true"})
        groups = sorted(
            [
                {
                    "name": g.get("name", ""),
                    "title": g.get("title", g.get("display_name", "")),
                    "description": g.get("description", ""),
                    "package_count": g.get("package_count", 0),
                }
                for g in result
            ],
            key=lambda x: x["package_count"],
            reverse=True,
        )
        return {"count": len(groups), "groups": groups}
    except Exception as e:
        logger.warning("list_groups error: %s", e)
        return {"error": str(e)}


async def list_tags(query: str = "", limit: int = 50) -> dict:
    """
    List available tags on the portal, optionally filtered.

    USE THIS WHEN:
    - User wants to discover tag vocabulary for search refinement
    - User asks what tags/keywords are available

    DO NOT USE WHEN:
    - You already know the tag name

    Parameters:
    - query: Optional substring filter on tag names (German)
    - limit: Max tags to return
    """
    try:
        logger.info("list_tags query=%r", query)
        params: dict = {}
        if query:
            params["query"] = query
        result = await ckan_get("tag_list", params)
        tags = sorted(result) if isinstance(result, list) else []
        tags = tags[:limit]
        return {"count": len(tags), "tags": tags}
    except Exception as e:
        logger.warning("list_tags error: %s", e)
        return {"error": str(e)}
