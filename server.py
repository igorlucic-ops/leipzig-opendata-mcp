from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import PlainTextResponse

INSTRUCTION_STRING = """
Leipzig Open Data MCP — City of Leipzig open data portal (opendata.leipzig.de)

USE THIS WHEN / DO NOT USE WHEN decision tree:

FINDING DATASETS:
→ USE search_datasets when: user asks about topics, needs to find what data exists,
  wants datasets by category/format/organization/tag.
→ USE get_dataset when: you have a specific dataset name or ID.
→ DO NOT guess dataset IDs — always search first if unknown.

EXPLORING CATALOG STRUCTURE:
→ USE list_organizations to discover city departments that publish data.
→ USE list_groups to discover thematic categories (Bevölkerung, Verkehr, Umwelt, etc.).
→ USE list_tags to discover keyword vocabulary.
→ Chain these: list_groups → search_datasets(fq="groups:<slug>") → get_dataset → list_resources.

ACCESSING DATA:
→ USE list_resources to see all files/services for a dataset with clear access notes.
→ USE query_datastore ONLY when resource has datastore_active=true.
→ DO NOT use query_datastore on WFS/WMS/GTFS/geo files — they are not DataStore-backed.
→ When datastore_active=false: return the download URL to the user.

IMPORTANT FACTS ABOUT THIS PORTAL:
- All metadata is in German. Do not translate dataset names/titles in tool calls.
- ~250+ datasets from Leipzig city departments.
- Categories use short slugs: soci=Bevölkerung, tran=Verkehr, envi=Umwelt, gove=Regierung.
- Many geodatasets expose WFS/WMS service URLs, not downloadable files.
- Not all CSV resources are DataStore-backed — always check datastore_active.

Data source: https://opendata.leipzig.de | License: Various (see per-dataset) | Language: German
"""

mcp = FastMCP(
    name="Leipzig Open Data",
    instructions=INSTRUCTION_STRING,
    version="1.0.0",
    website_url="https://opendata.leipzig.de",
)

from tools_leipzig import (
    search_datasets,
    get_dataset,
    list_resources,
    query_datastore,
    list_organizations,
    list_groups,
    list_tags,
)

mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True})(search_datasets)
mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True})(get_dataset)
mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True})(list_resources)
mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": True})(query_datastore)
mcp.tool(annotations={"readOnlyHint": True})(list_organizations)
mcp.tool(annotations={"readOnlyHint": True})(list_groups)
mcp.tool(annotations={"readOnlyHint": True})(list_tags)

# Health endpoint
@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")

# Build the MCP Starlette app with streamable-http
_inner_app = mcp.http_app(transport="streamable-http")


# Pure ASGI wrapper that rewrites /mcp -> /mcp/ to avoid 307 redirect
class SlashRewriteWrapper:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"] == "/mcp":
            scope = dict(scope)
            scope["path"] = "/mcp/"
            scope["raw_path"] = b"/mcp/"
        await self.app(scope, receive, send)


app = SlashRewriteWrapper(_inner_app)
