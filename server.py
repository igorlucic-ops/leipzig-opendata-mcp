import os

from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.server.auth.providers.jwt import JWTVerifier
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import PlainTextResponse

load_dotenv()

from tools_leipzig import (
    search_datasets,
    get_dataset,
    list_resources,
    query_datastore,
    list_organizations,
    list_groups,
    list_tags,
)

####### API KEY #######

verifier = JWTVerifier(
    public_key=os.getenv("MCP_SERVER_JWT_SECRET"),
    issuer=os.getenv("MCP_SERVER_JWT_ISSUER", ""),
    audience=os.getenv("MCP_SERVER_JWT_AUDIENCE", ""),
    algorithm="HS256",
)

middleware = []

####### SERVER CONFIGURATION #######

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
    auth=verifier,
)

####### TOOLS — all with requires_permission: False #######

mcp.tool(meta={"requires_permission": False})(search_datasets)
mcp.tool(meta={"requires_permission": False})(get_dataset)
mcp.tool(meta={"requires_permission": False})(list_resources)
mcp.tool(meta={"requires_permission": False})(query_datastore)
mcp.tool(meta={"requires_permission": False})(list_organizations)
mcp.tool(meta={"requires_permission": False})(list_groups)
mcp.tool(meta={"requires_permission": False})(list_tags)

####### CUSTOM ROUTES #######

@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")

####### RUNNING THE SERVER #######

app = mcp.http_app(middleware=middleware)
