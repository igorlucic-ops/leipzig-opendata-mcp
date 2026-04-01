# Leipzig Open Data MCP Server

MCP server wrapping the City of Leipzig's open data portal (https://opendata.leipzig.de) as structured tools for AI assistants.

## Tools

| Tool | Purpose |
|------|---------|
| `search_datasets` | Full-text and filtered search across ~390 datasets |
| `get_dataset` | Full metadata for a single dataset by slug or UUID |
| `list_resources` | List files/services for a dataset with access notes |
| `query_datastore` | Query DataStore-backed tabular resources |
| `list_organizations` | List publishing city departments |
| `list_groups` | List thematic categories |
| `list_tags` | List keyword tags |

## Setup

```bash
python3.12 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python3 test_tools.py        # Must exit 0
uvicorn server:app --port 8000
curl http://localhost:8000/health  # → OK
```

## Deploy to Railway

```bash
git init && git add . && git commit -m "feat: Leipzig open data MCP server"
railway up
```

Then in Railway UI: Settings → Source → Connect Environment to Branch → main.

## Connect to Intric

URL: `https://<your-railway-domain>.railway.app/mcp`
No token needed (no-auth build).
