"""
Drop-in replacement entry point for Replit deployment.
Runs the yahoo-finance-mcp server over Streamable HTTP transport
so Replit can expose it as a remote MCP endpoint.

Connect from Claude Desktop (claude_desktop_config.json):
  {
    "mcpServers": {
      "yahoo-finance": {
        "url": "https://<your-replit>.repl.co/mcp"
      }
    }
  }

Connect from Claude.ai remote MCP:
  Add the URL https://<your-replit>.repl.co/mcp in Settings → Integrations.
"""

import os

import uvicorn

from server import yfinance_server

PORT = int(os.environ.get("PORT", 8080))

# Disable DNS rebinding protection so the server is reachable from Claude.ai
# on a public Replit URL (the default FastMCP setting restricts to localhost).
yfinance_server.settings.transport_security = None

app = yfinance_server.streamable_http_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
