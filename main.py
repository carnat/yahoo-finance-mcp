"""
Drop-in replacement entry point for Replit deployment.
Runs the yahoo-finance-mcp server over HTTP (SSE transport)
so Replit can expose it as a remote MCP endpoint.
"""

import os
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.requests import Request
import uvicorn

# Import the mcp server object from the original server.py
# Make sure server.py is in the same directory
from server import mcp

PORT = int(os.environ.get("PORT", 8080))

sse = SseServerTransport("/messages/")

async def handle_sse(request: Request):
    async with sse.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await mcp.run(
            streams[0], streams[1], mcp.create_initialization_options()
        )

app = Starlette(
    routes=[
        Route("/sse", endpoint=handle_sse),
        Mount("/messages/", app=sse.handle_post_message),
    ]
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
