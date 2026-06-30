# Local Python MCP runtime image.
#
# Production deployment for this repository is the Cloudflare Worker under
# worker/. This image is for stdio/local Python MCP compatibility only.
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./
COPY main.py server.py tool_catalog.json tool_groups.py ./
COPY yfmcp ./yfmcp

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -e .

CMD ["python", "server.py"]
