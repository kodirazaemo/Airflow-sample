"""
Alpha Vantage MCP client for Airflow tasks.

Connects using the official connection patterns from
https://mcp.alphavantage.co/#connection-examples :

- http (default): Streamable HTTP
  https://mcp.alphavantage.co/mcp?apikey=YOUR_API_KEY
- sse: legacy HTTP+SSE
  https://mcp.alphavantage.co/sse?apikey=YOUR_API_KEY
- stdio: local server
  uvx marketdata-mcp-server YOUR_API_KEY

The API key is taken from ALPHA_VANTAGE_API_KEY (or ALPHA_VANTAGE_KEY).
Never log the raw key or URLs that include it.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

DEFAULT_MCP_HTTP_URL = 'https://mcp.alphavantage.co/mcp'
DEFAULT_MCP_SSE_URL = 'https://mcp.alphavantage.co/sse'
DEFAULT_STDIO_COMMAND = 'uvx'
DEFAULT_STDIO_ARGS = ('marketdata-mcp-server',)

_TRANSPORT_ALIASES = {
    'http': 'http',
    'streamable-http': 'http',
    'streamable_http': 'http',
    'mcp': 'http',
    'sse': 'sse',
    'stdio': 'stdio',
    'rest': 'rest',
}


def get_api_key() -> str:
    key = (
        os.environ.get('ALPHA_VANTAGE_API_KEY', '').strip()
        or os.environ.get('ALPHA_VANTAGE_KEY', '').strip()
    )
    if not key:
        raise ValueError(
            'ALPHA_VANTAGE_API_KEY (or ALPHA_VANTAGE_KEY) is not set. '
            'Get a free key at https://www.alphavantage.co/support/#api-key '
            'and export it (or set it in docker-compose / .env).'
        )
    return key


def get_transport() -> str:
    """Return the configured client transport: http, sse, stdio, or rest."""
    raw = os.environ.get('ALPHA_VANTAGE_TRANSPORT', 'http').strip().lower()
    transport = _TRANSPORT_ALIASES.get(raw)
    if not transport:
        valid = ', '.join(sorted(set(_TRANSPORT_ALIASES.values())))
        raise ValueError(
            f'Unknown ALPHA_VANTAGE_TRANSPORT={raw!r}. Use one of: {valid}.'
        )
    return transport


def redact_secrets(text: str, secret: str | None = None) -> str:
    """Strip API keys from URLs and exception text before logging."""
    redacted = re.sub(r'(?i)([?&]apikey=)[^&\s]+', r'\1***', text)
    redacted = re.sub(r'(?i)(apikey["\']?\s*[:=]\s*["\']?)[^&\s"\']+', r'\1***', redacted)
    if secret:
        redacted = redacted.replace(secret, '***')
    return redacted


def _with_apikey_query(url: str, api_key: str) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query['apikey'] = api_key
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def mcp_http_url(api_key: str | None = None) -> str:
    base = os.environ.get('ALPHA_VANTAGE_MCP_URL', DEFAULT_MCP_HTTP_URL).strip() or DEFAULT_MCP_HTTP_URL
    return _with_apikey_query(base, api_key or get_api_key())


def mcp_sse_url(api_key: str | None = None) -> str:
    base = os.environ.get('ALPHA_VANTAGE_MCP_SSE_URL', DEFAULT_MCP_SSE_URL).strip() or DEFAULT_MCP_SSE_URL
    return _with_apikey_query(base, api_key or get_api_key())


def stdio_server_spec(api_key: str | None = None) -> tuple[str, list[str]]:
    key = api_key or get_api_key()
    command = os.environ.get('ALPHA_VANTAGE_MCP_STDIO_COMMAND', DEFAULT_STDIO_COMMAND).strip() or DEFAULT_STDIO_COMMAND
    raw_args = os.environ.get('ALPHA_VANTAGE_MCP_STDIO_ARGS', ' '.join(DEFAULT_STDIO_ARGS)).strip()
    args = raw_args.split() if raw_args else list(DEFAULT_STDIO_ARGS)
    if args[-1:] != [key]:
        args.append(key)
    return command, args


def _exception_text(exc: BaseException) -> str:
    """Flatten ExceptionGroup / TaskGroup messages for Airflow logs."""
    parts: list[str] = [str(exc)]
    nested = getattr(exc, 'exceptions', None)
    if nested:
        parts.extend(str(item) for item in nested)
    cause = exc.__cause__
    if cause is not None:
        parts.append(str(cause))
        cause_nested = getattr(cause, 'exceptions', None)
        if cause_nested:
            parts.extend(str(item) for item in cause_nested)
    return ' | '.join(part for part in parts if part)


def _normalize_tool_payload(payload: Any, *, fallback_text: str = '') -> dict:
    if isinstance(payload, dict) and list(payload.keys()) == ['result']:
        payload = payload['result']
    if isinstance(payload, str):
        payload = _parse_tool_text(payload)
    if payload is None and fallback_text:
        payload = _parse_tool_text(fallback_text)
    if not isinstance(payload, dict):
        raise ValueError(f'Unexpected Alpha Vantage MCP payload type: {type(payload)}')
    return payload


def _parse_tool_text(text: str) -> dict:
    stripped = text.strip()
    if not stripped:
        raise ValueError('Alpha Vantage MCP tool returned no content')
    try:
        decoded = json.loads(stripped)
    except json.JSONDecodeError:
        decoded = None
    if isinstance(decoded, dict):
        return decoded
    if isinstance(decoded, list):
        return {'data': decoded}
    return _parse_csv_series(stripped)


def _parse_csv_series(text: str) -> dict:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) < 2 or ',' not in lines[0]:
        raise ValueError(f'Alpha Vantage MCP tool returned non-JSON text: {text[:300]}')
    headers = [item.strip().lower() for item in lines[0].split(',')]
    date_i = next((i for i, name in enumerate(headers) if name in {'timestamp', 'date', 'time'}), 0)
    value_i = next(
        (i for i, name in enumerate(headers) if name in {'value', 'price', 'close'}),
        1 if len(headers) > 1 else 0,
    )
    series = []
    for row in lines[1:]:
        parts = [item.strip() for item in row.split(',')]
        if len(parts) <= max(date_i, value_i):
            continue
        raw = parts[value_i]
        if raw in {'', '.'}:
            continue
        series.append({'date': parts[date_i][:10], 'value': raw})
    if not series:
        raise ValueError('Alpha Vantage MCP CSV contained no observations')
    return {'data': series}


def parse_mcp_tool_result(result: Any) -> dict:
    """Turn an MCP CallToolResult into an Alpha Vantage JSON object."""
    if getattr(result, 'isError', False):
        detail = _content_text(getattr(result, 'content', None)) or 'MCP tool returned isError'
        raise RuntimeError(f'Alpha Vantage MCP tool error: {detail}')

    text = _content_text(getattr(result, 'content', None))
    structured = getattr(result, 'structuredContent', None)
    if isinstance(structured, dict) and structured:
        payload = _normalize_tool_payload(structured, fallback_text=text)
    else:
        payload = _parse_tool_text(text)

    for key in ('Error Message', 'Information', 'Note'):
        if key in payload:
            raise RuntimeError(f'Alpha Vantage {key}: {payload[key]}')
    return payload


def _content_text(content: Any) -> str:
    if not content:
        return ''
    chunks: list[str] = []
    for item in content:
        text = getattr(item, 'text', None)
        if text:
            chunks.append(text)
        elif isinstance(item, dict) and item.get('text'):
            chunks.append(str(item['text']))
        elif isinstance(item, str):
            chunks.append(item)
    return '\n'.join(chunks).strip()


def list_mcp_tool_names(tools_result: Any) -> list[str]:
    tools = getattr(tools_result, 'tools', tools_result)
    names = []
    for tool in tools or []:
        name = getattr(tool, 'name', None) or (tool.get('name') if isinstance(tool, dict) else None)
        if name:
            names.append(name)
    return names


class AlphaVantageMcpClient:
    """Synchronous helper that keeps one MCP session open for multiple reads."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        transport: str | None = None,
        session_cm_factory=None,
    ):
        self.api_key = api_key or get_api_key()
        self.transport = transport or get_transport()
        if self.transport == 'rest':
            raise ValueError('AlphaVantageMcpClient is for MCP transports, not rest')
        self._session_cm_factory = session_cm_factory or _open_mcp_session
        self._loop: asyncio.AbstractEventLoop | None = None
        self._session_cm = None
        self._session = None

    def __enter__(self) -> 'AlphaVantageMcpClient':
        self._loop = asyncio.new_event_loop()
        try:
            self._session_cm = self._session_cm_factory(self.transport, self.api_key)
            self._session = self._loop.run_until_complete(self._session_cm.__aenter__())
        except Exception as exc:
            if self._loop is not None:
                self._loop.close()
                self._loop = None
            raise RuntimeError(
                f'Failed to open Alpha Vantage MCP ({self.transport}) session: '
                f'{redact_secrets(_exception_text(exc), self.api_key)}'
            ) from None
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if self._session_cm is not None and self._loop is not None:
                self._loop.run_until_complete(self._session_cm.__aexit__(exc_type, exc, tb))
        except Exception:
            # Remote MCP may return 501 on session DELETE; don't fail the task after a successful read.
            pass
        finally:
            if self._loop is not None:
                self._loop.close()
            self._session = None
            self._session_cm = None
            self._loop = None

    def list_tools(self) -> list[str]:
        try:
            result = self._run(self._session.list_tools())
            return list_mcp_tool_names(result)
        except Exception as exc:
            raise RuntimeError(
                f'Alpha Vantage MCP tools/list failed: '
                f'{redact_secrets(_exception_text(exc), self.api_key)}'
            ) from None

    def call_tool(self, name: str, arguments: dict | None = None) -> dict:
        try:
            result = self._run(self._session.call_tool(name, arguments or {}))
            return parse_mcp_tool_result(result)
        except Exception as exc:
            raise RuntimeError(
                f'Alpha Vantage MCP tools/call {name} failed: '
                f'{redact_secrets(_exception_text(exc), self.api_key)}'
            ) from None

    def _run(self, coro):
        if self._loop is None or self._session is None:
            raise RuntimeError('AlphaVantageMcpClient must be used as a context manager')
        return self._loop.run_until_complete(coro)


@asynccontextmanager
async def _open_mcp_session(transport: str, api_key: str):
    from mcp import ClientSession

    async with _mcp_streams(transport, api_key) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            yield session


@asynccontextmanager
async def _mcp_streams(transport: str, api_key: str):
    if transport == 'http':
        from mcp.client.streamable_http import streamable_http_client

        async with streamable_http_client(mcp_http_url(api_key)) as streams:
            yield streams[0], streams[1]
        return
    if transport == 'sse':
        from mcp.client.sse import sse_client

        async with sse_client(mcp_sse_url(api_key)) as (read_stream, write_stream):
            yield read_stream, write_stream
        return
    if transport == 'stdio':
        from mcp import StdioServerParameters
        from mcp.client.stdio import stdio_client

        command, args = stdio_server_spec(api_key)
        params = StdioServerParameters(command=command, args=args)
        async with stdio_client(params) as (read_stream, write_stream):
            yield read_stream, write_stream
        return
    raise ValueError(f'Unsupported MCP transport: {transport}')
