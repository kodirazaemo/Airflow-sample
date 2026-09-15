"""Unit tests for the Alpha Vantage MCP helper (no live API key)."""

from __future__ import annotations

import json
import os
import unittest
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

from alpha_vantage_mcp import (
    AlphaVantageMcpClient,
    get_transport,
    list_mcp_tool_names,
    mcp_http_url,
    mcp_sse_url,
    parse_mcp_tool_result,
    redact_secrets,
    stdio_server_spec,
)


class FakeSession:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    async def list_tools(self):
        return SimpleNamespace(
            tools=[SimpleNamespace(name='WTI'), SimpleNamespace(name='COPPER')]
        )

    async def call_tool(self, name, arguments=None):
        self.calls.append((name, arguments or {}))
        if name == 'FAIL':
            return SimpleNamespace(
                isError=True,
                structuredContent=None,
                content=[SimpleNamespace(text='boom')],
            )
        payload = {
            'unit': 'USD/barrel',
            'data': [{'date': '2024-01-01', 'value': '71.25'}],
        }
        return SimpleNamespace(
            isError=False,
            structuredContent=None,
            content=[SimpleNamespace(text=json.dumps(payload))],
        )


class TransportConfigTests(unittest.TestCase):
    def test_default_transport_is_http(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop('ALPHA_VANTAGE_TRANSPORT', None)
            self.assertEqual(get_transport(), 'http')

    def test_transport_aliases(self):
        with patch.dict(os.environ, {'ALPHA_VANTAGE_TRANSPORT': 'streamable-http'}):
            self.assertEqual(get_transport(), 'http')
        with patch.dict(os.environ, {'ALPHA_VANTAGE_TRANSPORT': 'sse'}):
            self.assertEqual(get_transport(), 'sse')
        with patch.dict(os.environ, {'ALPHA_VANTAGE_TRANSPORT': 'stdio'}):
            self.assertEqual(get_transport(), 'stdio')
        with patch.dict(os.environ, {'ALPHA_VANTAGE_TRANSPORT': 'rest'}):
            self.assertEqual(get_transport(), 'rest')

    def test_unknown_transport_raises(self):
        with patch.dict(os.environ, {'ALPHA_VANTAGE_TRANSPORT': 'ftp'}):
            with self.assertRaises(ValueError):
                get_transport()

    def test_http_url_puts_apikey_in_query_but_redacts_logs(self):
        url = mcp_http_url('secret-test-key')
        self.assertIn('mcp.alphavantage.co/mcp', url)
        self.assertIn('apikey=secret-test-key', url)
        self.assertNotIn('secret-test-key', redact_secrets(url, 'secret-test-key'))
        self.assertIn('apikey=***', redact_secrets(url))

    def test_sse_url_matches_connection_examples(self):
        url = mcp_sse_url('secret-test-key')
        self.assertTrue(url.startswith('https://mcp.alphavantage.co/sse'))
        self.assertIn('apikey=secret-test-key', url)

    def test_stdio_spec_matches_uvx_docs(self):
        command, args = stdio_server_spec('secret-test-key')
        self.assertEqual(command, 'uvx')
        self.assertEqual(args, ['marketdata-mcp-server', 'secret-test-key'])

    def test_redact_does_not_leak_key_in_exception_text(self):
        leaked = 'Failed https://mcp.alphavantage.co/mcp?apikey=secret-test-key timeout'
        self.assertEqual(
            redact_secrets(leaked, 'secret-test-key'),
            'Failed https://mcp.alphavantage.co/mcp?apikey=*** timeout',
        )


class ParseResultTests(unittest.TestCase):
    def test_parse_json_text_content(self):
        result = SimpleNamespace(
            isError=False,
            structuredContent=None,
            content=[SimpleNamespace(text='{"unit":"USD","data":[]}')],
        )
        self.assertEqual(parse_mcp_tool_result(result)['unit'], 'USD')

    def test_parse_structured_content(self):
        result = SimpleNamespace(
            isError=False,
            structuredContent={'price': '2300.1', 'unit': 'USD'},
            content=[],
        )
        self.assertEqual(parse_mcp_tool_result(result)['price'], '2300.1')

    def test_parse_csv_text_content(self):
        result = SimpleNamespace(
            isError=False,
            structuredContent={'result': 'timestamp,value\n2026-08-01,83.9\n2026-07-01,80.46\n'},
            content=[],
        )
        payload = parse_mcp_tool_result(result)
        self.assertEqual(payload['data'][0]['value'], '83.9')
        self.assertEqual(payload['data'][0]['date'], '2026-08-01')
        result = SimpleNamespace(
            isError=False,
            structuredContent={'price': '2300.1', 'unit': 'USD'},
            content=[],
        )
        self.assertEqual(parse_mcp_tool_result(result)['price'], '2300.1')

    def test_parse_error_flag(self):
        result = SimpleNamespace(
            isError=True,
            structuredContent=None,
            content=[SimpleNamespace(text='nope')],
        )
        with self.assertRaises(RuntimeError):
            parse_mcp_tool_result(result)

    def test_parse_alpha_vantage_note(self):
        result = SimpleNamespace(
            isError=False,
            structuredContent={'Note': 'rate limit'},
            content=[],
        )
        with self.assertRaises(RuntimeError):
            parse_mcp_tool_result(result)

    def test_list_tool_names(self):
        listed = SimpleNamespace(tools=[SimpleNamespace(name='WTI'), {'name': 'GOLD_SILVER_HISTORY'}])
        self.assertEqual(list_mcp_tool_names(listed), ['WTI', 'GOLD_SILVER_HISTORY'])


class MockedMcpSessionTests(unittest.TestCase):
    def test_list_and_call_tools_with_fake_session(self):
        fake = FakeSession()

        @asynccontextmanager
        async def factory(transport, api_key):
            self.assertEqual(transport, 'http')
            self.assertEqual(api_key, 'secret-test-key')
            yield fake

        with patch.dict(os.environ, {'ALPHA_VANTAGE_API_KEY': 'secret-test-key', 'ALPHA_VANTAGE_TRANSPORT': 'http'}):
            with AlphaVantageMcpClient(session_cm_factory=factory) as client:
                tools = client.list_tools()
                payload = client.call_tool('WTI', {'interval': 'monthly'})

        self.assertEqual(tools, ['WTI', 'COPPER'])
        self.assertEqual(payload['data'][0]['value'], '71.25')
        self.assertEqual(fake.calls, [('WTI', {'interval': 'monthly'})])

    def test_call_tool_errors_are_redacted(self):
        @asynccontextmanager
        async def factory(transport, api_key):
            class Boom:
                async def call_tool(self, name, arguments=None):
                    raise RuntimeError(f'connect apikey={api_key}')

            yield Boom()

        with patch.dict(os.environ, {'ALPHA_VANTAGE_API_KEY': 'secret-test-key'}):
            with AlphaVantageMcpClient(
                api_key='secret-test-key',
                transport='http',
                session_cm_factory=factory,
            ) as client:
                with self.assertRaises(RuntimeError) as raised:
                    client.call_tool('WTI', {})
        self.assertNotIn('secret-test-key', str(raised.exception))
        self.assertIn('***', str(raised.exception))


if __name__ == '__main__':
    unittest.main()
