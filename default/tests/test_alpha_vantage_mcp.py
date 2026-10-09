"""Unit tests for the Alpha Vantage MCP helper (no live API key)."""

from __future__ import annotations

import json
import os
import unittest
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

from alpha_vantage_mcp import (
    ALPHA_VANTAGE_CONN_ID,
    DEFAULT_FORWARDER_MCP_HTTP_URL,
    DEFAULT_MCP_HTTP_URL,
    DEFAULT_MCP_SSE_URL,
    AlphaVantageMcpClient,
    compose_connection_extra,
    connection_import_document,
    format_mcp_connect_failure,
    get_api_key,
    get_transport,
    list_mcp_tool_names,
    load_alpha_vantage_settings,
    mcp_http_url,
    mcp_sse_url,
    parse_mcp_tool_result,
    redact_secrets,
    stdio_server_spec,
    uses_host_mcp_forwarder,
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

    def test_default_mcp_urls_are_direct_not_forwarder(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop('ALPHA_VANTAGE_MCP_URL', None)
            os.environ.pop('ALPHA_VANTAGE_MCP_SSE_URL', None)
            with patch('alpha_vantage_mcp._connection_or_none', return_value=None):
                extra = compose_connection_extra()
                settings = load_alpha_vantage_settings()
                http_url = mcp_http_url('secret-test-key')
        self.assertEqual(extra['mcp_url'], DEFAULT_MCP_HTTP_URL)
        self.assertEqual(extra['mcp_sse_url'], DEFAULT_MCP_SSE_URL)
        self.assertEqual(settings['mcp_url'], DEFAULT_MCP_HTTP_URL)
        self.assertNotIn(':18080', extra['mcp_url'])
        self.assertTrue(http_url.startswith('https://mcp.alphavantage.co/mcp'))
        self.assertNotIn(':18080', http_url)
        self.assertFalse(uses_host_mcp_forwarder(DEFAULT_MCP_HTTP_URL))
        self.assertTrue(uses_host_mcp_forwarder(DEFAULT_FORWARDER_MCP_HTTP_URL))

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

    def test_env_fallback_when_no_airflow_connection(self):
        with patch.dict(
            os.environ,
            {
                'ALPHA_VANTAGE_API_KEY': 'secret-test-key',
                'ALPHA_VANTAGE_TRANSPORT': 'sse',
                'ALPHA_VANTAGE_MCP_URL': 'https://mcp.alphavantage.co:18080/mcp',
            },
            clear=True,
        ):
            with patch('alpha_vantage_mcp._connection_or_none', return_value=None):
                self.assertEqual(get_api_key(), 'secret-test-key')
                self.assertEqual(get_transport(), 'sse')
                self.assertIn(':18080/mcp', mcp_http_url())

    def test_connection_password_and_extra_override_env(self):
        conn = SimpleNamespace(
            password='conn-secret-key',
            extra_dejson={
                'transport': 'http',
                'mcp_url': 'https://mcp.alphavantage.co:18080/mcp',
                'mcp_sse_url': 'https://mcp.alphavantage.co:18080/sse',
            },
            extra=None,
        )
        with patch.dict(
            os.environ,
            {
                'ALPHA_VANTAGE_API_KEY': 'env-should-not-win',
                'ALPHA_VANTAGE_TRANSPORT': 'rest',
                'ALPHA_VANTAGE_MCP_URL': 'https://example.invalid/mcp',
            },
        ):
            with patch('alpha_vantage_mcp._connection_or_none', return_value=conn):
                settings = load_alpha_vantage_settings()
        self.assertEqual(settings['api_key'], 'conn-secret-key')
        self.assertEqual(settings['transport'], 'http')
        self.assertEqual(settings['mcp_url'], 'https://mcp.alphavantage.co:18080/mcp')
        self.assertEqual(settings['mcp_sse_url'], 'https://mcp.alphavantage.co:18080/sse')

    def test_connection_seed_document_defaults_to_direct_mcp_urls(self):
        with patch.dict(
            os.environ,
            {
                'ALPHA_VANTAGE_API_KEY': 'secret-test-key',
                'ALPHA_VANTAGE_TRANSPORT': 'http',
            },
            clear=True,
        ):
            extra = compose_connection_extra()
            doc = connection_import_document()
        self.assertEqual(extra['mcp_url'], DEFAULT_MCP_HTTP_URL)
        self.assertEqual(extra['mcp_sse_url'], DEFAULT_MCP_SSE_URL)
        self.assertIn(ALPHA_VANTAGE_CONN_ID, doc)
        self.assertEqual(doc[ALPHA_VANTAGE_CONN_ID]['password'], 'secret-test-key')
        self.assertEqual(doc[ALPHA_VANTAGE_CONN_ID]['conn_type'], 'generic')
        parsed_extra = json.loads(doc[ALPHA_VANTAGE_CONN_ID]['extra'])
        self.assertEqual(parsed_extra['transport'], 'http')
        self.assertEqual(parsed_extra['mcp_url'], extra['mcp_url'])

    def test_connection_seed_honors_forwarder_env_override(self):
        with patch.dict(
            os.environ,
            {
                'ALPHA_VANTAGE_API_KEY': 'secret-test-key',
                'ALPHA_VANTAGE_MCP_URL': DEFAULT_FORWARDER_MCP_HTTP_URL,
                'ALPHA_VANTAGE_MCP_SSE_URL': 'https://mcp.alphavantage.co:18080/sse',
            },
            clear=True,
        ):
            extra = compose_connection_extra()
        self.assertEqual(extra['mcp_url'], DEFAULT_FORWARDER_MCP_HTTP_URL)
        self.assertTrue(uses_host_mcp_forwarder(extra['mcp_url']))

    def test_connect_failure_includes_url_transport_and_desktop_hint(self):
        with patch.dict(os.environ, {'ALPHA_VANTAGE_API_KEY': 'secret-test-key'}, clear=True):
            with patch('alpha_vantage_mcp._connection_or_none', return_value=None):
                message = format_mcp_connect_failure(
                    'http',
                    'secret-test-key',
                    RuntimeError('All connection attempts failed'),
                )
        self.assertIn('http', message)
        self.assertIn(DEFAULT_MCP_HTTP_URL, message)
        self.assertIn('All connection attempts failed', message)
        self.assertIn('direct Streamable HTTP', message)
        self.assertIn('Docker Desktop', message)
        self.assertIn('WSL', message)
        self.assertNotIn('secret-test-key', message)

    def test_connect_failure_forwarder_url_tells_desktop_to_drop_port(self):
        with patch.dict(
            os.environ,
            {
                'ALPHA_VANTAGE_API_KEY': 'secret-test-key',
                'ALPHA_VANTAGE_MCP_URL': DEFAULT_FORWARDER_MCP_HTTP_URL,
            },
            clear=True,
        ):
            with patch('alpha_vantage_mcp._connection_or_none', return_value=None):
                message = format_mcp_connect_failure(
                    'http',
                    'secret-test-key',
                    RuntimeError('unhandled errors in a TaskGroup'),
                )
        self.assertIn(':18080', message)
        self.assertIn('network_mode: host', message)
        self.assertIn('https://mcp.alphavantage.co/mcp', message)
        self.assertNotIn('secret-test-key', message)

    def test_client_enter_wraps_connect_failure(self):
        @asynccontextmanager
        async def factory(transport, api_key):
            raise RuntimeError('All connection attempts failed')
            yield  # pragma: no cover

        with patch.dict(
            os.environ,
            {'ALPHA_VANTAGE_API_KEY': 'secret-test-key', 'ALPHA_VANTAGE_TRANSPORT': 'http'},
            clear=True,
        ):
            with patch('alpha_vantage_mcp._connection_or_none', return_value=None):
                with self.assertRaises(RuntimeError) as raised:
                    with AlphaVantageMcpClient(session_cm_factory=factory):
                        pass
        self.assertIn('Failed to open Alpha Vantage MCP (http)', str(raised.exception))
        self.assertIn('All connection attempts failed', str(raised.exception))
        self.assertIn(DEFAULT_MCP_HTTP_URL, str(raised.exception))
        self.assertNotIn('secret-test-key', str(raised.exception))


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
