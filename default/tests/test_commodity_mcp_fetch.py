"""Tests that commodity fetch uses a mocked MCP client (no live key)."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch


class CommodityMcpFetchTests(unittest.TestCase):
    def test_fetch_quote_via_mocked_mcp_client(self):
        # Import inside the test so helper-only tests can still run if Airflow
        # is not on PYTHONPATH. Docker / local Airflow images have it.
        try:
            import commodity_dag
        except ModuleNotFoundError as exc:
            if 'airflow' in str(exc):
                self.skipTest('apache-airflow is not installed in this environment')
            raise

        class FakeClient:
            def call_tool(self, name, arguments=None):
                self.last = (name, arguments)
                return {
                    'unit': 'USD/barrel',
                    'data': [
                        {'date': '2024-02-01', 'value': '80'},
                        {'date': '2024-01-01', 'value': '70'},
                    ],
                }

        fake = FakeClient()
        with patch.dict(os.environ, {'ALPHA_VANTAGE_TRANSPORT': 'http', 'ALPHA_VANTAGE_API_KEY': 'secret-test-key'}):
            quote = commodity_dag.fetch_commodity_quote(
                'CRUDE_OIL',
                {'function': 'WTI', 'params': {'interval': 'monthly'}, 'lot_size': 1000},
                client=fake,
            )
        self.assertEqual(quote['price'], 80.0)
        self.assertEqual(quote['function'], 'WTI')
        self.assertEqual(quote['source'], 'alpha_vantage_mcp')
        self.assertEqual(fake.last, ('WTI', {'interval': 'monthly'}))

    def test_rest_transport_does_not_open_mcp_client(self):
        try:
            import commodity_dag
        except ModuleNotFoundError as exc:
            if 'airflow' in str(exc):
                self.skipTest('apache-airflow is not installed in this environment')
            raise

        with patch.dict(os.environ, {'ALPHA_VANTAGE_TRANSPORT': 'rest', 'ALPHA_VANTAGE_API_KEY': 'secret-test-key'}):
            with patch.object(
                commodity_dag,
                '_alpha_vantage_rest_get',
                return_value={'unit': 'USD', 'price': '12.5', 'timestamp': '2024-01-02T00:00:00'},
            ) as rest:
                quote = commodity_dag.fetch_commodity_quote(
                    'GOLD',
                    {
                        'function': 'GOLD_SILVER_SPOT',
                        'params': {'symbol': 'GOLD'},
                        'lot_size': 10,
                    },
                )
        rest.assert_called_once()
        self.assertEqual(quote['price'], 12.5)
        self.assertEqual(quote['source'], 'alpha_vantage_rest')


if __name__ == '__main__':
    unittest.main()
