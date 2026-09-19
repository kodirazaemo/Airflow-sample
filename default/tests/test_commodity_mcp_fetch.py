"""Tests that commodity fetch uses a mocked MCP client (no live key)."""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from types import ModuleType
from unittest.mock import patch


def _ensure_airflow_stubs() -> None:
    """Allow importing sample DAGs without a full Airflow install."""
    if 'airflow.sdk' in sys.modules:
        return
    try:
        import airflow.sdk  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    def _mod(name: str) -> ModuleType:
        module = ModuleType(name)
        sys.modules[name] = module
        return module

    _mod('airflow')
    sdk = _mod('airflow.sdk')

    class DAG:
        def __init__(self, *args, **kwargs):
            self.dag_id = kwargs.get('dag_id', args[0] if args else None)
            self.description = kwargs.get('description')

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    sdk.DAG = DAG
    _mod('airflow.providers')
    _mod('airflow.providers.standard')
    _mod('airflow.providers.standard.operators')
    python_op = _mod('airflow.providers.standard.operators.python')

    class PythonOperator:
        def __init__(self, *args, **kwargs):
            pass

        def __rshift__(self, other):
            return other

    python_op.PythonOperator = PythonOperator


class CommodityMcpFetchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_airflow_stubs()
        import commodity_dag as dag_module

        cls.dag_module = dag_module

    def test_fetch_quote_via_mocked_mcp_client(self):
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
        with patch.dict(
            os.environ,
            {'ALPHA_VANTAGE_TRANSPORT': 'http', 'ALPHA_VANTAGE_API_KEY': 'secret-test-key'},
        ):
            quote = self.dag_module.fetch_commodity_quote(
                'CRUDE_OIL',
                {'function': 'WTI', 'params': {'interval': 'monthly'}, 'lot_size': 1000},
                client=fake,
            )
        self.assertEqual(quote['price'], 80.0)
        self.assertEqual(quote['function'], 'WTI')
        self.assertEqual(quote['source'], 'alpha_vantage_mcp')
        self.assertEqual(fake.last, ('WTI', {'interval': 'monthly'}))

    def test_rest_transport_does_not_open_mcp_client(self):
        with patch.dict(
            os.environ,
            {'ALPHA_VANTAGE_TRANSPORT': 'rest', 'ALPHA_VANTAGE_API_KEY': 'secret-test-key'},
        ):
            with patch.object(
                self.dag_module,
                '_alpha_vantage_rest_get',
                return_value={'unit': 'USD', 'price': '12.5', 'timestamp': '2024-01-02T00:00:00'},
            ) as rest:
                quote = self.dag_module.fetch_commodity_quote(
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

    def test_mcp_read_dag_imports(self):
        import alpha_vantage_mcp_read_dag as read_dag

        self.assertEqual(read_dag.dag.dag_id, 'alpha_vantage_mcp_read')
        self.assertEqual(self.dag_module.dag.dag_id, 'commodity_trading_dag')


class AsyncFetchMarketPricesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_airflow_stubs()
        import commodity_dag as dag_module

        cls.dag_module = dag_module

    def test_async_fetch_uses_gather_and_async_mcp_client(self):
        class FakeAsyncClient:
            def __init__(self):
                self.calls: list[tuple[str, dict]] = []
                self.list_calls = 0

            async def list_tools_async(self):
                self.list_calls += 1
                return ['WTI', 'COPPER', 'WHEAT', 'NATURAL_GAS', 'GOLD_SILVER_HISTORY']

            async def call_tool_async(self, name, arguments=None):
                self.calls.append((name, arguments or {}))
                await asyncio.sleep(0)
                price = 10.0 + len(self.calls)
                return {
                    'unit': 'USD',
                    'data': [
                        {'date': '2024-02-01', 'value': str(price)},
                        {'date': '2024-01-01', 'value': str(price - 1)},
                    ],
                }

            def run(self, coro):
                return asyncio.run(coro)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        fake = FakeAsyncClient()
        commodities = {
            'CRUDE_OIL': {'function': 'WTI', 'params': {'interval': 'monthly'}, 'lot_size': 1000},
            'COPPER': {'function': 'COPPER', 'params': {'interval': 'monthly'}, 'lot_size': 25},
        }
        pushed = {}

        class FakeTI:
            def xcom_push(self, key, value):
                pushed[key] = value

        with patch.dict(
            os.environ,
            {
                'ALPHA_VANTAGE_TRANSPORT': 'http',
                'ALPHA_VANTAGE_API_KEY': 'secret-test-key',
                'ALPHA_VANTAGE_REQUEST_PAUSE_SECONDS': '0',
                'ALPHA_VANTAGE_FETCH_CONCURRENCY': '2',
            },
        ):
            with patch.object(self.dag_module, 'get_commodities', return_value=commodities):
                with patch.object(self.dag_module, 'AlphaVantageMcpClient', return_value=fake):
                    prices = self.dag_module.fetch_market_prices(
                        ti=FakeTI(),
                    )

        self.assertEqual(fake.list_calls, 1)
        self.assertEqual(len(fake.calls), 2)
        self.assertEqual(set(prices), {'CRUDE_OIL', 'COPPER'})
        self.assertEqual(set(pushed['market_prices']), {'CRUDE_OIL', 'COPPER'})
        self.assertEqual(len(pushed['price_series']['CRUDE_OIL']), 2)
        self.assertTrue(all(q['source'] == 'alpha_vantage_mcp' for q in prices.values()))

    def test_async_rest_fetch_uses_to_thread(self):
        commodities = {
            'GOLD': {
                'function': 'GOLD_SILVER_SPOT',
                'params': {'symbol': 'GOLD'},
                'lot_size': 10,
            },
            'WHEAT': {'function': 'WHEAT', 'params': {'interval': 'monthly'}, 'lot_size': 25},
        }
        rest_calls: list[tuple[str, dict]] = []

        def fake_rest(function, params):
            rest_calls.append((function, params))
            return {
                'unit': 'USD',
                'data': [
                    {'date': '2024-03-01', 'value': '100'},
                    {'date': '2024-02-01', 'value': '90'},
                ],
            }

        pushed = {}

        class FakeTI:
            def xcom_push(self, key, value):
                pushed[key] = value

        with patch.dict(
            os.environ,
            {
                'ALPHA_VANTAGE_TRANSPORT': 'rest',
                'ALPHA_VANTAGE_API_KEY': 'secret-test-key',
                'ALPHA_VANTAGE_REQUEST_PAUSE_SECONDS': '0',
                'ALPHA_VANTAGE_FETCH_CONCURRENCY': '2',
            },
        ):
            with patch.object(self.dag_module, 'get_commodities', return_value=commodities):
                with patch.object(self.dag_module, '_alpha_vantage_rest_get', side_effect=fake_rest):
                    prices = self.dag_module.fetch_market_prices(ti=FakeTI())

        self.assertEqual(len(rest_calls), 2)
        self.assertEqual(set(prices), {'GOLD', 'WHEAT'})
        self.assertTrue(all(q['source'] == 'alpha_vantage_rest' for q in prices.values()))
        self.assertIn('market_prices', pushed)

    def test_request_pacer_spaces_calls(self):
        pacer = self.dag_module._RequestPacer(0.05)

        async def timed():
            start = asyncio.get_running_loop().time()
            await pacer.wait()
            first = asyncio.get_running_loop().time()
            await pacer.wait()
            second = asyncio.get_running_loop().time()
            return first - start, second - first

        first_gap, second_gap = asyncio.run(timed())
        self.assertLess(first_gap, 0.02)
        self.assertGreaterEqual(second_gap, 0.04)


if __name__ == '__main__':
    unittest.main()
