"""Tests that commodity fetch uses a mocked MCP client (no live key)."""

from __future__ import annotations

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


if __name__ == '__main__':
    unittest.main()
