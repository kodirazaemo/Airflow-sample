"""Medallion warehouse + OBV data-quality tests (no live API key)."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_commodity_mcp_fetch import _ensure_airflow_stubs


class MedallionAndObvTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_airflow_stubs()
        import commodity_dag as dag_module
        import medallion as warehouse

        cls.dag_module = dag_module
        cls.warehouse = warehouse

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmpdir.name)
        self.env = {
            'MEDALLION_DATA_DIR': str(self.data_dir),
            'MEDALLION_SQLALCHEMY_CONN': '',
            'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN': '',
            'ALPHA_VANTAGE_TRANSPORT': 'http',
            'ALPHA_VANTAGE_API_KEY': 'secret-test-key',
            'ALPHA_VANTAGE_REQUEST_PAUSE_SECONDS': '0',
        }
        self._env_patch = patch.dict(os.environ, self.env, clear=False)
        self._env_patch.start()
        # Force sqlite file under the temp dir even if a parent env set Postgres.
        os.environ['MEDALLION_SQLALCHEMY_CONN'] = ''
        os.environ.pop('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', None)
        self.warehouse.init_warehouse()

    def tearDown(self):
        self._env_patch.stop()
        self._tmpdir.cleanup()

    def _payload(self, values):
        data = [{'date': f'2024-{i:02d}-01', 'value': str(value)} for i, value in enumerate(values, start=1)]
        data.reverse()  # Alpha Vantage newest-first
        return {'unit': 'USD/barrel', 'data': data}

    def test_bronze_skips_refetch_when_snapshot_exists(self):
        class CountingClient:
            def __init__(self):
                self.calls = 0

            def list_tools(self):
                return ['WTI']

            def call_tool(self, name, arguments=None):
                self.calls += 1
                return self._payload([70, 72, 74, 76, 80])

            def _payload(self, values):
                data = [
                    {'date': f'2024-{i:02d}-01', 'value': str(value)}
                    for i, value in enumerate(values, start=1)
                ]
                data.reverse()
                return {'unit': 'USD/barrel', 'data': data}

        fake = CountingClient()
        meta = {'function': 'WTI', 'params': {'interval': 'monthly'}, 'lot_size': 1000}
        context = {'ds': '2024-06-01', 'ti': _FakeTI()}

        with patch.object(self.dag_module, 'get_commodities', return_value={'CRUDE_OIL': meta}):
            with patch.object(self.dag_module, 'AlphaVantageMcpClient') as client_cm:
                client_cm.return_value.__enter__.return_value = fake
                client_cm.return_value.__exit__.return_value = None
                first = self.dag_module.extract_bronze(**context)
                second = self.dag_module.extract_bronze(**context)

        self.assertEqual(fake.calls, 1)
        self.assertEqual(first['fetched'], ['CRUDE_OIL'])
        self.assertEqual(second['reused'], ['CRUDE_OIL'])
        self.assertEqual(second['fetched'], [])
        snapshot = self.warehouse.load_bronze_snapshot('2024-06-01', 'CRUDE_OIL')
        self.assertEqual(snapshot['function'], 'WTI')
        self.assertIn('data', snapshot['payload'])

    def test_silver_gold_sql_and_obv_quality_loop(self):
        ds = '2024-06-01'
        rising = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        payload = self._payload(rising)
        self.warehouse.write_bronze_snapshot(
            ds,
            'CRUDE_OIL',
            function='WTI',
            params={'interval': 'monthly'},
            payload=payload,
            source='alpha_vantage_mcp',
        )
        context = {'ds': ds, 'ti': _FakeTI()}
        meta = {'function': 'WTI', 'params': {'interval': 'monthly'}, 'lot_size': 1000}

        with patch.object(self.dag_module, 'get_commodities', return_value={'CRUDE_OIL': meta}):
            silver = self.dag_module.transform_silver(**context)
            gold = self.dag_module.compute_gold(**context)
            quality = self.dag_module.score_obv_data_quality(**context)

        self.assertEqual(silver['validated_count'], 1)
        metrics = gold['metrics']
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0]['symbol'], 'CRUDE_OIL')
        self.assertEqual(metrics[0]['obs_count'], 11)
        self.assertEqual(metrics[0]['latest_price'], 20)
        self.assertEqual(metrics[0]['min_price'], 10)
        self.assertGreater(metrics[0]['period_return_pct'], 0)
        self.assertGreater(quality['CRUDE_OIL']['quality_score'], 0.5)
        self.assertEqual(quality['CRUDE_OIL']['obv_action'], 'BUY')
        self.assertEqual(quality['CRUDE_OIL']['price_trend_action'], 'BUY')

    def test_silver_rejects_null_prices(self):
        ds = '2024-06-01'
        self.warehouse.write_bronze_snapshot(
            ds,
            'CRUDE_OIL',
            function='WTI',
            params={'interval': 'monthly'},
            payload={'unit': 'USD', 'data': [{'date': '2024-01-01', 'value': '.'}]},
            source='alpha_vantage_mcp',
        )
        context = {'ds': ds, 'ti': _FakeTI()}
        meta = {'function': 'WTI', 'params': {'interval': 'monthly'}, 'lot_size': 1000}
        with patch.object(self.dag_module, 'get_commodities', return_value={'CRUDE_OIL': meta}):
            with self.assertRaises(ValueError):
                self.dag_module.transform_silver(**context)

    def test_obv_quality_aligned_uptrend_and_short_history(self):
        aligned = self.dag_module.compute_obv_data_quality(
            [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        )
        self.assertEqual(aligned['obv_action'], 'BUY')
        self.assertEqual(aligned['price_trend_action'], 'BUY')
        self.assertEqual(aligned['agreement'], 1.0)
        self.assertGreater(aligned['quality_score'], 0.7)
        short = self.dag_module.compute_obv_data_quality([1.0, 2.0])
        self.assertEqual(short['quality_score'], 0.0)
        self.assertIn('insufficient', short['detail'])

    def test_commodity_dag_graph_includes_medallion_and_quality(self):
        self.assertEqual(self.dag_module.dag.dag_id, 'commodity_trading_dag')
        self.assertIn('medallion', self.dag_module.dag.description.lower())


class _FakeTI:
    def __init__(self):
        self.store = {}

    def xcom_push(self, key, value):
        self.store[key] = value

    def xcom_pull(self, task_ids=None, key=None):
        return self.store.get(key)


if __name__ == '__main__':
    unittest.main()
