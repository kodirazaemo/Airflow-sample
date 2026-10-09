"""Mocked bronze profiler tests (no live API key, no Postgres)."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_commodity_mcp_fetch import _ensure_airflow_stubs

_ROOT = Path(__file__).resolve().parents[1]
_PLUGINS = _ROOT / 'plugins'
if str(_PLUGINS) not in sys.path:
    sys.path.insert(0, str(_PLUGINS))

from bronze_data_profiler import (  # noqa: E402
    BronzeProfileError,
    assert_profiles_acceptable,
    profile_bronze_record,
    profile_payload,
    summarize_profiles,
)


def _history_payload(values, *, placeholders=0, unit='USD/barrel'):
    data = []
    for i, value in enumerate(values, start=1):
        data.append({'date': f'2024-{i:02d}-01', 'value': str(value)})
    for j in range(placeholders):
        data.append({'date': f'2023-{j + 1:02d}-01', 'value': '.'})
    data.reverse()
    return {'unit': unit, 'data': data}


class BronzeProfilerUnitTests(unittest.TestCase):
    def test_history_payload_ok(self):
        profile = profile_payload('CRUDE_OIL', _history_payload(range(10, 22)))
        self.assertEqual(profile['severity'], 'ok')
        self.assertEqual(profile['numeric_count'], 12)
        self.assertEqual(profile['min_price'], 10)
        self.assertEqual(profile['max_price'], 21)
        self.assertEqual(profile['issues'], [])

    def test_spot_payload_warns_short_series(self):
        profile = profile_payload(
            'GOLD',
            {'symbol': 'GOLD', 'price': '2400.5', 'timestamp': '2024-06-01T00:00:00'},
        )
        self.assertEqual(profile['severity'], 'warn')
        self.assertEqual(profile['numeric_count'], 1)
        self.assertTrue(any('short series' in w for w in profile['warnings']))

    def test_empty_and_unrecognized_are_errors(self):
        empty = profile_payload('WHEAT', {'unit': 'USD', 'data': []})
        self.assertEqual(empty['severity'], 'error')
        self.assertIn('no numeric observations', empty['issues'])

        bad = profile_payload('COPPER', {'Information': 'rate limited'})
        self.assertEqual(bad['severity'], 'error')
        self.assertTrue(any('unrecognized' in issue for issue in bad['issues']))

        missing = profile_bronze_record('GOLD', None)
        self.assertEqual(missing['severity'], 'error')
        self.assertIn('missing bronze snapshot', missing['issues'])

    def test_placeholder_rate_and_duplicate_dates_warn(self):
        payload = {
            'unit': 'USD',
            'data': [
                {'date': '2024-01-01', 'value': '10'},
                {'date': '2024-01-01', 'value': '11'},
                {'date': '2024-02-01', 'value': '.'},
                {'date': '2024-03-01', 'value': '.'},
            ],
        }
        profile = profile_payload('WHEAT', payload)
        self.assertEqual(profile['severity'], 'warn')
        self.assertEqual(profile['duplicate_dates'], 1)
        self.assertTrue(any('placeholder' in w for w in profile['warnings']))
        self.assertTrue(any('duplicate' in w for w in profile['warnings']))

    def test_assert_profiles_fail_or_warn(self):
        error_row = profile_bronze_record('GOLD', None)
        summary = summarize_profiles([error_row])
        with self.assertRaises(BronzeProfileError):
            assert_profiles_acceptable(summary, fail_on_error=True)
        assert_profiles_acceptable(summary, fail_on_error=False)

        warn_row = profile_payload('GOLD', {'price': '10', 'timestamp': '2024-01-01'})
        warn_summary = summarize_profiles([warn_row])
        self.assertEqual(len(warn_summary['warnings']), 1)
        assert_profiles_acceptable(warn_summary, fail_on_error=True)


class BronzeProfilerDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_airflow_stubs()
        import commodity_dag as dag_module
        import medallion as warehouse

        cls.dag_module = dag_module
        cls.warehouse = warehouse

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.env = {
            'MEDALLION_DATA_DIR': self._tmpdir.name,
            'MEDALLION_SQLALCHEMY_CONN': '',
            'ALPHA_VANTAGE_TRANSPORT': 'http',
            'ALPHA_VANTAGE_API_KEY': 'secret-test-key',
            'ALPHA_VANTAGE_REQUEST_PAUSE_SECONDS': '0',
            'BRONZE_PROFILE_FAIL_ON_ERROR': 'true',
        }
        self._env_patch = patch.dict(os.environ, self.env, clear=False)
        self._env_patch.start()
        os.environ['MEDALLION_SQLALCHEMY_CONN'] = ''
        os.environ.pop('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', None)
        self.warehouse.init_warehouse()

    def tearDown(self):
        self._env_patch.stop()
        self._tmpdir.cleanup()

    def test_profile_bronze_between_extract_and_silver(self):
        ds = '2024-06-01'
        self.warehouse.write_bronze_snapshot(
            ds,
            'CRUDE_OIL',
            function='WTI',
            params={'interval': 'monthly'},
            payload=_history_payload(range(10, 22)),
            source='alpha_vantage_mcp',
        )
        context = {'ds': ds, 'ti': _FakeTI()}
        meta = {'function': 'WTI', 'params': {'interval': 'monthly'}, 'lot_size': 1000}

        with patch.object(self.dag_module, 'get_commodities', return_value={'CRUDE_OIL': meta}):
            summary = self.dag_module.profile_bronze(**context)
            silver = self.dag_module.transform_silver(**context)

        self.assertEqual(summary['ok'], 1)
        self.assertEqual(summary['errors'], [])
        self.assertEqual(silver['validated_count'], 1)
        self.assertEqual(context['ti'].store['bronze_profile']['profiled'], 1)

    def test_profile_bronze_fails_on_missing_snapshot(self):
        context = {'ds': '2024-06-01', 'ti': _FakeTI()}
        meta = {'function': 'WTI', 'params': {'interval': 'monthly'}, 'lot_size': 1000}
        with patch.object(self.dag_module, 'get_commodities', return_value={'CRUDE_OIL': meta}):
            with self.assertRaises(BronzeProfileError):
                self.dag_module.profile_bronze(**context)

    def test_commodity_dag_exposes_profile_callable(self):
        self.assertTrue(callable(self.dag_module.profile_bronze))
        self.assertIn('profile', self.dag_module.dag.description.lower())


class _FakeTI:
    def __init__(self):
        self.store = {}

    def xcom_push(self, key, value):
        self.store[key] = value

    def xcom_pull(self, task_ids=None, key=None):
        return self.store.get(key)


if __name__ == '__main__':
    unittest.main()
