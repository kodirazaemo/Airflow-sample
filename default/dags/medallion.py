"""
Medallion warehouse helpers for the commodity sample.

Bronze / silver / gold live in dedicated Postgres schemas when Airflow's
metadata DB is Postgres (docker compose). Unit tests fall back to SQLite
plus immutable JSON snapshots under MEDALLION_DATA_DIR.

Bronze snapshots are append-once: if a (trading_date, symbol) payload
already exists, extract skips a live MCP call.
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

BRONZE_SNAPSHOTS = 'bronze.mcp_snapshots'
SILVER_QUOTES = 'silver.commodity_quotes'
SILVER_SERIES = 'silver.commodity_price_series'
GOLD_METRICS = 'gold.commodity_metrics'
GOLD_OBV_QUALITY = 'gold.obv_quality'


def data_dir() -> Path:
    raw = os.environ.get('MEDALLION_DATA_DIR', '').strip()
    path = Path(raw) if raw else Path('/app/data/medallion')
    path.mkdir(parents=True, exist_ok=True)
    return path


def bronze_json_path(trading_date: str, symbol: str) -> Path:
    folder = data_dir() / 'bronze' / trading_date
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f'{symbol}.json'


def warehouse_url() -> str:
    return (
        os.environ.get('MEDALLION_SQLALCHEMY_CONN', '').strip()
        or os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', '').strip()
    )


def _is_postgres(url: str) -> bool:
    return url.startswith('postgresql')


def _sqlite_path() -> Path:
    return data_dir() / 'warehouse.sqlite'


def _table(qualified: str) -> str:
    url = warehouse_url()
    if url and _is_postgres(url):
        return qualified
    return qualified.replace('.', '_')


def _psycopg_dsn(url: str) -> str:
    for prefix in ('postgresql+psycopg2://', 'postgresql+psycopg://'):
        if url.startswith(prefix):
            return 'postgresql://' + url[len(prefix) :]
    return url


@contextmanager
def warehouse_connection() -> Iterator[Any]:
    url = warehouse_url()
    if url and _is_postgres(url):
        import psycopg2

        conn = psycopg2.connect(_psycopg_dsn(url))
        conn.autocommit = False
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return

    sqlite_path = _sqlite_path()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _placeholder(conn) -> str:
    return '%s' if conn.__class__.__module__.startswith('psycopg2') else '?'


def init_warehouse() -> None:
    """Create schemas/tables (idempotent). Safe to run on every container boot."""
    url = warehouse_url()
    with warehouse_connection() as conn:
        cur = conn.cursor()
        postgres = conn.__class__.__module__.startswith('psycopg2')
        if postgres:
            cur.execute('CREATE SCHEMA IF NOT EXISTS bronze')
            cur.execute('CREATE SCHEMA IF NOT EXISTS silver')
            cur.execute('CREATE SCHEMA IF NOT EXISTS gold')

        bronze = _table(BRONZE_SNAPSHOTS)
        silver_quotes = _table(SILVER_QUOTES)
        silver_series = _table(SILVER_SERIES)
        gold_metrics = _table(GOLD_METRICS)
        gold_quality = _table(GOLD_OBV_QUALITY)

        cur.execute(
            f'''
            CREATE TABLE IF NOT EXISTS {bronze} (
                trading_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                function TEXT NOT NULL,
                params_json TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                source TEXT NOT NULL,
                extracted_at TEXT NOT NULL,
                PRIMARY KEY (trading_date, symbol)
            )
            '''
        )
        cur.execute(
            f'''
            CREATE TABLE IF NOT EXISTS {silver_quotes} (
                trading_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                unit TEXT,
                change_pct DOUBLE PRECISION,
                as_of TEXT,
                prior_as_of TEXT,
                interval TEXT,
                source TEXT,
                history_points INTEGER,
                PRIMARY KEY (trading_date, symbol)
            )
            '''
        )
        cur.execute(
            f'''
            CREATE TABLE IF NOT EXISTS {silver_series} (
                trading_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                obs_date TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                unit TEXT,
                PRIMARY KEY (trading_date, symbol, obs_date)
            )
            '''
        )
        cur.execute(
            f'''
            CREATE TABLE IF NOT EXISTS {gold_metrics} (
                trading_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                latest_price DOUBLE PRECISION,
                first_price DOUBLE PRECISION,
                min_price DOUBLE PRECISION,
                max_price DOUBLE PRECISION,
                avg_price DOUBLE PRECISION,
                obs_count INTEGER,
                period_return_pct DOUBLE PRECISION,
                unit TEXT,
                PRIMARY KEY (trading_date, symbol)
            )
            '''
        )
        cur.execute(
            f'''
            CREATE TABLE IF NOT EXISTS {gold_quality} (
                trading_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                quality_score DOUBLE PRECISION,
                obv_action TEXT,
                price_trend_action TEXT,
                obv_price_correlation DOUBLE PRECISION,
                agreement DOUBLE PRECISION,
                detail TEXT,
                PRIMARY KEY (trading_date, symbol)
            )
            '''
        )
        cur.close()


def bronze_exists(trading_date: str, symbol: str) -> bool:
    if bronze_json_path(trading_date, symbol).is_file():
        return True
    init_warehouse()
    with warehouse_connection() as conn:
        cur = conn.cursor()
        ph = _placeholder(conn)
        cur.execute(
            f'SELECT 1 FROM {_table(BRONZE_SNAPSHOTS)} '
            f'WHERE trading_date = {ph} AND symbol = {ph}',
            (trading_date, symbol),
        )
        row = cur.fetchone()
        cur.close()
        return row is not None


def write_bronze_snapshot(
    trading_date: str,
    symbol: str,
    *,
    function: str,
    params: dict,
    payload: dict,
    source: str,
) -> dict:
    """Persist an immutable bronze snapshot. Existing rows/files are left untouched."""
    record = {
        'trading_date': trading_date,
        'symbol': symbol,
        'function': function,
        'params': params,
        'payload': payload,
        'source': source,
        'extracted_at': datetime.now(timezone.utc).isoformat(),
    }
    path = bronze_json_path(trading_date, symbol)
    if not path.is_file():
        path.write_text(json.dumps(record, sort_keys=True) + '\n')

    init_warehouse()
    with warehouse_connection() as conn:
        cur = conn.cursor()
        ph = _placeholder(conn)
        table = _table(BRONZE_SNAPSHOTS)
        cur.execute(
            f'SELECT 1 FROM {table} WHERE trading_date = {ph} AND symbol = {ph}',
            (trading_date, symbol),
        )
        if cur.fetchone() is None:
            cur.execute(
                f'''
                INSERT INTO {table}
                    (trading_date, symbol, function, params_json, payload_json, source, extracted_at)
                VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
                ''',
                (
                    trading_date,
                    symbol,
                    function,
                    json.dumps(params, sort_keys=True),
                    json.dumps(payload),
                    source,
                    record['extracted_at'],
                ),
            )
        cur.close()
    return record


def load_bronze_snapshot(trading_date: str, symbol: str) -> dict:
    path = bronze_json_path(trading_date, symbol)
    if path.is_file():
        return json.loads(path.read_text())

    init_warehouse()
    with warehouse_connection() as conn:
        cur = conn.cursor()
        ph = _placeholder(conn)
        cur.execute(
            f'''
            SELECT function, params_json, payload_json, source, extracted_at
            FROM {_table(BRONZE_SNAPSHOTS)}
            WHERE trading_date = {ph} AND symbol = {ph}
            ''',
            (trading_date, symbol),
        )
        row = cur.fetchone()
        cur.close()
    if row is None:
        raise FileNotFoundError(f'No bronze snapshot for {symbol} on {trading_date}')
    function, params_json, payload_json, source, extracted_at = row
    return {
        'trading_date': trading_date,
        'symbol': symbol,
        'function': function,
        'params': json.loads(params_json),
        'payload': json.loads(payload_json),
        'source': source,
        'extracted_at': extracted_at,
    }


def replace_silver_symbol(
    trading_date: str,
    symbol: str,
    *,
    quote: dict,
    series: list[dict],
    unit: str,
) -> None:
    init_warehouse()
    with warehouse_connection() as conn:
        cur = conn.cursor()
        ph = _placeholder(conn)
        quotes = _table(SILVER_QUOTES)
        series_table = _table(SILVER_SERIES)
        cur.execute(
            f'DELETE FROM {quotes} WHERE trading_date = {ph} AND symbol = {ph}',
            (trading_date, symbol),
        )
        cur.execute(
            f'DELETE FROM {series_table} WHERE trading_date = {ph} AND symbol = {ph}',
            (trading_date, symbol),
        )
        cur.execute(
            f'''
            INSERT INTO {quotes}
                (trading_date, symbol, price, unit, change_pct, as_of, prior_as_of,
                 interval, source, history_points)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
            ''',
            (
                trading_date,
                symbol,
                quote['price'],
                unit,
                quote.get('change_pct'),
                quote.get('as_of'),
                quote.get('prior_as_of'),
                quote.get('interval'),
                quote.get('source'),
                len(series),
            ),
        )
        for point in series:
            cur.execute(
                f'''
                INSERT INTO {series_table}
                    (trading_date, symbol, obs_date, price, unit)
                VALUES ({ph}, {ph}, {ph}, {ph}, {ph})
                ''',
                (trading_date, symbol, point['date'], point['price'], unit),
            )
        cur.close()


def load_silver_quotes(trading_date: str) -> dict[str, dict]:
    init_warehouse()
    with warehouse_connection() as conn:
        cur = conn.cursor()
        ph = _placeholder(conn)
        cur.execute(
            f'''
            SELECT symbol, price, unit, change_pct, as_of, prior_as_of, interval, source, history_points
            FROM {_table(SILVER_QUOTES)}
            WHERE trading_date = {ph}
            ''',
            (trading_date,),
        )
        rows = cur.fetchall()
        cur.close()
    quotes = {}
    for row in rows:
        quotes[row[0]] = {
            'price': row[1],
            'unit': row[2],
            'change_pct': row[3],
            'as_of': row[4],
            'prior_as_of': row[5],
            'interval': row[6],
            'source': row[7],
            'history_points': row[8],
        }
    return quotes


def load_silver_series(trading_date: str, symbol: str) -> list[dict]:
    init_warehouse()
    with warehouse_connection() as conn:
        cur = conn.cursor()
        ph = _placeholder(conn)
        cur.execute(
            f'''
            SELECT obs_date, price, unit
            FROM {_table(SILVER_SERIES)}
            WHERE trading_date = {ph} AND symbol = {ph}
            ORDER BY obs_date ASC
            ''',
            (trading_date, symbol),
        )
        rows = cur.fetchall()
        cur.close()
    return [{'date': row[0], 'price': row[1], 'unit': row[2]} for row in rows]


def compute_gold_metrics_sql(trading_date: str) -> list[dict]:
    """
    Gold layer: business metrics aggregated in SQL from silver series.

    A BI tool can read gold.commodity_metrics directly.
    """
    init_warehouse()
    series = _table(SILVER_SERIES)
    quotes = _table(SILVER_QUOTES)
    gold = _table(GOLD_METRICS)
    with warehouse_connection() as conn:
        cur = conn.cursor()
        ph = _placeholder(conn)
        cur.execute(f'DELETE FROM {gold} WHERE trading_date = {ph}', (trading_date,))
        cur.execute(
            f'''
            INSERT INTO {gold} (
                trading_date, symbol, latest_price, first_price, min_price, max_price,
                avg_price, obs_count, period_return_pct, unit
            )
            SELECT
                s.trading_date,
                s.symbol,
                (
                    SELECT s2.price FROM {series} s2
                    WHERE s2.trading_date = s.trading_date AND s2.symbol = s.symbol
                    ORDER BY s2.obs_date DESC LIMIT 1
                ) AS latest_price,
                (
                    SELECT s2.price FROM {series} s2
                    WHERE s2.trading_date = s.trading_date AND s2.symbol = s.symbol
                    ORDER BY s2.obs_date ASC LIMIT 1
                ) AS first_price,
                MIN(s.price) AS min_price,
                MAX(s.price) AS max_price,
                AVG(s.price) AS avg_price,
                COUNT(*) AS obs_count,
                CASE
                    WHEN (
                        SELECT s2.price FROM {series} s2
                        WHERE s2.trading_date = s.trading_date AND s2.symbol = s.symbol
                        ORDER BY s2.obs_date ASC LIMIT 1
                    ) IS NULL
                    OR (
                        SELECT s2.price FROM {series} s2
                        WHERE s2.trading_date = s.trading_date AND s2.symbol = s.symbol
                        ORDER BY s2.obs_date ASC LIMIT 1
                    ) = 0
                    THEN NULL
                    ELSE 100.0 * (
                        (
                            SELECT s2.price FROM {series} s2
                            WHERE s2.trading_date = s.trading_date AND s2.symbol = s.symbol
                            ORDER BY s2.obs_date DESC LIMIT 1
                        ) - (
                            SELECT s2.price FROM {series} s2
                            WHERE s2.trading_date = s.trading_date AND s2.symbol = s.symbol
                            ORDER BY s2.obs_date ASC LIMIT 1
                        )
                    ) / (
                        SELECT s2.price FROM {series} s2
                        WHERE s2.trading_date = s.trading_date AND s2.symbol = s.symbol
                        ORDER BY s2.obs_date ASC LIMIT 1
                    )
                END AS period_return_pct,
                (
                    SELECT q.unit FROM {quotes} q
                    WHERE q.trading_date = s.trading_date AND q.symbol = s.symbol
                ) AS unit
            FROM {series} s
            WHERE s.trading_date = {ph}
            GROUP BY s.trading_date, s.symbol
            ''',
            (trading_date,),
        )
        cur.execute(
            f'''
            SELECT trading_date, symbol, latest_price, first_price, min_price, max_price,
                   avg_price, obs_count, period_return_pct, unit
            FROM {gold}
            WHERE trading_date = {ph}
            ORDER BY symbol
            ''',
            (trading_date,),
        )
        rows = cur.fetchall()
        cur.close()
    metrics = []
    for row in rows:
        metrics.append(
            {
                'trading_date': row[0],
                'symbol': row[1],
                'latest_price': row[2],
                'first_price': row[3],
                'min_price': row[4],
                'max_price': row[5],
                'avg_price': row[6],
                'obs_count': row[7],
                'period_return_pct': row[8],
                'unit': row[9],
            }
        )
    return metrics


def upsert_obv_quality(trading_date: str, symbol: str, quality: dict) -> None:
    init_warehouse()
    with warehouse_connection() as conn:
        cur = conn.cursor()
        ph = _placeholder(conn)
        table = _table(GOLD_OBV_QUALITY)
        cur.execute(
            f'DELETE FROM {table} WHERE trading_date = {ph} AND symbol = {ph}',
            (trading_date, symbol),
        )
        cur.execute(
            f'''
            INSERT INTO {table}
                (trading_date, symbol, quality_score, obv_action, price_trend_action,
                 obv_price_correlation, agreement, detail)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
            ''',
            (
                trading_date,
                symbol,
                quality.get('quality_score'),
                quality.get('obv_action'),
                quality.get('price_trend_action'),
                quality.get('obv_price_correlation'),
                quality.get('agreement'),
                quality.get('detail'),
            ),
        )
        cur.close()


def load_obv_quality(trading_date: str) -> dict[str, dict]:
    init_warehouse()
    with warehouse_connection() as conn:
        cur = conn.cursor()
        ph = _placeholder(conn)
        cur.execute(
            f'''
            SELECT symbol, quality_score, obv_action, price_trend_action,
                   obv_price_correlation, agreement, detail
            FROM {_table(GOLD_OBV_QUALITY)}
            WHERE trading_date = {ph}
            ''',
            (trading_date,),
        )
        rows = cur.fetchall()
        cur.close()
    out = {}
    for row in rows:
        out[row[0]] = {
            'quality_score': row[1],
            'obv_action': row[2],
            'price_trend_action': row[3],
            'obv_price_correlation': row[4],
            'agreement': row[5],
            'detail': row[6],
        }
    return out
