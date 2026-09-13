"""
Commodity trading sample DAG.

Daily pipeline that:
1. Fetches commodity prices from the Alpha Vantage API
2. Validates data quality
3. Computes simple trading signals
4. Generates trade recommendations
5. Publishes a daily summary report

Requires env var ALPHA_VANTAGE_API_KEY
(https://www.alphavantage.co/support/#api-key).
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'trading',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

ALPHA_VANTAGE_BASE_URL = 'https://www.alphavantage.co/query'

# Intervals accepted by Alpha Vantage for industrial / agricultural commodities.
_MONTHLY_ONLY = frozenset({'monthly', 'quarterly', 'annual'})


def _preferred_interval(monthly_only: bool = False) -> str:
    """
    Default to monthly for free-tier compatibility.

    Set ALPHA_VANTAGE_INTERVAL=daily to prefer daily where the endpoint supports it.
    """
    requested = os.environ.get('ALPHA_VANTAGE_INTERVAL', 'monthly').strip().lower()
    if monthly_only:
        return requested if requested in _MONTHLY_ONLY else 'monthly'
    if requested in {'daily', 'weekly', 'monthly'}:
        return requested
    return 'monthly'


def _interval_attempts(monthly_only: bool = False) -> list[str]:
    primary = _preferred_interval(monthly_only=monthly_only)
    if monthly_only:
        return [primary]
    # Always keep monthly as a fallback for free/demo keys
    ordered = [primary]
    for candidate in ('daily', 'monthly'):
        if candidate not in ordered:
            ordered.append(candidate)
    return ordered


def _build_commodities() -> dict:
    """Build commodity config using the configured interval preference."""
    gold_attempts = _interval_attempts(monthly_only=False)
    energy_attempts = _interval_attempts(monthly_only=False)
    ag_interval = _preferred_interval(monthly_only=True)

    def energy(function: str, lot_size: int) -> dict:
        primary, *rest = energy_attempts
        cfg = {
            'function': function,
            'params': {'interval': primary},
            'lot_size': lot_size,
        }
        if rest:
            cfg['fallback_params'] = {'interval': rest[0]}
        return cfg

    gold_primary, *gold_rest = gold_attempts
    gold_cfg = {
        'function': 'GOLD_SILVER_HISTORY',
        'params': {'symbol': 'GOLD', 'interval': gold_primary},
        'lot_size': 10,  # troy ounces
        # Last-resort live quote if history is unavailable for this API key
        'spot_fallback': {
            'function': 'GOLD_SILVER_SPOT',
            'params': {'symbol': 'GOLD'},
        },
    }
    if gold_rest:
        gold_cfg['fallback_params'] = {'symbol': 'GOLD', 'interval': gold_rest[0]}

    return {
        'GOLD': gold_cfg,
        'CRUDE_OIL': energy('WTI', 1000),  # barrels
        'WHEAT': {
            'function': 'WHEAT',
            'params': {'interval': ag_interval},
            'lot_size': 25,  # metric tons
        },
        'COPPER': {
            'function': 'COPPER',
            'params': {'interval': ag_interval},
            'lot_size': 25,  # metric tons
        },
        'NATURAL_GAS': energy('NATURAL_GAS', 10000),  # MMBtu
    }


def get_commodities() -> dict:
    """Resolve commodity config from the current environment."""
    return _build_commodities()


def _api_key() -> str:
    key = os.environ.get('ALPHA_VANTAGE_API_KEY', '').strip()
    if not key:
        raise ValueError(
            'ALPHA_VANTAGE_API_KEY is not set. '
            'Get a free key at https://www.alphavantage.co/support/#api-key '
            'and export it (or set it in docker-compose / .env).'
        )
    return key


def _request_pause_seconds() -> float:
    """Pause between API calls to respect free-tier rate limits."""
    raw = os.environ.get('ALPHA_VANTAGE_REQUEST_PAUSE_SECONDS', '15')
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 15.0


def _alpha_vantage_get(function: str, params: dict) -> dict:
    # Keep apikey last. The public "demo" key rejects some query orderings.
    query = {'function': function, **params, 'apikey': _api_key()}
    url = f'{ALPHA_VANTAGE_BASE_URL}?{urllib.parse.urlencode(query)}'
    request = urllib.request.Request(
        url,
        headers={'User-Agent': 'airflow-commodity-sample/1.0'},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f'Alpha Vantage HTTP {exc.code} for {function}: {exc.reason}') from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f'Alpha Vantage network error for {function}: {exc.reason}') from exc

    if not isinstance(payload, dict):
        raise ValueError(f'Unexpected Alpha Vantage payload type for {function}: {type(payload)}')

    # Common Alpha Vantage soft-error envelopes
    for key in ('Error Message', 'Information', 'Note'):
        if key in payload:
            raise RuntimeError(f'Alpha Vantage {key} for {function}: {payload[key]}')

    return payload


def _parse_price_series(payload: dict) -> tuple[list[dict], str]:
    """
    Normalize Alpha Vantage commodity / gold-silver responses.

    Commodity endpoints use data[].value; gold/silver history uses data[].price.
    Spot responses expose a top-level price.
    """
    unit = payload.get('unit') or 'USD'

    if 'data' in payload:
        series = []
        for row in payload['data']:
            raw = row.get('value', row.get('price'))
            # Alpha Vantage occasionally emits '.' for missing observations
            if raw is None or raw == '' or raw == '.':
                continue
            series.append({'date': row['date'], 'price': float(raw)})
        if not series:
            raise ValueError('Alpha Vantage returned an empty price series')
        return series, unit

    if 'price' in payload:
        timestamp = str(payload.get('timestamp', ''))
        as_of = timestamp[:10] if timestamp else ''
        return [{'date': as_of, 'price': float(payload['price'])}], unit

    raise ValueError(f'Unrecognized Alpha Vantage payload keys: {sorted(payload)}')


def _latest_quote(series: list[dict], unit: str) -> dict:
    latest = series[0]
    previous = series[1] if len(series) > 1 else None
    price = latest['price']
    if previous and previous['price']:
        change_pct = round((price - previous['price']) / previous['price'] * 100, 3)
        prior_as_of = previous['date']
    else:
        change_pct = 0.0
        prior_as_of = None

    return {
        'price': round(price, 4),
        'unit': unit,
        'change_pct': change_pct,
        'as_of': latest['date'],
        'prior_as_of': prior_as_of,
        'source': 'alpha_vantage',
    }


def fetch_commodity_quote(symbol: str, meta: dict) -> dict:
    """Fetch one commodity from Alpha Vantage, with optional interval / spot fallback."""
    attempts = [(meta['function'], meta['params'])]
    if meta.get('fallback_params'):
        attempts.append((meta['function'], meta['fallback_params']))
    if meta.get('spot_fallback'):
        spot = meta['spot_fallback']
        attempts.append((spot['function'], spot['params']))

    errors = []
    pause = _request_pause_seconds()
    for index, (function, params) in enumerate(attempts):
        try:
            payload = _alpha_vantage_get(function, params)
            series, unit = _parse_price_series(payload)
            quote = _latest_quote(series, unit)
            quote['symbol'] = symbol
            quote['function'] = function
            quote['interval'] = params.get('interval', 'spot')
            return quote
        except Exception as exc:  # noqa: BLE001 - collect and try fallback
            errors.append(str(exc))
            if index < len(attempts) - 1 and pause > 0:
                time.sleep(pause)

    raise RuntimeError(
        f'Failed to fetch {symbol} from Alpha Vantage after {len(attempts)} attempt(s): '
        + ' | '.join(errors)
    )


def fetch_market_prices(**context):
    """Pull latest commodity prices from Alpha Vantage."""
    commodities = get_commodities()
    prices = {}
    symbols = list(commodities.items())
    pause = _request_pause_seconds()

    for index, (symbol, meta) in enumerate(symbols):
        quote = fetch_commodity_quote(symbol, meta)
        prices[symbol] = {
            'price': quote['price'],
            'unit': quote['unit'],
            'change_pct': quote['change_pct'],
            'as_of': quote['as_of'],
            'prior_as_of': quote.get('prior_as_of'),
            'interval': quote.get('interval'),
            'source': 'alpha_vantage',
        }
        print(
            f"Fetched {symbol} via Alpha Vantage ({quote.get('interval')}): "
            f"{quote['price']} {quote['unit']} ({quote['change_pct']:+.3f}%) "
            f"as of {quote['as_of']}"
        )
        if index < len(symbols) - 1 and pause > 0:
            time.sleep(pause)

    context['ti'].xcom_push(key='market_prices', value=prices)
    return prices


def validate_market_data(**context):
    """Ensure prices are present and within plausible bounds."""
    commodities = get_commodities()
    prices = context['ti'].xcom_pull(task_ids='fetch_market_prices', key='market_prices')
    if not prices:
        raise ValueError('No market prices received from upstream task')

    errors = []
    for symbol, quote in prices.items():
        price = quote['price']
        if price is None or price <= 0:
            errors.append(f'{symbol}: non-positive price ({price})')
        # Guardrail: reject moves larger than 50% as likely bad ticks
        # (monthly series can move more than daily)
        if abs(quote['change_pct']) > 50:
            errors.append(f'{symbol}: extreme move {quote["change_pct"]}%')

    missing = set(commodities) - set(prices)
    if missing:
        errors.append(f'Missing symbols: {sorted(missing)}')

    if errors:
        raise ValueError('Market data validation failed: ' + '; '.join(errors))

    print(f'Validated {len(prices)} Alpha Vantage commodity quotes successfully')
    context['ti'].xcom_push(key='validated_prices', value=prices)
    return {'validated_count': len(prices)}


def compute_trading_signals(**context):
    """
    Compute simple momentum signals from period-over-period change.

    Rules (demo only):
    - BUY when change > +1.0%
    - SELL when change < -1.0%
    - HOLD otherwise
    """
    prices = context['ti'].xcom_pull(task_ids='validate_market_data', key='validated_prices')
    signals = {}

    for symbol, quote in prices.items():
        change = quote['change_pct']
        if change > 1.0:
            action = 'BUY'
            reason = f'momentum up {change:+.3f}%'
        elif change < -1.0:
            action = 'SELL'
            reason = f'momentum down {change:+.3f}%'
        else:
            action = 'HOLD'
            reason = f'range-bound {change:+.3f}%'

        signals[symbol] = {
            'action': action,
            'price': quote['price'],
            'unit': quote['unit'],
            'change_pct': change,
            'reason': reason,
        }
        print(f'{symbol}: {action} @ {quote["price"]} ({reason})')

    context['ti'].xcom_push(key='trading_signals', value=signals)
    return signals


def generate_trade_orders(**context):
    """Turn BUY/SELL signals into notional trade orders."""
    commodities = get_commodities()
    signals = context['ti'].xcom_pull(task_ids='compute_trading_signals', key='trading_signals')

    orders = []
    for symbol, signal in signals.items():
        if signal['action'] == 'HOLD':
            continue
        qty = commodities[symbol]['lot_size']
        notional = round(qty * signal['price'], 2)
        order = {
            'symbol': symbol,
            'side': signal['action'],
            'quantity': qty,
            'price': signal['price'],
            'notional_usd': notional,
            'reason': signal['reason'],
        }
        orders.append(order)
        print(
            f"Order: {order['side']} {order['quantity']} {symbol} "
            f"@ {order['price']} (notional ${notional:,.2f})"
        )

    if not orders:
        print('No actionable trades today — all signals are HOLD')

    context['ti'].xcom_push(key='trade_orders', value=orders)
    return {'order_count': len(orders), 'orders': orders}


def publish_daily_report(**context):
    """Build a short end-of-day trading summary."""
    prices = context['ti'].xcom_pull(task_ids='validate_market_data', key='validated_prices')
    signals = context['ti'].xcom_pull(task_ids='compute_trading_signals', key='trading_signals')
    orders = context['ti'].xcom_pull(task_ids='generate_trade_orders', key='trade_orders') or []

    buys = sum(1 for s in signals.values() if s['action'] == 'BUY')
    sells = sum(1 for s in signals.values() if s['action'] == 'SELL')
    holds = sum(1 for s in signals.values() if s['action'] == 'HOLD')
    total_notional = sum(o['notional_usd'] for o in orders)

    report = {
        'trading_date': context['ds'],
        'price_source': 'alpha_vantage',
        'commodities_tracked': len(prices),
        'signals': {'BUY': buys, 'SELL': sells, 'HOLD': holds},
        'orders_generated': len(orders),
        'total_notional_usd': round(total_notional, 2),
    }

    print('=' * 60)
    print(f"Commodity Trading Daily Report — {report['trading_date']}")
    print('Price source: Alpha Vantage')
    print('=' * 60)
    print(f"Tracked: {report['commodities_tracked']} commodities")
    for symbol, quote in prices.items():
        print(
            f"  {symbol:<12} {quote['price']:>12} {quote['unit']:<28} "
            f"{quote['change_pct']:+.3f}%  as of {quote['as_of']}"
        )
    print(f"Signals: BUY={buys} SELL={sells} HOLD={holds}")
    print(f"Orders:  {len(orders)} (notional ${total_notional:,.2f})")
    for order in orders:
        print(
            f"  - {order['side']:4} {order['quantity']:>8} {order['symbol']:<12} "
            f"@ {order['price']}"
        )
    print('=' * 60)

    context['ti'].xcom_push(key='daily_report', value=report)
    return report


with DAG(
    'commodity_trading_dag',
    default_args=default_args,
    description='Commodity trading pipeline using Alpha Vantage market data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['commodity', 'trading', 'alpha-vantage', 'sample'],
) as dag:

    start = BashOperator(
        task_id='start_trading_session',
        bash_command='echo "Opening commodity trading session for {{ ds }} (Alpha Vantage)"',
    )

    fetch_prices = PythonOperator(
        task_id='fetch_market_prices',
        python_callable=fetch_market_prices,
    )

    validate_data = PythonOperator(
        task_id='validate_market_data',
        python_callable=validate_market_data,
    )

    compute_signals = PythonOperator(
        task_id='compute_trading_signals',
        python_callable=compute_trading_signals,
    )

    generate_orders = PythonOperator(
        task_id='generate_trade_orders',
        python_callable=generate_trade_orders,
    )

    publish_report = PythonOperator(
        task_id='publish_daily_report',
        python_callable=publish_daily_report,
    )

    end = BashOperator(
        task_id='close_trading_session',
        bash_command='echo "Commodity trading session closed for {{ ds }}"',
    )

    start >> fetch_prices >> validate_data >> compute_signals >> generate_orders >> publish_report >> end
