"""
Commodity trading sample DAG.

Simulates a daily pipeline that:
1. Pulls commodity market prices
2. Validates data quality
3. Computes simple trading signals
4. Generates trade recommendations
5. Publishes a daily summary report
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'trading',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Sample commodities used by this demo pipeline
COMMODITIES = {
    'GOLD': {'unit': 'USD/oz', 'base_price': 2350.0, 'volatility': 0.012},
    'CRUDE_OIL': {'unit': 'USD/bbl', 'base_price': 78.5, 'volatility': 0.025},
    'WHEAT': {'unit': 'USD/bu', 'base_price': 5.85, 'volatility': 0.018},
    'COPPER': {'unit': 'USD/lb', 'base_price': 4.15, 'volatility': 0.015},
    'NATURAL_GAS': {'unit': 'USD/MMBtu', 'base_price': 2.95, 'volatility': 0.035},
}


def fetch_market_prices(**context):
    """Simulate fetching end-of-day commodity prices."""
    import hashlib
    import random

    # Deterministic "randomness" keyed by logical date so runs are reproducible
    logical_date = context['ds']
    seed = int(hashlib.md5(logical_date.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    prices = {}
    for symbol, meta in COMMODITIES.items():
        move = rng.uniform(-meta['volatility'], meta['volatility'])
        price = round(meta['base_price'] * (1 + move), 4)
        prices[symbol] = {
            'price': price,
            'unit': meta['unit'],
            'change_pct': round(move * 100, 3),
            'as_of': logical_date,
        }
        print(f"Fetched {symbol}: {price} {meta['unit']} ({move * 100:+.3f}%)")

    context['ti'].xcom_push(key='market_prices', value=prices)
    return prices


def validate_market_data(**context):
    """Ensure prices are present and within plausible bounds."""
    prices = context['ti'].xcom_pull(task_ids='fetch_market_prices', key='market_prices')
    if not prices:
        raise ValueError('No market prices received from upstream task')

    errors = []
    for symbol, quote in prices.items():
        price = quote['price']
        if price is None or price <= 0:
            errors.append(f'{symbol}: non-positive price ({price})')
        # Guardrail: reject moves larger than 20% as likely bad ticks
        if abs(quote['change_pct']) > 20:
            errors.append(f'{symbol}: extreme move {quote["change_pct"]}%')

    missing = set(COMMODITIES) - set(prices)
    if missing:
        errors.append(f'Missing symbols: {sorted(missing)}')

    if errors:
        raise ValueError('Market data validation failed: ' + '; '.join(errors))

    print(f'Validated {len(prices)} commodity quotes successfully')
    context['ti'].xcom_push(key='validated_prices', value=prices)
    return {'validated_count': len(prices)}


def compute_trading_signals(**context):
    """
    Compute simple momentum signals.

    Rules (demo only):
    - BUY when daily change > +1.0%
    - SELL when daily change < -1.0%
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
    signals = context['ti'].xcom_pull(task_ids='compute_trading_signals', key='trading_signals')
    # Fixed demo position size per active signal
    lot_sizes = {
        'GOLD': 10,          # ounces
        'CRUDE_OIL': 1000,   # barrels
        'WHEAT': 5000,       # bushels
        'COPPER': 25000,     # pounds
        'NATURAL_GAS': 10000 # MMBtu
    }

    orders = []
    for symbol, signal in signals.items():
        if signal['action'] == 'HOLD':
            continue
        qty = lot_sizes[symbol]
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
        'commodities_tracked': len(prices),
        'signals': {'BUY': buys, 'SELL': sells, 'HOLD': holds},
        'orders_generated': len(orders),
        'total_notional_usd': round(total_notional, 2),
    }

    print('=' * 60)
    print(f"Commodity Trading Daily Report — {report['trading_date']}")
    print('=' * 60)
    print(f"Tracked: {report['commodities_tracked']} commodities")
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
    description='Sample daily commodity trading pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['commodity', 'trading', 'sample'],
) as dag:

    start = BashOperator(
        task_id='start_trading_session',
        bash_command='echo "Opening commodity trading session for {{ ds }}"',
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
