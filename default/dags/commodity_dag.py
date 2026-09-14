"""
Commodity trading sample DAG.

Daily pipeline that:
1. Fetches commodity prices from the Alpha Vantage API
2. Validates data quality
3. Computes trading signals (momentum, moving averages, RSI, MACD, OBV)
4. Combines signals with a weighted decision model
5. Generates trade recommendations
6. Publishes a daily summary report

Requires env var ALPHA_VANTAGE_API_KEY (or ALPHA_VANTAGE_KEY)
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

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

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

# Keep enough history for MACD (26+9) while staying within XCom-friendly size.
_MAX_SERIES_POINTS = 120

# Weighted decision model (weights should sum to ~1.0).
# Override any weight with env SIGNAL_WEIGHT_<NAME>, e.g. SIGNAL_WEIGHT_RSI=0.3
DEFAULT_SIGNAL_WEIGHTS = {
    'momentum': 0.15,
    'moving_average': 0.25,
    'rsi': 0.25,
    'macd': 0.25,
    'obv': 0.10,
}

# Aggregate score thresholds for the final action.
BUY_SCORE_THRESHOLD = float(os.environ.get('SIGNAL_BUY_THRESHOLD', '0.25'))
SELL_SCORE_THRESHOLD = float(os.environ.get('SIGNAL_SELL_THRESHOLD', '-0.25'))

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
    key = (
        os.environ.get('ALPHA_VANTAGE_API_KEY', '').strip()
        or os.environ.get('ALPHA_VANTAGE_KEY', '').strip()
    )
    if not key:
        raise ValueError(
            'ALPHA_VANTAGE_API_KEY (or ALPHA_VANTAGE_KEY) is not set. '
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

    Returns series newest-first (Alpha Vantage order).
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


def _chrono_prices(series_newest_first: list[dict], limit: int = _MAX_SERIES_POINTS) -> list[dict]:
    """Return oldest→newest price points, capped for XCom size."""
    chrono = list(reversed(series_newest_first))
    if len(chrono) > limit:
        chrono = chrono[-limit:]
    return chrono


def _latest_quote(series_newest_first: list[dict], unit: str) -> dict:
    latest = series_newest_first[0]
    previous = series_newest_first[1] if len(series_newest_first) > 1 else None
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
            series_newest_first, unit = _parse_price_series(payload)
            quote = _latest_quote(series_newest_first, unit)
            quote['symbol'] = symbol
            quote['function'] = function
            quote['interval'] = params.get('interval', 'spot')
            quote['series'] = _chrono_prices(series_newest_first)
            return quote
        except Exception as exc:  # noqa: BLE001 - collect and try fallback
            errors.append(str(exc))
            if index < len(attempts) - 1 and pause > 0:
                time.sleep(pause)

    raise RuntimeError(
        f'Failed to fetch {symbol} from Alpha Vantage after {len(attempts)} attempt(s): '
        + ' | '.join(errors)
    )


# ---------------------------------------------------------------------------
# Technical indicator helpers (pure functions; prices are oldest→newest)
# ---------------------------------------------------------------------------

def _sma(values: list[float], window: int) -> list[float | None]:
    """Simple moving average; leading values are None until the window is full."""
    out: list[float | None] = [None] * len(values)
    if window <= 0 or len(values) < window:
        return out
    running = sum(values[:window])
    out[window - 1] = running / window
    for i in range(window, len(values)):
        running += values[i] - values[i - window]
        out[i] = running / window
    return out


def _ema(values: list[float], window: int) -> list[float | None]:
    """Exponential moving average; seeding with SMA of the first window."""
    out: list[float | None] = [None] * len(values)
    if window <= 0 or len(values) < window:
        return out
    seed = sum(values[:window]) / window
    out[window - 1] = seed
    mult = 2.0 / (window + 1)
    prev = seed
    for i in range(window, len(values)):
        prev = (values[i] - prev) * mult + prev
        out[i] = prev
    return out


def _signal_result(score: int, value, detail: str) -> dict:
    if score > 0:
        action = 'BUY'
    elif score < 0:
        action = 'SELL'
    else:
        action = 'HOLD'
    return {
        'action': action,
        'score': int(score),
        'value': value,
        'detail': detail,
    }


def compute_momentum_signal(prices: list[float]) -> dict:
    """
    Period-over-period momentum.

    BUY when last change > +1%, SELL when < -1%, else HOLD.
    """
    if len(prices) < 2 or prices[-2] == 0:
        return _signal_result(0, None, 'insufficient history for momentum')

    change_pct = (prices[-1] - prices[-2]) / prices[-2] * 100
    if change_pct > 1.0:
        return _signal_result(1, round(change_pct, 3), f'momentum up {change_pct:+.3f}%')
    if change_pct < -1.0:
        return _signal_result(-1, round(change_pct, 3), f'momentum down {change_pct:+.3f}%')
    return _signal_result(0, round(change_pct, 3), f'momentum flat {change_pct:+.3f}%')


def compute_moving_average_signal(
    prices: list[float],
    short_window: int = 5,
    long_window: int = 20,
) -> dict:
    """
    Dual SMA crossover / price-vs-trend signal.

    BUY when short SMA > long SMA and price >= short SMA.
    SELL when short SMA < long SMA and price <= short SMA.
    """
    if len(prices) < long_window:
        return _signal_result(
            0,
            None,
            f'insufficient history for MA ({len(prices)}/{long_window})',
        )

    short = _sma(prices, short_window)
    long = _sma(prices, long_window)
    short_v = short[-1]
    long_v = long[-1]
    price = prices[-1]
    if short_v is None or long_v is None:
        return _signal_result(0, None, 'moving averages not ready')

    spread_pct = (short_v - long_v) / long_v * 100
    value = {
        'short_sma': round(short_v, 4),
        'long_sma': round(long_v, 4),
        'spread_pct': round(spread_pct, 3),
    }

    if short_v > long_v and price >= short_v:
        return _signal_result(
            1,
            value,
            f'SMA{short_window}>SMA{long_window} bullish ({spread_pct:+.3f}%)',
        )
    if short_v < long_v and price <= short_v:
        return _signal_result(
            -1,
            value,
            f'SMA{short_window}<SMA{long_window} bearish ({spread_pct:+.3f}%)',
        )
    return _signal_result(
        0,
        value,
        f'SMA{short_window}/SMA{long_window} mixed ({spread_pct:+.3f}%)',
    )


def compute_rsi_signal(prices: list[float], period: int = 14) -> dict:
    """
    Relative Strength Index.

    BUY when RSI < 30 (oversold), SELL when RSI > 70 (overbought).
    """
    if len(prices) < period + 1:
        return _signal_result(
            0,
            None,
            f'insufficient history for RSI ({len(prices)}/{period + 1})',
        )

    gains = []
    losses = []
    for i in range(1, len(prices)):
        delta = prices[i] - prices[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))

    rsi = round(rsi, 2)
    if rsi < 30:
        return _signal_result(1, rsi, f'RSI oversold ({rsi})')
    if rsi > 70:
        return _signal_result(-1, rsi, f'RSI overbought ({rsi})')
    return _signal_result(0, rsi, f'RSI neutral ({rsi})')


def compute_macd_signal(
    prices: list[float],
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> dict:
    """
    MACD line vs signal line.

    BUY when MACD > signal, SELL when MACD < signal.
    """
    min_len = slow + signal_period
    if len(prices) < min_len:
        return _signal_result(
            0,
            None,
            f'insufficient history for MACD ({len(prices)}/{min_len})',
        )

    fast_ema = _ema(prices, fast)
    slow_ema = _ema(prices, slow)
    macd_line: list[float | None] = [None] * len(prices)
    for i in range(len(prices)):
        if fast_ema[i] is not None and slow_ema[i] is not None:
            macd_line[i] = fast_ema[i] - slow_ema[i]

    macd_values = [v for v in macd_line if v is not None]
    signal_ema = _ema(macd_values, signal_period)
    if not signal_ema or signal_ema[-1] is None:
        return _signal_result(0, None, 'MACD signal line not ready')

    macd_v = macd_values[-1]
    signal_v = signal_ema[-1]
    hist = macd_v - signal_v
    value = {
        'macd': round(macd_v, 4),
        'signal': round(signal_v, 4),
        'histogram': round(hist, 4),
    }

    if hist > 0:
        return _signal_result(1, value, f'MACD bullish hist={hist:+.4f}')
    if hist < 0:
        return _signal_result(-1, value, f'MACD bearish hist={hist:+.4f}')
    return _signal_result(0, value, 'MACD flat')


def compute_obv_signal(prices: list[float], trend_window: int = 5) -> dict:
    """
    On-Balance Volume using synthetic volume.

    Alpha Vantage commodity series do not include volume, so volume is
    approximated as abs(period price change). OBV trend vs its SMA drives
    the signal: rising OBV → BUY, falling OBV → SELL.
    """
    if len(prices) < trend_window + 1:
        return _signal_result(
            0,
            None,
            f'insufficient history for OBV ({len(prices)}/{trend_window + 1})',
        )

    obv = [0.0]
    for i in range(1, len(prices)):
        volume = abs(prices[i] - prices[i - 1])
        if prices[i] > prices[i - 1]:
            obv.append(obv[-1] + volume)
        elif prices[i] < prices[i - 1]:
            obv.append(obv[-1] - volume)
        else:
            obv.append(obv[-1])

    obv_sma = _sma(obv, trend_window)
    latest_obv = obv[-1]
    latest_sma = obv_sma[-1]
    if latest_sma is None:
        return _signal_result(0, None, 'OBV trend not ready')

    delta = latest_obv - latest_sma
    value = {
        'obv': round(latest_obv, 4),
        'obv_sma': round(latest_sma, 4),
        'delta': round(delta, 4),
        'volume_proxy': 'abs_price_change',
    }

    # Require a small relative separation to avoid noise around the SMA.
    threshold = max(abs(latest_sma) * 0.01, 1e-6)
    if delta > threshold:
        return _signal_result(1, value, f'OBV rising vs SMA{trend_window}')
    if delta < -threshold:
        return _signal_result(-1, value, f'OBV falling vs SMA{trend_window}')
    return _signal_result(0, value, f'OBV flat vs SMA{trend_window}')


def get_signal_weights() -> dict[str, float]:
    """Load signal weights (env overrides supported)."""
    weights = dict(DEFAULT_SIGNAL_WEIGHTS)
    for name in list(weights):
        raw = os.environ.get(f'SIGNAL_WEIGHT_{name.upper()}')
        if raw is None:
            continue
        try:
            weights[name] = float(raw)
        except ValueError:
            pass

    total = sum(weights.values())
    if total <= 0:
        return dict(DEFAULT_SIGNAL_WEIGHTS)
    # Normalize so weights sum to 1.0
    return {name: value / total for name, value in weights.items()}


def combine_weighted_signals(component_signals: dict[str, dict], weights: dict[str, float]) -> dict:
    """
    Combine per-indicator scores into a single BUY / SELL / HOLD decision.

    Each component contributes score ∈ {-1, 0, +1} scaled by its weight.
    """
    weighted_score = 0.0
    for name, result in component_signals.items():
        weighted_score += weights.get(name, 0.0) * result.get('score', 0)

    weighted_score = round(weighted_score, 4)
    if weighted_score >= BUY_SCORE_THRESHOLD:
        action = 'BUY'
    elif weighted_score <= SELL_SCORE_THRESHOLD:
        action = 'SELL'
    else:
        action = 'HOLD'

    reasons = [
        f"{name}={result['action']}({result['detail']})"
        for name, result in component_signals.items()
    ]
    return {
        'action': action,
        'weighted_score': weighted_score,
        'thresholds': {
            'buy': BUY_SCORE_THRESHOLD,
            'sell': SELL_SCORE_THRESHOLD,
        },
        'weights': weights,
        'reason': (
            f'weighted_score={weighted_score:+.4f} → {action}; '
            + '; '.join(reasons)
        ),
    }


# ---------------------------------------------------------------------------
# Airflow tasks
# ---------------------------------------------------------------------------

def fetch_market_prices(**context):
    """Pull latest commodity prices (and history) from Alpha Vantage."""
    commodities = get_commodities()
    prices = {}
    series_by_symbol = {}
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
            'history_points': len(quote.get('series') or []),
        }
        series_by_symbol[symbol] = quote.get('series') or []
        print(
            f"Fetched {symbol} via Alpha Vantage ({quote.get('interval')}): "
            f"{quote['price']} {quote['unit']} ({quote['change_pct']:+.3f}%) "
            f"as of {quote['as_of']} [{prices[symbol]['history_points']} pts]"
        )
        if index < len(symbols) - 1 and pause > 0:
            time.sleep(pause)

    context['ti'].xcom_push(key='market_prices', value=prices)
    context['ti'].xcom_push(key='price_series', value=series_by_symbol)
    return prices


def validate_market_data(**context):
    """Ensure prices are present and within plausible bounds."""
    commodities = get_commodities()
    prices = context['ti'].xcom_pull(task_ids='fetch_market_prices', key='market_prices')
    series_by_symbol = context['ti'].xcom_pull(task_ids='fetch_market_prices', key='price_series') or {}
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
    context['ti'].xcom_push(key='price_series', value=series_by_symbol)
    return {'validated_count': len(prices)}


def compute_trading_signals(**context):
    """
    Compute per-indicator signals and combine them with a weighted model.

    Indicators (each in its own function):
    - momentum
    - moving averages
    - RSI
    - MACD
    - OBV (synthetic volume from abs price change)
    """
    prices = context['ti'].xcom_pull(task_ids='validate_market_data', key='validated_prices')
    series_by_symbol = context['ti'].xcom_pull(task_ids='validate_market_data', key='price_series') or {}
    weights = get_signal_weights()
    signals = {}

    print('Signal weights:', {k: round(v, 4) for k, v in weights.items()})
    print(f'Thresholds: BUY>={BUY_SCORE_THRESHOLD}, SELL<={SELL_SCORE_THRESHOLD}')

    for symbol, quote in prices.items():
        series = series_by_symbol.get(symbol) or []
        close_prices = [point['price'] for point in series]
        if not close_prices:
            close_prices = [quote['price']]

        components = {
            'momentum': compute_momentum_signal(close_prices),
            'moving_average': compute_moving_average_signal(close_prices),
            'rsi': compute_rsi_signal(close_prices),
            'macd': compute_macd_signal(close_prices),
            'obv': compute_obv_signal(close_prices),
        }
        decision = combine_weighted_signals(components, weights)

        signals[symbol] = {
            'action': decision['action'],
            'price': quote['price'],
            'unit': quote['unit'],
            'change_pct': quote['change_pct'],
            'weighted_score': decision['weighted_score'],
            'reason': decision['reason'],
            'components': components,
            'weights': weights,
        }
        print(
            f"{symbol}: {decision['action']} @ {quote['price']} "
            f"(score={decision['weighted_score']:+.4f})"
        )
        for name, result in components.items():
            print(f"  - {name}: {result['action']} | {result['detail']}")

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
            'weighted_score': signal.get('weighted_score'),
            'reason': signal['reason'],
        }
        orders.append(order)
        print(
            f"Order: {order['side']} {order['quantity']} {symbol} "
            f"@ {order['price']} (notional ${notional:,.2f}, "
            f"score={order['weighted_score']:+.4f})"
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
        'model': 'weighted_indicators',
    }

    print('=' * 60)
    print(f"Commodity Trading Daily Report — {report['trading_date']}")
    print('Price source: Alpha Vantage')
    print('Decision model: weighted momentum + MA + RSI + MACD + OBV')
    print('=' * 60)
    print(f"Tracked: {report['commodities_tracked']} commodities")
    for symbol, quote in prices.items():
        signal = signals[symbol]
        print(
            f"  {symbol:<12} {quote['price']:>12} {quote['unit']:<28} "
            f"{quote['change_pct']:+.3f}%  as of {quote['as_of']}  "
            f"→ {signal['action']} (score={signal['weighted_score']:+.4f})"
        )
        for name, component in signal.get('components', {}).items():
            print(f"      {name:<16} {component['action']:<4} {component['detail']}")
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
    dag_id='commodity_trading_dag',
    default_args=default_args,
    description='Commodity trading pipeline using Alpha Vantage + weighted technical signals',
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(days=1),
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
