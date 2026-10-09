"""
Commodity trading sample DAG (medallion layout).

Daily pipeline that:
1. Bronze — extracts raw Alpha Vantage MCP payloads to immutable snapshots
2. Silver — validates bronze (schema / nulls) and writes cleaned tables
3. Gold — SQL business metrics a BI tool can read, plus weighted signals
4. Compares synthetic-volume OBV against historical price trend lines
5. Generates trade recommendations and a daily report

Auth: Airflow Connection ``alpha_vantage_default`` (seeded from .env on boot).
Set ALPHA_VANTAGE_TRANSPORT=rest to use the legacy www.alphavantage.co REST API.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from alpha_vantage_mcp import AlphaVantageMcpClient, get_api_key, get_transport
from medallion import (
    bronze_exists,
    compute_gold_metrics_sql,
    init_warehouse,
    load_bronze_snapshot,
    load_obv_quality,
    load_silver_quotes,
    load_silver_series,
    replace_silver_symbol,
    upsert_obv_quality,
    write_bronze_snapshot,
)

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
    return get_api_key()


def _request_pause_seconds() -> float:
    """Pause between API calls to respect free-tier rate limits."""
    raw = os.environ.get('ALPHA_VANTAGE_REQUEST_PAUSE_SECONDS', '15')
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 15.0


def _alpha_vantage_rest_get(function: str, params: dict) -> dict:
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

    for key in ('Error Message', 'Information', 'Note'):
        if key in payload:
            raise RuntimeError(f'Alpha Vantage {key} for {function}: {payload[key]}')

    return payload


def _alpha_vantage_get(function: str, params: dict, client: AlphaVantageMcpClient | None = None) -> dict:
    """Read one Alpha Vantage function via MCP tools/call, or REST if configured."""
    if get_transport() == 'rest':
        return _alpha_vantage_rest_get(function, params)
    if client is None:
        with AlphaVantageMcpClient() as owned:
            return owned.call_tool(function, params)
    return client.call_tool(function, params)


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


def _price_source() -> str:
    return 'alpha_vantage_rest' if get_transport() == 'rest' else 'alpha_vantage_mcp'


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
        'source': _price_source(),
    }


def fetch_commodity_quote(
    symbol: str,
    meta: dict,
    client: AlphaVantageMcpClient | None = None,
) -> dict:
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
            payload = _alpha_vantage_get(function, params, client=client)
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


def fetch_raw_commodity_payload(
    symbol: str,
    meta: dict,
    client: AlphaVantageMcpClient | None = None,
) -> dict:
    """Fetch the raw MCP/REST JSON for bronze. Does not parse prices."""
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
            payload = _alpha_vantage_get(function, params, client=client)
            if not isinstance(payload, dict):
                raise ValueError(f'Unexpected payload type for {symbol}: {type(payload)}')
            return {
                'symbol': symbol,
                'function': function,
                'params': params,
                'payload': payload,
                'source': _price_source(),
            }
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


def _obv_series(prices: list[float]) -> list[float]:
    obv = [0.0]
    for i in range(1, len(prices)):
        volume = abs(prices[i] - prices[i - 1])
        if prices[i] > prices[i - 1]:
            obv.append(obv[-1] + volume)
        elif prices[i] < prices[i - 1]:
            obv.append(obv[-1] - volume)
        else:
            obv.append(obv[-1])
    return obv


def _linear_trend_slope(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(values) / n
    denom = sum((x - x_mean) ** 2 for x in xs)
    if denom == 0:
        return 0.0
    return sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values)) / denom


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = min(len(xs), len(ys))
    if n < 3:
        return None
    xs = xs[-n:]
    ys = ys[-n:]
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    cov = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    x_var = sum((x - x_mean) ** 2 for x in xs)
    y_var = sum((y - y_mean) ** 2 for y in ys)
    denom = (x_var * y_var) ** 0.5
    if denom == 0:
        return None
    return cov / denom


def compute_obv_data_quality(prices: list[float], trend_window: int = 5) -> dict:
    """
    Compare synthetic-volume OBV (compute_obv_signal) to the actual price trend.

    Commodity MCP series have no volume, so OBV uses |price change| as a proxy.
    This score measures whether that proxy agrees with a linear trend line on
    the same history (plus Pearson correlation of OBV vs price).
    """
    obv_signal = compute_obv_signal(prices, trend_window=trend_window)
    if len(prices) < trend_window + 1:
        return {
            'quality_score': 0.0,
            'obv_action': obv_signal['action'],
            'price_trend_action': 'HOLD',
            'obv_price_correlation': None,
            'agreement': 0.0,
            'price_trend_slope': None,
            'detail': obv_signal['detail'],
        }

    window = prices[-max(trend_window, 5) :]
    slope = _linear_trend_slope(window)
    last = window[-1] or 1.0
    rel_slope = slope / last
    # Relative slope threshold: ~0.1% per step in the fitted window.
    if rel_slope > 0.001:
        trend_score = 1
        trend_action = 'BUY'
    elif rel_slope < -0.001:
        trend_score = -1
        trend_action = 'SELL'
    else:
        trend_score = 0
        trend_action = 'HOLD'

    obv_score = int(obv_signal.get('score') or 0)
    if obv_score == trend_score:
        agreement = 1.0 if obv_score != 0 else 0.8
    elif obv_score == 0 or trend_score == 0:
        agreement = 0.5
    else:
        agreement = 0.0

    corr = _pearson(prices, _obv_series(prices))
    corr_score = 0.5 if corr is None else max(0.0, min(1.0, (corr + 1.0) / 2.0))
    quality = round(0.7 * agreement + 0.3 * corr_score, 4)
    detail = (
        f"OBV {obv_signal['action']} vs price-trend {trend_action} "
        f'(agreement={agreement:.2f}, corr={corr if corr is not None else "n/a"})'
    )
    return {
        'quality_score': quality,
        'obv_action': obv_signal['action'],
        'price_trend_action': trend_action,
        'obv_price_correlation': None if corr is None else round(corr, 4),
        'agreement': agreement,
        'price_trend_slope': round(rel_slope, 6),
        'detail': detail,
    }


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
# Airflow tasks (bronze → silver → gold)
# ---------------------------------------------------------------------------

def extract_bronze(**context):
    """
    Bronze extract: persist raw MCP/REST payloads.

    If a snapshot already exists for this trading date + symbol, skip the
    live call so a later task retry does not re-hit Alpha Vantage.
    """
    init_warehouse()
    ds = context['ds']
    commodities = get_commodities()
    symbols = list(commodities.items())
    pause = _request_pause_seconds()
    transport = get_transport()
    reused = []
    fetched = []

    pending = [(symbol, meta) for symbol, meta in symbols if not bronze_exists(ds, symbol)]
    already = [symbol for symbol, _meta in symbols if bronze_exists(ds, symbol)]
    reused.extend(already)
    for symbol in already:
        print(f'Reusing bronze snapshot for {symbol} on {ds} (skip MCP)')

    def _run(client: AlphaVantageMcpClient | None) -> None:
        if client is not None and pending:
            tools = client.list_tools()
            print(
                f'Alpha Vantage MCP ({transport}) connected; '
                f'{len(tools)} tools available'
            )
        for index, (symbol, meta) in enumerate(pending):
            raw = fetch_raw_commodity_payload(symbol, meta, client=client)
            write_bronze_snapshot(
                ds,
                symbol,
                function=raw['function'],
                params=raw['params'],
                payload=raw['payload'],
                source=raw['source'],
            )
            fetched.append(symbol)
            print(f"Bronze snapshot {symbol} via {raw['source']} ({raw['function']})")
            if index < len(pending) - 1 and pause > 0:
                time.sleep(pause)

    if not pending:
        pass
    elif transport == 'rest':
        _run(None)
    else:
        with AlphaVantageMcpClient() as client:
            _run(client)

    summary = {
        'trading_date': ds,
        'fetched': fetched,
        'reused': reused,
        'symbols': [symbol for symbol, _meta in symbols],
    }
    context['ti'].xcom_push(key='bronze_extract', value=summary)
    return summary


def transform_silver(**context):
    """Silver: validate bronze payloads (schema / nulls) and write cleaned tables."""
    init_warehouse()
    ds = context['ds']
    commodities = get_commodities()
    errors = []
    quotes = {}

    for symbol in commodities:
        try:
            bronze = load_bronze_snapshot(ds, symbol)
        except FileNotFoundError:
            errors.append(f'{symbol}: missing bronze snapshot')
            continue
        payload = bronze.get('payload')
        if not isinstance(payload, dict):
            errors.append(f'{symbol}: bronze payload is not an object')
            continue
        try:
            series_newest_first, unit = _parse_price_series(payload)
        except Exception as exc:  # noqa: BLE001 - collect per-symbol errors
            errors.append(f'{symbol}: {exc}')
            continue

        chrono = _chrono_prices(series_newest_first)
        quote = _latest_quote(series_newest_first, unit)
        quote['symbol'] = symbol
        quote['function'] = bronze.get('function')
        quote['interval'] = (bronze.get('params') or {}).get('interval', 'spot')
        quote['source'] = bronze.get('source') or _price_source()

        if quote['price'] is None or quote['price'] <= 0:
            errors.append(f'{symbol}: non-positive price ({quote["price"]})')
            continue
        if abs(quote['change_pct']) > 50:
            errors.append(f'{symbol}: extreme move {quote["change_pct"]}%')
            continue
        if any(point['price'] is None or point['price'] <= 0 for point in chrono):
            errors.append(f'{symbol}: null/non-positive observation in series')
            continue

        replace_silver_symbol(ds, symbol, quote=quote, series=chrono, unit=unit)
        quotes[symbol] = {
            'price': quote['price'],
            'unit': unit,
            'change_pct': quote['change_pct'],
            'as_of': quote['as_of'],
            'prior_as_of': quote.get('prior_as_of'),
            'interval': quote.get('interval'),
            'source': quote['source'],
            'history_points': len(chrono),
        }
        print(
            f"Silver {symbol}: {quote['price']} {unit} "
            f"({quote['change_pct']:+.3f}%) [{len(chrono)} pts]"
        )

    missing = set(commodities) - set(quotes)
    if missing:
        errors.append(f'Missing symbols: {sorted(missing)}')
    if errors:
        raise ValueError('Silver validation failed: ' + '; '.join(errors))

    context['ti'].xcom_push(key='validated_prices', value=quotes)
    return {'validated_count': len(quotes)}


def compute_gold(**context):
    """
    Gold: SQL aggregations from silver, plus weighted trading signals.

    Metrics (min/max/avg/period return) are computed in SQL so a BI tool can
    read gold.commodity_metrics. Indicator scores remain Python functions.
    """
    ds = context['ds']
    quotes = load_silver_quotes(ds)
    if not quotes:
        raise ValueError(f'No silver quotes for {ds}')
    metrics = compute_gold_metrics_sql(ds)
    weights = get_signal_weights()
    signals = {}

    print('Gold SQL metrics:', {row['symbol']: round(row['avg_price'] or 0, 4) for row in metrics})
    print('Signal weights:', {k: round(v, 4) for k, v in weights.items()})
    print(f'Thresholds: BUY>={BUY_SCORE_THRESHOLD}, SELL<={SELL_SCORE_THRESHOLD}')

    for symbol, quote in quotes.items():
        series = load_silver_series(ds, symbol)
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

    context['ti'].xcom_push(key='gold_metrics', value=metrics)
    context['ti'].xcom_push(key='trading_signals', value=signals)
    return {'metrics': metrics, 'signals': signals}


def score_obv_data_quality(**context):
    """Compare synthetic OBV against historical price trend lines; write gold."""
    ds = context['ds']
    quotes = load_silver_quotes(ds)
    scores = {}
    for symbol in quotes:
        series = load_silver_series(ds, symbol)
        prices = [point['price'] for point in series]
        quality = compute_obv_data_quality(prices)
        upsert_obv_quality(ds, symbol, quality)
        scores[symbol] = quality
        print(f"OBV quality {symbol}: {quality['quality_score']} | {quality['detail']}")
    context['ti'].xcom_push(key='obv_quality', value=scores)
    return scores


def generate_trade_orders(**context):
    """Turn BUY/SELL signals into notional trade orders."""
    commodities = get_commodities()
    signals = context['ti'].xcom_pull(task_ids='compute_gold', key='trading_signals')

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
    ds = context['ds']
    prices = load_silver_quotes(ds)
    signals = context['ti'].xcom_pull(task_ids='compute_gold', key='trading_signals')
    orders = context['ti'].xcom_pull(task_ids='generate_trade_orders', key='trade_orders') or []
    quality = load_obv_quality(ds)
    metrics = context['ti'].xcom_pull(task_ids='compute_gold', key='gold_metrics') or []

    buys = sum(1 for s in signals.values() if s['action'] == 'BUY')
    sells = sum(1 for s in signals.values() if s['action'] == 'SELL')
    holds = sum(1 for s in signals.values() if s['action'] == 'HOLD')
    total_notional = sum(o['notional_usd'] for o in orders)

    report = {
        'trading_date': ds,
        'price_source': _price_source(),
        'commodities_tracked': len(prices),
        'signals': {'BUY': buys, 'SELL': sells, 'HOLD': holds},
        'orders_generated': len(orders),
        'total_notional_usd': round(total_notional, 2),
        'model': 'weighted_indicators',
        'gold_metrics': metrics,
        'obv_quality': {symbol: row['quality_score'] for symbol, row in quality.items()},
    }

    print('=' * 60)
    print(f"Commodity Trading Daily Report — {report['trading_date']}")
    print(f"Price source: {_price_source()}")
    print('Layers: bronze MCP snapshot → silver quotes → gold SQL metrics')
    print('Decision model: weighted momentum + MA + RSI + MACD + OBV')
    print('=' * 60)
    print(f"Tracked: {report['commodities_tracked']} commodities")
    for symbol, quote in prices.items():
        signal = signals[symbol]
        q = quality.get(symbol) or {}
        print(
            f"  {symbol:<12} {quote['price']:>12} {quote['unit']:<28} "
            f"{quote['change_pct']:+.3f}%  as of {quote['as_of']}  "
            f"→ {signal['action']} (score={signal['weighted_score']:+.4f}, "
            f"obv_dq={q.get('quality_score')})"
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


def open_trading_session(**context):
    """Session open marker (replaces BashOperator to avoid shell dependency)."""
    print(f"Opening commodity trading session for {context['ds']} ({_price_source()})")
    return {'session': 'open', 'trading_date': context['ds']}


def close_trading_session(**context):
    """Session close marker."""
    print(f"Commodity trading session closed for {context['ds']}")
    return {'session': 'closed', 'trading_date': context['ds']}


with DAG(
    dag_id='commodity_trading_dag',
    default_args=default_args,
    description=(
        'Commodity medallion pipeline: bronze MCP snapshots, silver quotes, '
        'gold SQL metrics, OBV data-quality vs price trend'
    ),
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    tags=['commodity', 'trading', 'alpha-vantage', 'mcp', 'medallion', 'sample'],
) as dag:

    start = PythonOperator(
        task_id='start_trading_session',
        python_callable=open_trading_session,
    )

    bronze = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_bronze,
    )

    silver = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver,
    )

    gold = PythonOperator(
        task_id='compute_gold',
        python_callable=compute_gold,
    )

    obv_quality = PythonOperator(
        task_id='score_obv_data_quality',
        python_callable=score_obv_data_quality,
    )

    generate_orders = PythonOperator(
        task_id='generate_trade_orders',
        python_callable=generate_trade_orders,
    )

    publish_report = PythonOperator(
        task_id='publish_daily_report',
        python_callable=publish_daily_report,
    )

    end = PythonOperator(
        task_id='close_trading_session',
        python_callable=close_trading_session,
    )

    start >> bronze >> silver >> gold >> obv_quality >> generate_orders >> publish_report >> end
