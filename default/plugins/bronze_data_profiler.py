"""
Bronze-layer data profiler for commodity MCP/REST snapshots.

Runs after extract and before silver. Hard failures (empty / unparseable
payloads) raise BronzeProfileError. Sparse series, placeholder rates, and
duplicate dates are warnings so a free-tier / spot-fallback run still
completes.
"""

from __future__ import annotations

import os
from typing import Any

# Filename is unique on purpose: Airflow registers plugins by bare module name.

WARN_MIN_POINTS_DEFAULT = 5
WARN_PLACEHOLDER_RATE = 0.20


class BronzeProfileError(ValueError):
    """Raised when bronze profiles have blocking issues."""


def _warn_min_points() -> int:
    raw = os.environ.get('BRONZE_PROFILE_WARN_MIN_POINTS', str(WARN_MIN_POINTS_DEFAULT))
    try:
        return max(1, int(raw))
    except ValueError:
        return WARN_MIN_POINTS_DEFAULT


def fail_on_profile_error() -> bool:
    raw = os.environ.get('BRONZE_PROFILE_FAIL_ON_ERROR', 'true').strip().lower()
    return raw not in {'0', 'false', 'no', 'off'}


def _numeric_or_none(raw: Any) -> float | None:
    if raw is None or raw == '' or raw == '.':
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def profile_payload(symbol: str, payload: Any) -> dict:
    """
    Profile one Alpha Vantage-shaped JSON object.

    Commodity history uses data[].value; gold/silver history uses data[].price;
    spot quotes expose a top-level price.
    """
    issues: list[str] = []
    warnings: list[str] = []
    row_count = 0
    numeric_count = 0
    placeholder_count = 0
    invalid_count = 0
    duplicate_dates = 0
    dates: list[str] = []
    prices: list[float] = []
    schema_keys: list[str] = []

    if not isinstance(payload, dict):
        issues.append('payload is not an object')
        return _result(
            symbol,
            severity='error',
            issues=issues,
            warnings=warnings,
            row_count=0,
            numeric_count=0,
            placeholder_count=0,
            invalid_count=0,
            duplicate_dates=0,
            min_price=None,
            max_price=None,
            schema_keys=[],
        )

    schema_keys = sorted(payload.keys())

    if 'data' in payload:
        rows = payload.get('data')
        if not isinstance(rows, list):
            issues.append('data is not a list')
        else:
            seen: set[str] = set()
            for row in rows:
                row_count += 1
                if not isinstance(row, dict):
                    invalid_count += 1
                    continue
                date = str(row.get('date') or '')
                if date:
                    if date in seen:
                        duplicate_dates += 1
                    seen.add(date)
                    dates.append(date)
                raw = row.get('value', row.get('price'))
                if raw is None or raw == '' or raw == '.':
                    placeholder_count += 1
                    continue
                value = _numeric_or_none(raw)
                if value is None:
                    invalid_count += 1
                    continue
                numeric_count += 1
                prices.append(value)
    elif 'price' in payload:
        row_count = 1
        value = _numeric_or_none(payload.get('price'))
        if value is None:
            placeholder_count = 1
        else:
            numeric_count = 1
            prices.append(value)
        stamp = str(payload.get('timestamp') or '')
        if stamp:
            dates.append(stamp[:10])
    else:
        issues.append(
            f'unrecognized payload keys: {schema_keys or ["<empty>"]}'
        )

    if numeric_count == 0 and 'unrecognized payload keys' not in ' '.join(issues):
        issues.append('no numeric observations')

    if prices and any(p <= 0 for p in prices):
        warnings.append(f'{sum(1 for p in prices if p <= 0)} non-positive price(s)')

    if row_count and placeholder_count / row_count >= WARN_PLACEHOLDER_RATE:
        warnings.append(
            f'placeholder/missing rate {placeholder_count / row_count:.0%} '
            f'({placeholder_count}/{row_count})'
        )

    if duplicate_dates:
        warnings.append(f'{duplicate_dates} duplicate observation date(s)')

    if 0 < numeric_count < _warn_min_points():
        warnings.append(
            f'short series ({numeric_count} numeric point(s); '
            f'warn below {_warn_min_points()})'
        )

    severity = 'error' if issues else ('warn' if warnings else 'ok')
    return _result(
        symbol,
        severity=severity,
        issues=issues,
        warnings=warnings,
        row_count=row_count,
        numeric_count=numeric_count,
        placeholder_count=placeholder_count,
        invalid_count=invalid_count,
        duplicate_dates=duplicate_dates,
        min_price=min(prices) if prices else None,
        max_price=max(prices) if prices else None,
        schema_keys=schema_keys,
        date_min=min(dates) if dates else None,
        date_max=max(dates) if dates else None,
    )


def _result(
    symbol: str,
    *,
    severity: str,
    issues: list[str],
    warnings: list[str],
    row_count: int,
    numeric_count: int,
    placeholder_count: int,
    invalid_count: int,
    duplicate_dates: int,
    min_price: float | None,
    max_price: float | None,
    schema_keys: list[str],
    date_min: str | None = None,
    date_max: str | None = None,
) -> dict:
    return {
        'symbol': symbol,
        'severity': severity,
        'issues': issues,
        'warnings': warnings,
        'row_count': row_count,
        'numeric_count': numeric_count,
        'placeholder_count': placeholder_count,
        'invalid_count': invalid_count,
        'duplicate_dates': duplicate_dates,
        'min_price': min_price,
        'max_price': max_price,
        'schema_keys': schema_keys,
        'date_min': date_min,
        'date_max': date_max,
    }


def profile_bronze_record(symbol: str, bronze: dict | None) -> dict:
    """Profile a warehouse bronze record (or missing snapshot)."""
    if bronze is None:
        return _result(
            symbol,
            severity='error',
            issues=['missing bronze snapshot'],
            warnings=[],
            row_count=0,
            numeric_count=0,
            placeholder_count=0,
            invalid_count=0,
            duplicate_dates=0,
            min_price=None,
            max_price=None,
            schema_keys=[],
        )
    return profile_payload(symbol, bronze.get('payload'))


def summarize_profiles(profiles: list[dict]) -> dict:
    errors = [p for p in profiles if p.get('severity') == 'error']
    warns = [p for p in profiles if p.get('severity') == 'warn']
    return {
        'profiled': len(profiles),
        'ok': sum(1 for p in profiles if p.get('severity') == 'ok'),
        'warnings': warns,
        'errors': errors,
        'profiles': profiles,
    }


def assert_profiles_acceptable(summary: dict, *, fail_on_error: bool | None = None) -> None:
    """Raise BronzeProfileError on blocking issues when fail_on_error is true."""
    if fail_on_error is None:
        fail_on_error = fail_on_profile_error()
    errors = summary.get('errors') or []
    if fail_on_error and errors:
        details = '; '.join(
            f"{row['symbol']}: {', '.join(row.get('issues') or ['error'])}"
            for row in errors
        )
        raise BronzeProfileError(f'Bronze profile failed: {details}')
