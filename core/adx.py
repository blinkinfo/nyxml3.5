"""ADX (Average Directional Index) filter — Coinbase 5-min candles + Wilder's smoothing."""

from __future__ import annotations

import logging
from typing import Any

import httpx

import config as cfg

log = logging.getLogger(__name__)

# Coinbase public candles endpoint (no auth required)
_COINBASE_URL = "https://api.exchange.coinbase.com/products/BTC-USD/candles"


async def fetch_candles(count: int | None = None) -> list[dict[str, float]] | None:
    """Fetch recent 5-minute BTC-USD candles from Coinbase.

    Returns a list of dicts sorted OLDEST-first:
      [{"time": ..., "open": ..., "high": ..., "low": ..., "close": ...}, ...]

    Coinbase returns candles newest-first, so we reverse.
    We use explicit start/end params to guarantee Coinbase returns exactly
    cfg.ADX_CANDLE_COUNT (300) candles — the maximum the API allows.
    """
    import time as _time

    n = count or cfg.ADX_CANDLE_COUNT
    granularity = 300  # 5 minutes in seconds

    # Pin the time window so Coinbase returns exactly n candles.
    # end = now (unix epoch seconds), start = now - n * granularity
    end_ts = int(_time.time())
    start_ts = end_ts - n * granularity

    params = {
        "granularity": granularity,
        "start": start_ts,
        "end": end_ts,
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(_COINBASE_URL, params=params)
            resp.raise_for_status()
            raw = resp.json()
    except Exception:
        log.exception("Coinbase candle fetch failed")
        return None

    if not raw or not isinstance(raw, list):
        log.error("Coinbase returned empty or invalid response")
        return None

    # Coinbase format: [time, low, high, open, close, volume] — newest first
    # Take ALL returned candles (do NOT slice to n — we need all for warm-up)
    candles = []
    for row in raw:
        try:
            candles.append({
                "time": float(row[0]),
                "low":  float(row[1]),
                "high": float(row[2]),
                "open": float(row[3]),
                "close": float(row[4]),
            })
        except (IndexError, ValueError, TypeError):
            continue

    if len(candles) < n:
        log.warning(
            "Coinbase returned %d candles, requested %d — ADX may be less accurate",
            len(candles), n,
        )

    candles.reverse()  # oldest first
    return candles
def compute_adx(candles: list[dict[str, float]], length: int | None = None) -> list[float] | None:
    """Compute ADX series from candle data using Wilder's smoothing.

    Parameters
    ----------
    candles : list of dicts with keys "high", "low", "close" (oldest first)
    length  : ADX period (default: cfg.ADX_LENGTH, typically 14)

    Returns
    -------
    List of ADX values (one per candle, starting from index where ADX is first valid).
    Returns None if insufficient data.

    The algorithm:
    1. Compute True Range (TR), +DM, -DM for each candle pair.
    2. Apply Wilder's smoothing (length periods) to get smoothed TR, +DM, -DM.
    3. Compute +DI and -DI from smoothed values.
    4. Compute DX = |+DI - -DI| / (+DI + -DI) * 100.
    5. Apply Wilder's smoothing to DX to get ADX.
    """
    n = length or cfg.ADX_LENGTH

    min_candles = 3 * n  # need 3×period for ADX to meaningfully converge
    if len(candles) < min_candles:
        log.error(
            "Not enough candles for ADX(%d): have %d, need at least %d (3×period for convergence)",
            n, len(candles), min_candles,
        )
        return None

    # Step 1: TR, +DM, -DM
    tr_list: list[float] = []
    plus_dm_list: list[float] = []
    minus_dm_list: list[float] = []

    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]

        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_list.append(tr)

        up_move = high - candles[i - 1]["high"]
        down_move = candles[i - 1]["low"] - low

        plus_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm = down_move if (down_move > up_move and down_move > 0) else 0.0

        plus_dm_list.append(plus_dm)
        minus_dm_list.append(minus_dm)

    # Step 2: Wilder's smoothing for TR, +DM, -DM (first value = sum of first N)
    if len(tr_list) < n:
        return None

    smoothed_tr = sum(tr_list[:n])
    smoothed_plus_dm = sum(plus_dm_list[:n])
    smoothed_minus_dm = sum(minus_dm_list[:n])

    plus_di_list: list[float] = []
    minus_di_list: list[float] = []

    # First DI values
    plus_di = (smoothed_plus_dm / smoothed_tr * 100) if smoothed_tr != 0 else 0.0
    minus_di = (smoothed_minus_dm / smoothed_tr * 100) if smoothed_tr != 0 else 0.0
    plus_di_list.append(plus_di)
    minus_di_list.append(minus_di)

    # Continue Wilder's smoothing for remaining candles
    for i in range(n, len(tr_list)):
        smoothed_tr = smoothed_tr - (smoothed_tr / n) + tr_list[i]
        smoothed_plus_dm = smoothed_plus_dm - (smoothed_plus_dm / n) + plus_dm_list[i]
        smoothed_minus_dm = smoothed_minus_dm - (smoothed_minus_dm / n) + minus_dm_list[i]

        plus_di = (smoothed_plus_dm / smoothed_tr * 100) if smoothed_tr != 0 else 0.0
        minus_di = (smoothed_minus_dm / smoothed_tr * 100) if smoothed_tr != 0 else 0.0
        plus_di_list.append(plus_di)
        minus_di_list.append(minus_di)

    # Step 3: DX series
    dx_list: list[float] = []
    for pdi, mdi in zip(plus_di_list, minus_di_list):
        di_sum = pdi + mdi
        dx = (abs(pdi - mdi) / di_sum * 100) if di_sum != 0 else 0.0
        dx_list.append(dx)

    # Step 4: ADX — Wilder's smoothing of DX
    if len(dx_list) < n:
        return None

    adx = sum(dx_list[:n]) / n  # first ADX = SMA of first N DX values
    adx_list: list[float] = [adx]

    for i in range(n, len(dx_list)):
        adx = (adx * (n - 1) + dx_list[i]) / n
        adx_list.append(adx)

    return adx_list


async def get_adx_direction() -> dict[str, Any] | None:
    """Fetch candles, compute ADX(14), and determine if ADX is rising or falling.

    Returns
    -------
    dict with:
      - "direction": "rising" | "falling" | "flat"
      - "adx_current": float  (latest ADX value)
      - "adx_previous": float (second-to-last ADX value)
    or None on error.
    """
    candles = await fetch_candles()
    if candles is None:
        log.error("Cannot compute ADX — candle fetch failed")
        return None

    adx_series = compute_adx(candles)
    if adx_series is None or len(adx_series) < 2:
        log.error("Cannot compute ADX — insufficient data (got %s values)",
                  len(adx_series) if adx_series else 0)
        return None

    current = adx_series[-1]
    previous = adx_series[-2]

    if current > previous:
        direction = "rising"
    elif current < previous:
        direction = "falling"
    else:
        direction = "flat"  # treated as "falling" by strategy (keep signal as-is)

    log.info(
        "ADX(14): current=%.2f  previous=%.2f  direction=%s",
        current, previous, direction,
    )

    return {
        "direction": direction,
        "adx_current": round(current, 2),
        "adx_previous": round(previous, 2),
    }
