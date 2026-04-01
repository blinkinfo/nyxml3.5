"""Coinbase candle resolver — determines 5-min slot winner (Up/Down) from BTC-USD candle data.

Instead of polling the Gamma API for market resolution, this module fetches
the single 5-minute candle from Coinbase covering the slot window and compares
close vs open:
  - close >= open  →  winner = "Up"
  - close <  open  →  winner = "Down"

resolve_slot() retries up to 3 times at 5-second intervals (15 s worst case).
check_resolution() performs a single attempt with no retries.
"""

from __future__ import annotations

import asyncio
import logging

import httpx

import config as cfg

log = logging.getLogger(__name__)

MAX_RETRIES = 3       # resolve_slot retries before giving up
RETRY_INTERVAL = 5    # seconds between retries


def _extract_slot_start_ts(slug: str) -> int:
    """Extract the slot-start unix timestamp from a slug.

    Slug format: "btc-updown-5m-{unix_ts}" (may have more segments).
    Always splits from the right to handle any prefix safely.
    """
    return int(slug.rsplit("-", 1)[-1])


async def _fetch_candle(slot_start_ts: int) -> tuple[float, float] | None:
    """Fetch the single 5-min candle for *slot_start_ts* from Coinbase.

    Returns (open, close) on success, or None if the candle is unavailable.
    """
    params = {
        "granularity": 300,
        "start": slot_start_ts,
        "end": slot_start_ts + 300,
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(cfg.COINBASE_CANDLE_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception:
        log.exception("Coinbase candle fetch failed for ts=%d", slot_start_ts)
        return None

    if not data or not isinstance(data, list) or len(data) == 0:
        log.warning("Coinbase returned no candle for ts=%d", slot_start_ts)
        return None

    # Coinbase format: [time, low, high, open, close, volume] — newest first
    try:
        candle = data[0]
        open_price = float(candle[3])
        close_price = float(candle[4])
        return open_price, close_price
    except (IndexError, ValueError, TypeError):
        log.exception("Failed to parse Coinbase candle for ts=%d", slot_start_ts)
        return None


async def check_resolution(slug: str) -> tuple[str | None, bool]:
    """Single-attempt resolution check via Coinbase candle data.

    Returns (winning_side, True) if the candle is available,
    or (None, False) if the data is not yet available.
    """
    slot_start_ts = _extract_slot_start_ts(slug)
    result = await _fetch_candle(slot_start_ts)

    if result is None:
        return None, False

    open_price, close_price = result
    winner = "Up" if close_price >= open_price else "Down"
    log.info("Slot %s resolved: winner=%s (open=%.2f, close=%.2f)", slug, winner, open_price, close_price)
    return winner, True


async def resolve_slot(slug: str) -> str | None:
    """Fetch the Coinbase candle for the slot, retrying up to MAX_RETRIES times.

    Retries at RETRY_INTERVAL-second intervals.  Worst case: 15 seconds
    (3 attempts × 5 s).  Returns the winning side ("Up" or "Down") or None
    if the candle could not be retrieved after all attempts.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        winner, resolved = await check_resolution(slug)
        if resolved:
            return winner
        log.debug("Slot %s not yet resolved (attempt %d/%d)", slug, attempt, MAX_RETRIES)
        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_INTERVAL)

    log.warning("Slot %s did not resolve after %d attempts", slug, MAX_RETRIES)
    return None
