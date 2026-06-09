"""
NSW median prices — NSW Valuer General bulk property sales data.

Source: https://valuation.property.nsw.gov.au/embed/propertySalesInformation
Format: ZIP of .DAT files (one per LGA), pipe-delimited
Licence: Creative Commons Attribution 4.0 (CC BY 4.0)

We download the full weekly ZIP (~50-100 MB), parse all .DAT files,
filter to residential sales in the last 24 months, and compute suburb
median house/unit prices.

.DAT record format (semicolon-delimited, one record per property):
  District code ; file type ; Valuation Num ; Property ID ; Unit ; Num ;
  Street ; Suburb ; PostCode ; Area ; AreaType ; Contract Date ;
  Settlement Date ; PurchasePrice ; ZoningCode ; NatureOfProperty ;
  PrimaryPurpose ; Strata Lot ; Component Code ; SaleCode ; Interest % ;
  Dealing Num

Relevant fields (0-based):
  7  = Suburb
  11 = Contract Date  (YYYYMMDD)
  13 = PurchasePrice
  15 = NatureOfProperty  (V=Vacant Land, R=Residence, etc.)
  16 = PrimaryPurpose    (RESIDENCE, UNIT, etc.)

Data cached in-process for 24 hours; first request triggers ~50-100 MB download.
"""

import asyncio
import io
import statistics
import time
import zipfile
from datetime import date, timedelta

import httpx

_SALES_ZIP_URL = (
    "https://valuation.property.nsw.gov.au/embed/propertySalesInformation"
)
_LOOKBACK_DAYS = 365 * 2   # 24 months of sales for median calculation
_MIN_SALES     = 3          # minimum transactions to report a median

_cache: dict = {}
_cache_ts: float = 0.0
_cache_lock = asyncio.Lock()

_HOUSE_PURPOSES  = {"RESIDENCE", "RURAL RESIDENTIAL", "HOUSE"}
_UNIT_PURPOSES   = {"UNIT", "FLAT", "APARTMENT", "STRATA UNIT"}
_HOUSE_NATURES   = {"R", "3"}   # R = Residence
_CUTOFF: date    = date.today() - timedelta(days=_LOOKBACK_DAYS)


def _parse_dat_bytes(raw: bytes) -> tuple[dict[str, list], dict[str, list]]:
    """Parse a single .DAT file; return (house_prices, unit_prices) keyed by suburb."""
    house: dict[str, list] = {}
    unit:  dict[str, list] = {}

    text = raw.decode("latin-1", errors="replace")
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(";"):
            continue
        parts = line.split(";")
        if len(parts) < 17:
            continue

        suburb_raw = parts[7].strip().upper()
        if not suburb_raw:
            continue

        contract_date_raw = parts[11].strip()
        try:
            contract_date = date(
                int(contract_date_raw[:4]),
                int(contract_date_raw[4:6]),
                int(contract_date_raw[6:8]),
            )
        except (ValueError, IndexError):
            continue

        if contract_date < _CUTOFF:
            continue

        try:
            price = int(str(parts[13]).strip().replace(",", "") or 0)
        except (ValueError, TypeError):
            continue

        if price < 50_000 or price > 50_000_000:
            continue

        nature  = parts[15].strip().upper() if len(parts) > 15 else ""
        purpose = parts[16].strip().upper() if len(parts) > 16 else ""

        is_house = (
            purpose in _HOUSE_PURPOSES
            or nature in _HOUSE_NATURES
        )
        is_unit = purpose in _UNIT_PURPOSES

        if is_house and not is_unit:
            house.setdefault(suburb_raw, []).append(price)
        elif is_unit:
            unit.setdefault(suburb_raw, []).append(price)

    return house, unit


def _median(prices: list[int]) -> int | None:
    if len(prices) < _MIN_SALES:
        return None
    return int(statistics.median(prices))


async def _load() -> dict:
    async with httpx.AsyncClient(timeout=300, follow_redirects=True) as client:
        r = await client.get(_SALES_ZIP_URL)
        r.raise_for_status()

    all_house: dict[str, list] = {}
    all_unit:  dict[str, list] = {}

    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        dat_files = [n for n in zf.namelist() if n.upper().endswith(".DAT")]
        for name in dat_files:
            raw = zf.read(name)
            h, u = _parse_dat_bytes(raw)
            for suburb, prices in h.items():
                all_house.setdefault(suburb, []).extend(prices)
            for suburb, prices in u.items():
                all_unit.setdefault(suburb, []).extend(prices)

    house_medians: dict[str, int] = {}
    unit_medians:  dict[str, int] = {}

    for suburb, prices in all_house.items():
        m = _median(prices)
        if m:
            house_medians[suburb] = m

    for suburb, prices in all_unit.items():
        m = _median(prices)
        if m:
            unit_medians[suburb] = m

    data_period = f"last 24 months to {date.today().strftime('%B %Y')}"
    return {
        "house":        house_medians,
        "unit":         unit_medians,
        "data_period":  data_period,
    }


async def _data() -> dict:
    global _cache, _cache_ts
    async with _cache_lock:
        if _cache and (time.time() - _cache_ts) < 86_400:
            return _cache
        _cache = await _load()
        _cache_ts = time.time()
        return _cache


async def get_nsw_median(suburb: str) -> dict:
    suburb_upper = suburb.strip().upper()
    data = await _data()

    house = data["house"].get(suburb_upper)
    unit  = data["unit"].get(suburb_upper)

    if not house and not unit:
        return {
            "suburb":   suburb,
            "state":    "NSW",
            "coverage": "no_data",
            "message":  f"No sales records found for '{suburb}' in NSW Valuer General dataset.",
        }

    result: dict = {
        "suburb":      suburb,
        "state":       "NSW",
        "coverage":    "available",
        "data_period": data["data_period"],
        "data_source": "NSW Valuer General — Bulk Property Sales Data (CC BY 4.0)",
    }
    if house:
        result["median_house_price"] = house
    if unit:
        result["median_unit_price"] = unit

    return result
