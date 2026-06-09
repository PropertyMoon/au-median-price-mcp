"""
NSW property data — NSW Valuer General bulk property sales data.

Source: https://valuation.property.nsw.gov.au/embed/propertySalesInformation
Format: ZIP of .DAT files (one per LGA), semicolon-delimited
Licence: Creative Commons Attribution 4.0 (CC BY 4.0)

We download the full weekly ZIP (~50-100 MB), parse all .DAT files,
filter to residential sales in the last 24 months, and compute:
  - Suburb median house/unit prices
  - Individual sale records for comparable sales lookups

.DAT record format (semicolon-delimited, one record per property):
  District code ; file type ; Valuation Num ; Property ID ; Unit ; Num ;
  Street ; Suburb ; PostCode ; Area ; AreaType ; Contract Date ;
  Settlement Date ; PurchasePrice ; ZoningCode ; NatureOfProperty ;
  PrimaryPurpose ; Strata Lot ; Component Code ; SaleCode ; Interest % ;
  Dealing Num

Relevant fields (0-based):
  4  = Unit number
  5  = Street number
  6  = Street name
  7  = Suburb
  8  = PostCode
  9  = Area
  10 = AreaType  (M=sqm, H=hectares)
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
_HOUSE_NATURES   = {"R", "3"}
_VACANT_NATURES  = {"V"}
_CUTOFF: date    = date.today() - timedelta(days=_LOOKBACK_DAYS)


def _parse_dat_bytes(raw: bytes) -> tuple[dict, dict, dict]:
    """Parse a single .DAT file; return (house_prices, unit_prices, sales_by_suburb)."""
    house: dict[str, list[int]] = {}
    unit:  dict[str, list[int]] = {}
    sales: dict[str, list[dict]] = {}

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

        is_house = (purpose in _HOUSE_PURPOSES or nature in _HOUSE_NATURES)
        is_unit  = purpose in _UNIT_PURPOSES
        is_land  = nature in _VACANT_NATURES

        if is_house and not is_unit:
            house.setdefault(suburb_raw, []).append(price)
            prop_type = "house"
        elif is_unit:
            unit.setdefault(suburb_raw, []).append(price)
            prop_type = "unit"
        elif is_land:
            prop_type = "land"
        else:
            prop_type = "other"

        # Build address
        unit_num   = parts[4].strip()
        street_num = parts[5].strip()
        street     = parts[6].strip()
        if unit_num:
            address = f"{unit_num}/{street_num} {street}".strip()
        else:
            address = f"{street_num} {street}".strip()

        postcode  = parts[8].strip() if len(parts) > 8 else ""
        area_raw  = parts[9].strip() if len(parts) > 9 else ""
        area_type = parts[10].strip().upper() if len(parts) > 10 else ""

        record: dict = {
            "address":       address,
            "postcode":      postcode,
            "sale_date":     contract_date.strftime("%Y-%m-%d"),
            "price":         price,
            "property_type": prop_type,
        }

        try:
            area = float(area_raw) if area_raw else None
            if area and area > 0:
                record["land_area_sqm"] = (
                    int(area * 10000) if area_type == "H" else int(area)
                )
        except ValueError:
            pass

        sales.setdefault(suburb_raw, []).append(record)

    return house, unit, sales


def _median(prices: list[int]) -> int | None:
    if len(prices) < _MIN_SALES:
        return None
    return int(statistics.median(prices))


async def _load() -> dict:
    async with httpx.AsyncClient(timeout=300, follow_redirects=True) as client:
        r = await client.get(_SALES_ZIP_URL)
        r.raise_for_status()

    all_house: dict[str, list[int]] = {}
    all_unit:  dict[str, list[int]] = {}
    all_sales: dict[str, list[dict]] = {}

    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        dat_files = [n for n in zf.namelist() if n.upper().endswith(".DAT")]
        for name in dat_files:
            raw = zf.read(name)
            h, u, s = _parse_dat_bytes(raw)
            for suburb, prices in h.items():
                all_house.setdefault(suburb, []).extend(prices)
            for suburb, prices in u.items():
                all_unit.setdefault(suburb, []).extend(prices)
            for suburb, records in s.items():
                all_sales.setdefault(suburb, []).extend(records)

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

    # Sort each suburb's sales by date descending (most recent first)
    for suburb in all_sales:
        all_sales[suburb].sort(key=lambda x: x["sale_date"], reverse=True)

    data_period = f"last 24 months to {date.today().strftime('%B %Y')}"
    print(f"  ✅ NSW: {len(house_medians)} house suburbs, {len(unit_medians)} unit suburbs, "
          f"{sum(len(v) for v in all_sales.values())} individual sales loaded")
    return {
        "house":       house_medians,
        "unit":        unit_medians,
        "sales":       all_sales,
        "data_period": data_period,
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


async def get_nsw_comparable_sales(
    suburb: str,
    property_type: str | None = None,
    min_price: int | None = None,
    max_price: int | None = None,
    limit: int = 20,
) -> dict:
    suburb_upper = suburb.strip().upper()
    data = await _data()

    records = data["sales"].get(suburb_upper, [])

    if not records:
        return {
            "suburb":   suburb,
            "state":    "NSW",
            "coverage": "no_data",
            "message":  f"No sales records found for '{suburb}' in NSW dataset.",
        }

    filtered = records
    if property_type:
        pt = property_type.strip().lower()
        filtered = [r for r in filtered if r["property_type"] == pt]
    if min_price:
        filtered = [r for r in filtered if r["price"] >= min_price]
    if max_price:
        filtered = [r for r in filtered if r["price"] <= max_price]

    total = len(filtered)

    return {
        "suburb":            suburb,
        "state":             "NSW",
        "coverage":          "available",
        "total_sales_found": total,
        "comparable_sales":  filtered[:limit],
        "data_period":       data["data_period"],
        "data_source":       "NSW Valuer General — Bulk Property Sales Data (CC BY 4.0)",
    }
