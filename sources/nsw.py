"""
NSW property data — NSW Valuer General bulk property sales data.

Source: https://valuation.property.nsw.gov.au/embed/propertySalesInformation
Format: Yearly ZIPs (one per calendar year) + weekly ZIPs for the current year
        Each ZIP contains .DAT files (one per LGA), semicolon-delimited
Licence: Creative Commons Attribution 4.0 (CC BY 4.0)

We parse the index page to discover:
  - Yearly ZIPs for each year within the 24-month window
  - Weekly ZIPs for the current year within the 24-month window
All ZIPs are downloaded concurrently, then merged.

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

Data cached in-process for 24 hours; first request triggers downloads.
"""

import asyncio
import io
import re
import statistics
import time
import zipfile
from datetime import date, timedelta

import httpx

_INDEX_URL   = "https://valuation.property.nsw.gov.au/embed/propertySalesInformation"
_BASE_URL    = "https://www.valuergeneral.nsw.gov.au"

_LOOKBACK_DAYS = 365 * 2   # 24 months of sales for median calculation
_MIN_SALES     = 3          # minimum transactions to report a median
_DL_CONCURRENCY = 5         # max simultaneous ZIP downloads

_cache: dict = {}
_cache_ts: float = 0.0
_cache_lock = asyncio.Lock()

_HOUSE_PURPOSES  = {"RESIDENCE", "RURAL RESIDENTIAL", "HOUSE"}
_UNIT_PURPOSES   = {"UNIT", "FLAT", "APARTMENT", "STRATA UNIT"}
_HOUSE_NATURES   = {"R", "3"}
_VACANT_NATURES  = {"V"}
_CUTOFF: date    = date.today() - timedelta(days=_LOOKBACK_DAYS)


async def _discover_zip_urls() -> list[str]:
    """Parse the index page and return ZIP URLs relevant to the 24-month window."""
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(_INDEX_URL)
        r.raise_for_status()

    cutoff_year  = _CUTOFF.year
    current_year = date.today().year
    urls = []

    for href in re.findall(r'href="([^"]+\.zip)"', r.text):
        if not href.startswith("http"):
            href = _BASE_URL + href

        yearly = re.search(r'/yearly/(\d{4})\.zip', href)
        if yearly:
            year = int(yearly.group(1))
            if cutoff_year <= year < current_year:
                urls.append(href)
            continue

        weekly = re.search(r'/weekly/(\d{4})(\d{2})(\d{2})\.zip', href)
        if weekly:
            try:
                file_date = date(int(weekly.group(1)), int(weekly.group(2)), int(weekly.group(3)))
                if file_date >= _CUTOFF:
                    urls.append(href)
            except ValueError:
                pass

    print(f"  📥 NSW: {len(urls)} ZIP files to download")
    return urls


async def _fetch_zip(sem: asyncio.Semaphore, client: httpx.AsyncClient, url: str) -> bytes:
    async with sem:
        r = await client.get(url)
        r.raise_for_status()
        return r.content


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

        unit_num   = parts[4].strip()
        street_num = parts[5].strip()
        street     = parts[6].strip()
        address    = f"{unit_num}/{street_num} {street}".strip() if unit_num else f"{street_num} {street}".strip()

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
                record["land_area_sqm"] = int(area * 10000) if area_type == "H" else int(area)
        except ValueError:
            pass

        sales.setdefault(suburb_raw, []).append(record)

    return house, unit, sales


def _median(prices: list[int]) -> int | None:
    if len(prices) < _MIN_SALES:
        return None
    return int(statistics.median(prices))


async def _load() -> dict:
    urls = await _discover_zip_urls()
    if not urls:
        raise RuntimeError("No NSW ZIP URLs discovered from index page")

    sem = asyncio.Semaphore(_DL_CONCURRENCY)
    async with httpx.AsyncClient(timeout=300, follow_redirects=True) as client:
        zip_contents = await asyncio.gather(*[_fetch_zip(sem, client, u) for u in urls])

    all_house: dict[str, list[int]] = {}
    all_unit:  dict[str, list[int]] = {}
    all_sales: dict[str, list[dict]] = {}

    for content in zip_contents:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for name in zf.namelist():
                if not name.upper().endswith(".DAT"):
                    continue
                h, u, s = _parse_dat_bytes(zf.read(name))
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

    return {
        "suburb":            suburb,
        "state":             "NSW",
        "coverage":          "available",
        "total_sales_found": len(filtered),
        "comparable_sales":  filtered[:limit],
        "data_period":       data["data_period"],
        "data_source":       "NSW Valuer General — Bulk Property Sales Data (CC BY 4.0)",
    }
