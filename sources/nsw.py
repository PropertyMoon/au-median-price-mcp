"""
NSW property data — NSW Valuer General bulk property sales data.

Source: https://valuation.property.nsw.gov.au/embed/propertySalesInformation
Licence: Creative Commons Attribution 4.0 (CC BY 4.0)

Two DAT file formats exist:
  YEARLY  (__psi/yearly/YYYY.zip)  — pipe-separated, one property per line
    0:district  1:filetype  2:valnum  3:propID  4:unit  5:streetnum  6:street
    7:suburb  8:postcode  9:area  10:areatype  11:contractdate  12:settlementdate
    13:price  14:zone  15:nature  16:purpose  17:strata ...

  WEEKLY  (__psi/weekly/YYYYMMDD.zip)  — B-record format, record type in field 0
    A = file header, B = sale record, C = title ref, D = party info
    B record:
    0:B  1:district  2:propID  3:seq  4:timestamp  5:unitprefix  6:unitnum
    7:streetnum  8:street  9:suburb  10:postcode  11:area  12:areatype
    13:contractdate  14:settlementdate  15:price  16:zone  17:nature  18:purpose ...

Comparable sales radius search:
  - Uses pgeocode (offline AU postcode centroids, no API key)
  - Optionally geocodes input address via OSM Nominatim (1 call per unique address)
  - Filters sales to postcodes whose centroid is within radius_km of the input point

Data cached in-process for 24 hours.
"""

import asyncio
import io
import math
import re
import statistics
import time
import zipfile
from datetime import date, timedelta

import httpx

_INDEX_URL      = "https://valuation.property.nsw.gov.au/embed/propertySalesInformation"
_BASE_URL       = "https://www.valuergeneral.nsw.gov.au"
_NOMINATIM_URL  = "https://nominatim.openstreetmap.org/search"
_NOMINATIM_UA   = "au-median-price-mcp/1.0 (property research)"

_LOOKBACK_DAYS  = 365 * 2
_MIN_SALES      = 3
_DL_CONCURRENCY = 5

_cache: dict        = {}
_cache_ts: float    = 0.0
_cache_lock         = asyncio.Lock()
_geocode_cache: dict[str, tuple[float, float] | None] = {}

_HOUSE_PURPOSES = {"RESIDENCE", "RURAL RESIDENTIAL", "HOUSE"}
_UNIT_PURPOSES  = {"UNIT", "FLAT", "APARTMENT", "STRATA UNIT"}
_HOUSE_NATURES  = {"R", "3"}
_VACANT_NATURES = {"V"}
_CUTOFF: date   = date.today() - timedelta(days=_LOOKBACK_DAYS)

# pgeocode for offline AU postcode centroids — optional, degrades gracefully
try:
    import pgeocode as _pgeocode
    _PCODE_DB = _pgeocode.Nominatim("AU")
    _PGEOCODE_OK = True
except Exception:
    _PCODE_DB    = None
    _PGEOCODE_OK = False


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a = math.sin(Δφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _postcode_centroid(postcode: str) -> tuple[float, float] | None:
    if not _PGEOCODE_OK or _PCODE_DB is None:
        return None
    try:
        row = _PCODE_DB.query_postal_code(postcode)
        if row is not None and not math.isnan(float(row.latitude)):
            return (float(row.latitude), float(row.longitude))
    except Exception:
        pass
    return None


async def _geocode_address(address: str) -> tuple[float, float] | None:
    """Geocode a free-text address via OSM Nominatim. Results cached in-process."""
    if address in _geocode_cache:
        return _geocode_cache[address]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                _NOMINATIM_URL,
                params={"q": address, "format": "json", "limit": 1, "countrycodes": "au"},
                headers={"User-Agent": _NOMINATIM_UA},
            )
            r.raise_for_status()
            results = r.json()
            if results:
                coords = (float(results[0]["lat"]), float(results[0]["lon"]))
                _geocode_cache[address] = coords
                return coords
    except Exception as e:
        print(f"  ⚠️  Geocoding failed for '{address}': {e}")
    _geocode_cache[address] = None
    return None


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

async def _discover_zip_urls() -> list[str]:
    cutoff_year  = _CUTOFF.year
    current_year = date.today().year

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(_INDEX_URL)
        r.raise_for_status()

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


def _extract_record(parts: list[str]) -> dict | None:
    """
    Extract fields from one DAT line, handling both yearly and weekly B-record formats.
    Returns None if the line should be skipped.
    """
    if not parts or not parts[0]:
        return None

    rec_type = parts[0].strip().upper()

    if rec_type == "B":
        # Weekly B-record format
        if len(parts) < 19:
            return None
        return {
            "suburb":    parts[9].strip().upper(),
            "postcode":  parts[10].strip(),
            "area":      parts[11].strip(),
            "area_type": parts[12].strip().upper(),
            "date_raw":  parts[13].strip(),
            "price_raw": parts[15].strip(),
            "nature":    parts[17].strip().upper() if len(parts) > 17 else "",
            "purpose":   parts[18].strip().upper() if len(parts) > 18 else "",
            "strata":    parts[19].strip()          if len(parts) > 19 else "",
            "unit_num":  parts[6].strip(),
            "street_num":parts[7].strip(),
            "street":    parts[8].strip(),
        }
    elif rec_type in ("A", "C", "D", "E"):
        # Weekly non-data records
        return None
    else:
        # Yearly format — first field is district code (numeric)
        if len(parts) < 17:
            return None
        return {
            "suburb":    parts[7].strip().upper(),
            "postcode":  parts[8].strip(),
            "area":      parts[9].strip(),
            "area_type": parts[10].strip().upper(),
            "date_raw":  parts[11].strip(),
            "price_raw": parts[13].strip(),
            "nature":    parts[15].strip().upper() if len(parts) > 15 else "",
            "purpose":   parts[16].strip().upper() if len(parts) > 16 else "",
            "strata":    parts[17].strip()          if len(parts) > 17 else "",
            "unit_num":  parts[4].strip(),
            "street_num":parts[5].strip(),
            "street":    parts[6].strip(),
        }


def _parse_dat_bytes(raw: bytes) -> tuple[dict, dict, dict]:
    house: dict[str, list[int]] = {}
    unit:  dict[str, list[int]] = {}
    sales: dict[str, list[dict]] = {}

    text = raw.decode("latin-1", errors="replace")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(";")
        rec = _extract_record(parts)
        if rec is None:
            continue

        suburb_raw = rec["suburb"]
        if not suburb_raw:
            continue

        try:
            d = rec["date_raw"]
            contract_date = date(int(d[:4]), int(d[4:6]), int(d[6:8]))
        except (ValueError, IndexError):
            continue

        if contract_date < _CUTOFF:
            continue

        try:
            price = int(rec["price_raw"].replace(",", "") or 0)
        except (ValueError, TypeError):
            continue

        if price < 50_000 or price > 50_000_000:
            continue

        nature  = rec["nature"]
        purpose = rec["purpose"]
        strata  = rec.get("strata", "")

        is_strata = bool(strata)   # non-empty strata lot = villa/townhouse/apartment
        is_house  = (purpose in _HOUSE_PURPOSES or nature in _HOUSE_NATURES) and not is_strata
        is_unit   = purpose in _UNIT_PURPOSES or is_strata
        is_land   = nature in _VACANT_NATURES

        if is_house:
            house.setdefault(suburb_raw, []).append(price)
            prop_type = "house"
        elif is_unit:
            unit.setdefault(suburb_raw, []).append(price)
            prop_type = "unit"
        elif is_land:
            prop_type = "land"
        else:
            prop_type = "other"

        unit_num   = rec["unit_num"]
        street_num = rec["street_num"]
        street     = rec["street"]
        address    = f"{unit_num}/{street_num} {street}".strip() if unit_num else f"{street_num} {street}".strip()

        sale_rec: dict = {
            "address":       address,
            "suburb":        suburb_raw,
            "postcode":      rec["postcode"],
            "sale_date":     contract_date.strftime("%Y-%m-%d"),
            "price":         price,
            "property_type": prop_type,
        }

        try:
            area = float(rec["area"]) if rec["area"] else None
            if area and area > 0:
                sale_rec["land_area_sqm"] = int(area * 10000) if rec["area_type"] == "H" else int(area)
        except ValueError:
            pass

        sales.setdefault(suburb_raw, []).append(sale_rec)

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

    # Sort by date desc and build postcode index
    postcode_sales: dict[str, list[dict]] = {}
    for suburb in all_sales:
        all_sales[suburb].sort(key=lambda x: x["sale_date"], reverse=True)
        for rec in all_sales[suburb]:
            pc = rec.get("postcode", "")
            if pc:
                postcode_sales.setdefault(pc, []).append(rec)

    # Pre-compute postcode centroids for fast radius queries
    postcode_centroids: dict[str, tuple[float, float]] = {}
    if _PGEOCODE_OK:
        for pc in postcode_sales:
            c = _postcode_centroid(pc)
            if c:
                postcode_centroids[pc] = c

    data_period = f"last 24 months to {date.today().strftime('%B %Y')}"
    total_sales = sum(len(v) for v in all_sales.values())
    print(f"  ✅ NSW: {len(house_medians)} house suburbs, {len(unit_medians)} unit suburbs, "
          f"{total_sales} individual sales, {len(postcode_centroids)} postcode centroids loaded")
    return {
        "house":               house_medians,
        "unit":                unit_medians,
        "sales":               all_sales,
        "postcode_sales":      postcode_sales,
        "postcode_centroids":  postcode_centroids,
        "data_period":         data_period,
    }


async def _data() -> dict:
    global _cache, _cache_ts
    async with _cache_lock:
        if _cache and (time.time() - _cache_ts) < 86_400:
            return _cache
        _cache = await _load()
        _cache_ts = time.time()
        return _cache


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

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
    address: str | None = None,
    radius_km: float = 1.0,
) -> dict:
    data = await _data()

    if address:
        coords = await _geocode_address(address)
        if coords:
            lat, lon = coords
            centroids = data["postcode_centroids"]
            near_postcodes = {
                pc for pc, (plat, plon) in centroids.items()
                if _haversine_km(lat, lon, plat, plon) <= radius_km
            }
            records: list[dict] = []
            for pc in near_postcodes:
                records.extend(data["postcode_sales"].get(pc, []))
            records.sort(key=lambda x: x["sale_date"], reverse=True)
            search_desc = f"{radius_km}km radius of '{address}'"
        else:
            # Geocoding failed — fall back to suburb
            records = data["sales"].get(suburb.strip().upper(), [])
            search_desc = f"suburb '{suburb}' (geocoding failed)"
    else:
        records = data["sales"].get(suburb.strip().upper(), [])
        search_desc = f"suburb '{suburb}'"

    if not records:
        return {
            "suburb":   suburb,
            "state":    "NSW",
            "coverage": "no_data",
            "message":  f"No sales records found for {search_desc}.",
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
        "search_area":       search_desc,
        "total_sales_found": len(filtered),
        "comparable_sales":  filtered[:limit],
        "data_period":       data["data_period"],
        "data_source":       "NSW Valuer General — Bulk Property Sales Data (CC BY 4.0)",
    }
