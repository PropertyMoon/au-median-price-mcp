"""
Scrapfly-based comparable sales scraper — REA + Domain.

Used as fallback for states not covered by open government datasets
(i.e. everything except NSW which has the Valuer General bulk data).

Credit design:
  - No render_js: REA and Domain embed listing data in server-rendered HTML.
  - soldIn=12 on REA, dateRange[min] on Domain: limits results to last 12 months.
  - REA + Domain fetched in parallel (asyncio.gather) to keep total latency under 20s.
  - Same-street search omitted — suburb-level gives enough comparables and
    same-street rarely returns results for smaller suburbs.
"""

import asyncio
import datetime
import json
import os
import re

import httpx
from dotenv import load_dotenv

load_dotenv()

_SCRAPFLY_KEY = os.getenv("SCRAPFLY_API_KEY", "")
_SCRAPFLY_URL = "https://api.scrapfly.io/scrape"


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

async def _fetch(url: str) -> str | None:
    if not _SCRAPFLY_KEY:
        return None
    params = {
        "key":     _SCRAPFLY_KEY,
        "url":     url,
        "asp":     "true",
        "country": "au",
    }
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(_SCRAPFLY_URL, params=params)
        r.raise_for_status()
        data = r.json()
        result = data.get("result", {})
        sc = result.get("status_code", 0)
        if sc == 200:
            print(f"  Scrapfly OK ({len(result.get('content',''))} chars): {url[:80]}")
            return result.get("content")
        print(f"  Scrapfly status {sc}: {url[:80]}")
        return None
    except Exception as e:
        print(f"  Scrapfly error ({e}): {url[:80]}")
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_price(raw) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        p = int(raw)
        return p if p > 50_000 else None
    clean = re.sub(r"[^\d]", "", str(raw))
    if not clean:
        return None
    try:
        p = int(clean)
        return p if p > 50_000 else None
    except ValueError:
        return None


def _parse_date(raw) -> str | None:
    if not raw:
        return None
    s = str(raw)
    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12:
            return f"{months[month - 1]} {year}"
    m2 = re.match(r"([A-Za-z]+)\s+(\d{4})", s)
    if m2:
        return f"{m2.group(1)} {m2.group(2)}"
    return s


def _year_from_date(date_str: str | None) -> int | None:
    if not date_str:
        return None
    m = re.search(r"\b(\d{4})\b", date_str)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# REA parser — data lives in window.ArgonautExchange
# ---------------------------------------------------------------------------

def _looks_like_listing(item: dict) -> bool:
    """True if this dict has any key that suggests it's a property listing."""
    keys = set(item.keys())
    return bool(keys & {"listing", "propertyDetails", "dateSold", "priceDetails",
                        "address", "price", "soldPrice", "beds", "bedrooms"})


def _find_listings_recursive(obj, depth: int = 0) -> list | None:
    """Walk ArgonautExchange until we find a non-empty list of listing-like dicts."""
    if depth > 8:
        return None
    if isinstance(obj, list) and len(obj) >= 1:
        if isinstance(obj[0], dict) and _looks_like_listing(obj[0]):
            return obj
    if isinstance(obj, dict):
        for v in obj.values():
            result = _find_listings_recursive(v, depth + 1)
            if result is not None:
                return result
    return None


def _parse_rea(html: str) -> list[dict]:
    results = []

    # Extract ArgonautExchange JSON — try two patterns to handle minified vs formatted
    m = re.search(r"window\.ArgonautExchange\s*=\s*(\{.+?\});\s*(?:window\.|</script>)", html, re.DOTALL)
    if not m:
        m = re.search(r"window\.ArgonautExchange\s*=\s*(\{.+)", html)
    if not m:
        print("  REA: ArgonautExchange not found in page")
        return []

    raw_json = m.group(1)
    # If greedy match captured too much, truncate at the last balanced brace
    # (fast sanity: try parse as-is first, then trim)
    data = None
    for candidate in (raw_json, raw_json.rsplit("};", 1)[0] + "}"):
        try:
            data = json.loads(candidate)
            break
        except json.JSONDecodeError:
            continue
    if data is None:
        print("  REA: ArgonautExchange JSON parse failed")
        return []

    # Log top-level keys to help diagnose future structure changes
    print(f"  REA ArgonautExchange keys: {list(data.keys())[:6]}")

    # First try the known paths, then fall back to recursive search
    listings_raw: list | None = None
    for value in data.values():
        if not isinstance(value, dict):
            continue
        d = value.get("data", {})
        if not isinstance(d, dict):
            continue
        for subkey in ("results", "listings", "data", "listingResponseData",
                       "propertySaleActivityResults", "soldResults"):
            candidate = d.get(subkey)
            if isinstance(candidate, list) and candidate:
                listings_raw = candidate
                print(f"  REA: found listings via .data.{subkey} ({len(listings_raw)} items)")
                break
        if listings_raw:
            break

    if not listings_raw:
        listings_raw = _find_listings_recursive(data)
        if listings_raw:
            print(f"  REA: found listings via recursive search ({len(listings_raw)} items)")
        else:
            print("  REA: no listings found — structure unknown, skipping REA")
            return []

    for item in listings_raw:
        try:
            listing = item.get("listing", item)
            pd = listing.get("propertyDetails", listing)

            parts = [
                str(pd.get("streetNumberDisplay") or pd.get("streetNumber") or ""),
                str(pd.get("street") or ""),
                str(pd.get("suburb") or ""),
                str(pd.get("state") or ""),
                str(pd.get("postcode") or ""),
            ]
            address = " ".join(p for p in parts if p.strip())
            if not address.strip():
                continue

            price_data = listing.get("priceDetails") or {}
            price = _parse_price(
                price_data.get("price") or price_data.get("soldPrice") or listing.get("price")
            )

            date_data = listing.get("dateSold") or {}
            date_str = _parse_date(
                date_data.get("date") or date_data.get("dateDisplay") or listing.get("soldDate")
            )

            land_raw = pd.get("landArea") or pd.get("land")
            results.append({
                "address":    address.strip(),
                "sale_price": price,
                "sale_date":  date_str,
                "bedrooms":   pd.get("bedrooms") or pd.get("beds"),
                "bathrooms":  pd.get("bathrooms") or pd.get("baths"),
                "land_sqm":   int(land_raw) if land_raw else None,
                "source":     "rea",
            })
        except Exception as e:
            print(f"  REA item error: {e}")

    print(f"  REA: {len(results)} listings extracted")
    return results


# ---------------------------------------------------------------------------
# Domain parser — data lives in __NEXT_DATA__
# ---------------------------------------------------------------------------

def _parse_domain(html: str) -> list[dict]:
    results = []
    pat = r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>'
    m = re.search(pat, html, re.DOTALL)
    if not m:
        print("  Domain: __NEXT_DATA__ not found")
        return []

    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError as e:
        print(f"  Domain: JSON parse error: {e}")
        return []

    page_props = data.get("props", {}).get("pageProps", {})
    listings_map = None
    for path in [
        ["componentProps", "listingsMap"],
        ["componentProps", "listings"],
        ["listingsMap"],
        ["listings"],
        ["searchResults", "listings"],
        ["searchResults", "results"],
    ]:
        node = page_props
        for key in path:
            node = node.get(key, {}) if isinstance(node, dict) else {}
        if node:
            listings_map = node
            break

    if not listings_map:
        print("  Domain: no listings in __NEXT_DATA__")
        return []

    items = listings_map.values() if isinstance(listings_map, dict) else listings_map
    for item in items:
        try:
            addr = item.get("address") or {}
            address_str = (
                addr.get("fullAddress") or
                (addr.get("display") or {}).get("fullAddress") or ""
            )
            if not address_str:
                parts = [
                    str(addr.get("streetNumber") or ""),
                    str(addr.get("street") or ""),
                    str(addr.get("suburb") or ""),
                    str(addr.get("state") or ""),
                    str(addr.get("postcode") or ""),
                ]
                address_str = " ".join(p for p in parts if p.strip())
            if not address_str.strip():
                continue

            price_detail = item.get("price") or item.get("soldDetails") or {}
            price = _parse_price(
                price_detail.get("displayPrice") or
                price_detail.get("value") or
                item.get("soldPrice")
            )

            date_str = _parse_date(
                item.get("soldDate") or
                item.get("dateSold") or
                (item.get("soldDetails") or {}).get("soldDate")
            )

            features = item.get("features") or item.get("propertyFeatures") or {}
            land_raw = features.get("landArea") or item.get("landArea")

            results.append({
                "address":    address_str.strip(),
                "sale_price": price,
                "sale_date":  date_str,
                "bedrooms":   features.get("bedrooms") or item.get("bedrooms"),
                "bathrooms":  features.get("bathrooms") or item.get("bathrooms"),
                "land_sqm":   int(land_raw) if land_raw else None,
                "source":     "domain",
            })
        except Exception as e:
            print(f"  Domain item error: {e}")

    print(f"  Domain: {len(results)} listings extracted")
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def get_scraper_comparable_sales(
    suburb: str,
    state: str,
    postcode: str,
    street: str | None = None,   # kept for signature compat, not used (saves 1 credit)
) -> dict:
    """
    Fetch comparable sold properties from REA + Domain via Scrapfly.
    Fetches REA and Domain in parallel to keep total latency ~10-15s.
    """
    if not _SCRAPFLY_KEY:
        return {
            "suburb":   suburb,
            "state":    state,
            "coverage": "unavailable",
            "message":  "SCRAPFLY_API_KEY not configured.",
        }

    now        = datetime.datetime.now()
    cur_yr     = now.year
    prv_yr     = now.year - 1
    cutoff     = datetime.date(prv_yr, 7, 1)
    domain_min = cutoff.strftime("%Y-%m-%d")

    suburb_rea    = suburb.lower().replace(" ", "+")
    suburb_domain = suburb.lower().replace(" ", "-")
    state_lower   = state.lower()

    rea_url = (
        f"https://www.realestate.com.au/sold/in-{suburb_rea}+"
        f"{state_lower}+{postcode}/list-1"
        f"?includeSurrounding=false&source=refinement&soldIn=12"
    )
    domain_url = (
        f"https://www.domain.com.au/sold-listings/{suburb_domain}-{state_lower}-{postcode}/"
        f"?excludepricewithheld=1&ssubs=0&dateRange[min]={domain_min}"
    )

    # Fetch both in parallel — total latency = max(rea, domain) instead of sum
    rea_html, domain_html = await asyncio.gather(
        _fetch(rea_url),
        _fetch(domain_url),
        return_exceptions=False,
    )

    all_results: list[dict] = []

    if rea_html:
        for r in _parse_rea(rea_html):
            r["proximity_tier"] = "same_suburb"
            all_results.append(r)

    if domain_html:
        for r in _parse_domain(domain_html):
            r["proximity_tier"] = "same_suburb"
            all_results.append(r)

    # Deduplicate by normalised address
    seen: set[str] = set()
    deduped: list[dict] = []
    for r in all_results:
        key = re.sub(r"\s+", " ", r["address"].lower().strip())
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    # Date guard — drop anything older than cutoff
    filtered = [
        r for r in deduped
        if _year_from_date(r.get("sale_date")) in (cur_yr, prv_yr, None)
    ]

    if not filtered:
        return {
            "suburb":   suburb,
            "state":    state,
            "coverage": "no_data",
            "message":  f"No comparable sales found for {suburb} {state} {postcode} in the last 12 months.",
        }

    return {
        "suburb":            suburb,
        "state":             state,
        "postcode":          postcode,
        "coverage":          "available",
        "total_sales_found": len(filtered),
        "comparable_sales":  filtered,
        "data_period":       f"July {prv_yr} – present",
        "data_source":       "realestate.com.au + domain.com.au (via Scrapfly)",
    }
