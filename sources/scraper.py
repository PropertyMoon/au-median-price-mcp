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
    """
    Fetch via Scrapfly. Tries without ASP first (cheaper / avoids 422 on
    restricted plans); retries with asp=true if the target page returns a
    bot-detection status (403/503/429). Logs the full Scrapfly error body
    on API-level failures so we can diagnose plan/credit issues.
    """
    if not _SCRAPFLY_KEY:
        return None

    for attempt, use_asp in enumerate((False, True), start=1):
        params: dict = {
            "key":     _SCRAPFLY_KEY,
            "url":     url,
            "country": "au",
        }
        if use_asp:
            params["asp"] = "true"

        label = "asp" if use_asp else "no-asp"
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                r = await client.get(_SCRAPFLY_URL, params=params)
        except Exception as e:
            raw = f"{type(e).__name__}: {str(e)}"
            err = raw.replace(_SCRAPFLY_KEY, "scp-***") if _SCRAPFLY_KEY else raw
            print(f"  Scrapfly network error ({label}, attempt {attempt}): {err} — {url[:80]}")
            if not use_asp:
                continue
            return None

        # Scrapfly API returned a non-200 HTTP status — log the body for diagnosis
        if r.status_code != 200:
            try:
                body = r.json()
                msg = (body.get("message") or body.get("error")
                       or body.get("description") or json.dumps(body)[:300])
            except Exception:
                msg = r.text[:300]
            msg = msg.replace(_SCRAPFLY_KEY, "scp-***") if _SCRAPFLY_KEY else msg
            print(f"  Scrapfly {r.status_code} ({label}, attempt {attempt}): {msg} — {url[:80]}")
            # 422 with ASP = plan restriction; 422 without ASP = retry with ASP
            if not use_asp:
                continue
            return None

        data   = r.json()
        result = data.get("result", {})
        sc     = result.get("status_code", 0)

        if sc == 200:
            print(f"  Scrapfly OK ({len(result.get('content', ''))} chars, {label}): {url[:80]}")
            return result.get("content")

        # Bot-detection from the target site — retry with ASP if we haven't yet
        print(f"  Scrapfly target-status {sc} ({label}): {url[:80]}")
        if sc in (403, 503, 429) and not use_asp:
            continue
        return None

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


def _date_from_tag_text(tag_text: str | None) -> str | None:
    """Extract sale date from Domain tag like 'Sold by private treaty 14 May 2026'."""
    if not tag_text:
        return None
    m = re.search(
        r'\b(\d{1,2})\s+(January|February|March|April|May|June|'
        r'July|August|September|October|November|December)\s+(\d{4})\b',
        tag_text,
    )
    return f"{m.group(2)} {m.group(3)}" if m else None


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
            # Domain now wraps all listing data inside "listingModel"; fall back to item itself
            # for older structures that stored fields directly on the item.
            lm = item.get("listingModel") or item

            # Skip non-sold items — Domain's sold-listings page can include active
            # for-sale listings. Require either tagClassName or tagText to contain
            # "sold". Items with no tags at all are kept (fallback for older structure).
            tags     = lm.get("tags") or {}
            tag_cls  = tags.get("tagClassName", "").lower()
            tag_text = tags.get("tagText", "").lower()
            if (tag_cls or tag_text) and "sold" not in tag_cls and "sold" not in tag_text:
                continue

            addr = lm.get("address") or item.get("address") or {}
            address_str = (
                addr.get("fullAddress") or
                (addr.get("display") or {}).get("fullAddress") or ""
            )
            if not address_str:
                # New structure: addr["street"] already includes the street number.
                # Old structure: addr["streetNumber"] + addr["street"] are separate.
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

            # Price: new structure stores a display string at lm["price"] (e.g. "$1,222,500").
            # Old structure stores a dict with displayPrice/value.
            price_raw = lm.get("price") or lm.get("soldDetails") or item.get("soldPrice")
            if isinstance(price_raw, dict):
                price_raw = price_raw.get("displayPrice") or price_raw.get("value")
            price = _parse_price(price_raw)

            # Date: new structure stores "Sold by private treaty 14 May 2026" in
            # lm["tags"]["tagText"]. Old structure had an ISO date in soldDate/dateSold.
            tag_text = (lm.get("tags") or {}).get("tagText", "")
            date_str = _parse_date(
                lm.get("soldDate") or
                lm.get("dateSold") or
                (lm.get("soldDetails") or {}).get("soldDate") or
                item.get("soldDate")
            ) or _date_from_tag_text(tag_text)

            features = lm.get("features") or item.get("features") or item.get("propertyFeatures") or {}
            land_raw = (
                features.get("landSize") or features.get("landArea") or
                lm.get("landArea") or item.get("landArea")
            )

            results.append({
                "address":    address_str.strip(),
                "sale_price": price,
                "sale_date":  date_str,
                "bedrooms":   features.get("beds") or features.get("bedrooms") or item.get("bedrooms"),
                "bathrooms":  features.get("baths") or features.get("bathrooms") or item.get("bathrooms"),
                "land_sqm":   int(land_raw) if land_raw else None,
                "source":     "domain",
            })
        except Exception as e:
            print(f"  Domain item error: {e}")

    print(f"  Domain: {len(results)} listings extracted")
    return results


# ---------------------------------------------------------------------------
# Domain suburb profile — median price + rental yield
# ---------------------------------------------------------------------------

def _parse_domain_next_data(html: str) -> dict | None:
    """Extract and JSON-parse __NEXT_DATA__ from a Domain Next.js page."""
    m = re.search(r'<script\s+id="__NEXT_DATA__"[^>]*>([^<]+)</script>', html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def _extract_price_history(page_props: dict) -> list[dict]:
    """
    Try multiple known paths for year-by-year price history in Domain's
    suburb profile __NEXT_DATA__. Returns list of {year, median_house_price}.
    """
    candidates: list = []

    # Path 1: suburbInsights.priceHistory or suburbInsights.medians.house.priceHistory
    si = page_props.get("suburbInsights") or {}
    for path in [
        si.get("priceHistory"),
        (si.get("medians") or {}).get("house", {}).get("priceHistory"),
        (si.get("medians") or {}).get("houses", {}).get("priceHistory"),
        si.get("historicalData"),
        si.get("historicalPrices"),
    ]:
        if isinstance(path, list) and path:
            candidates = path
            break

    # Path 2: componentProps sub-sections
    if not candidates:
        comp = page_props.get("componentProps") or {}
        for section_key in ("priceHistory", "historicalSales", "historicalData",
                             "medianPriceHistory", "suburbHistory"):
            raw = comp.get(section_key) or page_props.get(section_key)
            if isinstance(raw, list) and raw:
                candidates = raw
                break
            if isinstance(raw, dict):
                # might be {house: [...], unit: [...]}
                inner = raw.get("house") or raw.get("houses") or []
                if isinstance(inner, list) and inner:
                    candidates = inner
                    break

    if not candidates:
        return []

    points: list[dict] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        yr = (item.get("year") or item.get("financialYear")
              or item.get("calendarYear") or item.get("period"))
        price = (item.get("medianSalePrice") or item.get("soldMedianPrice")
                 or item.get("medianPrice") or item.get("price")
                 or item.get("median"))
        if yr is None or price is None:
            continue
        try:
            year_int = int(str(yr)[:4])
            price_int = _parse_price(price)
        except (ValueError, TypeError):
            continue
        if price_int and 2018 <= year_int <= 2030:
            points.append({"year": year_int, "median_house_price": price_int})

    # Deduplicate and sort, keep last 6
    seen: set = set()
    unique = []
    for p in sorted(points, key=lambda x: x["year"]):
        if p["year"] not in seen:
            seen.add(p["year"])
            unique.append(p)
    return unique[-6:]


async def scrape_domain_suburb_median(suburb: str, state: str, postcode: str) -> dict | None:
    """
    Scrape domain.com.au/suburb-profile for median prices, gross rental yield,
    and year-by-year price history (last 6 years).

    Returns a dict with coverage="available" using the same field names as the
    VIC/NSW MCP sources. Returns None if fetch or parse fails.
    """
    if not _SCRAPFLY_KEY:
        return None

    suburb_slug = suburb.lower().replace(" ", "-")
    state_lower = state.lower()
    url = f"https://www.domain.com.au/suburb-profile/{suburb_slug}-{state_lower}-{postcode}"

    html = await _fetch(url)
    if not html:
        return None

    nd = _parse_domain_next_data(html)
    if not nd:
        print(f"  Domain suburb profile: __NEXT_DATA__ not found for {suburb} {state}")
        return None

    page_props = nd.get("props", {}).get("pageProps", {})

    median_house  = None
    median_unit   = None
    gross_yield   = None
    data_period   = None

    # --- Path 1: suburbInsights.medians ---
    si      = page_props.get("suburbInsights") or {}
    medians = si.get("medians") or {}
    for h_key in ("house", "houses"):
        h = medians.get(h_key) or {}
        p = h.get("medianSalePrice") or h.get("soldMedianPrice") or h.get("price")
        if p:
            median_house = p
            gross_yield  = h.get("grossYield") or h.get("yield") or h.get("rentalYield")
            data_period  = h.get("period") or si.get("dataPeriod") or si.get("period")
            break
    for u_key in ("unit", "units"):
        u = medians.get(u_key) or {}
        p = u.get("medianSalePrice") or u.get("soldMedianPrice") or u.get("price")
        if p:
            median_unit = p
            break

    # --- Path 2: componentProps sub-sections ---
    if not median_house:
        comp = page_props.get("componentProps") or {}
        for section_key in ("suburbStatistics", "marketInsights", "statistics",
                             "suburbProfile", "insights", "marketTrends"):
            section = comp.get(section_key) or page_props.get(section_key) or {}
            h = section.get("house") or section.get("houses") or {}
            if not isinstance(h, dict):
                h = section
            p = (h.get("medianSalePrice") or h.get("soldMedianPrice")
                 or h.get("medianPrice") or h.get("price"))
            if p:
                median_house = p
                gross_yield  = h.get("grossYield") or h.get("yield") or h.get("rentalYield")
                data_period  = section.get("period") or section.get("dataPeriod")
                u = section.get("unit") or section.get("units") or {}
                median_unit  = (u.get("medianSalePrice") or u.get("soldMedianPrice")
                                or u.get("price"))
                break

    # --- Path 3: flat fields on pageProps ---
    if not median_house:
        for key in ("medianHousePrice", "houseMedSalePrice", "houseMedianPrice"):
            if page_props.get(key):
                median_house = page_props[key]
                break
    if not median_unit:
        for key in ("medianUnitPrice", "unitMedSalePrice", "unitMedianPrice"):
            if page_props.get(key):
                median_unit = page_props[key]
                break

    if not median_house and not median_unit:
        comp_keys = list((page_props.get("componentProps") or {}).keys())[:15]
        print(
            f"  Domain suburb profile: no median found for {suburb} {state}. "
            f"pageProps keys={list(page_props.keys())[:15]} "
            f"componentProps keys={comp_keys}"
        )
        return None

    hp            = _parse_price(median_house)
    up            = _parse_price(median_unit) if median_unit else None
    price_history = _extract_price_history(page_props)

    result: dict = {
        "suburb":      suburb,
        "state":       state,
        "coverage":    "available",
        "data_period": data_period or "last 12 months",
        "data_source": "domain.com.au suburb profile (via Scrapfly)",
    }
    if hp:
        result["median_house_price"] = hp
    if up:
        result["median_unit_price"] = up
    if gross_yield is not None:
        try:
            result["gross_rental_yield"] = float(gross_yield)
        except (TypeError, ValueError):
            pass
    if price_history:
        result["price_history_5yr"] = price_history

    print(
        f"  Domain suburb profile: {suburb} {state} — "
        f"house=${hp:,} unit={f'${up:,}' if up else 'n/a'} "
        f"yield={gross_yield} history={len(price_history)} pts"
    )
    return result


# ---------------------------------------------------------------------------
# Debug helper — Domain suburb profile __NEXT_DATA__ structure
# ---------------------------------------------------------------------------

async def debug_domain_suburb_profile(suburb: str, state: str, postcode: str) -> dict:
    """
    Diagnostic endpoint: fetch Domain suburb profile and return __NEXT_DATA__
    structure summary so parser paths can be verified/fixed.
    """
    suburb_slug = suburb.lower().replace(" ", "-")
    state_lower = state.lower()
    url = f"https://www.domain.com.au/suburb-profile/{suburb_slug}-{state_lower}-{postcode}"

    html = await _fetch(url)
    if not html:
        return {"error": "fetch failed", "url": url}

    nd = _parse_domain_next_data(html)
    if not nd:
        return {
            "error": "__NEXT_DATA__ not found",
            "url": url,
            "html_length": len(html),
            "html_preview": html[:500],
        }

    page_props = nd.get("props", {}).get("pageProps", {})

    def _summarise(obj, depth: int = 0):
        if depth > 3:
            return "…"
        if isinstance(obj, dict):
            return {k: _summarise(v, depth + 1) for k, v in list(obj.items())[:12]}
        if isinstance(obj, list):
            if not obj:
                return []
            first = _summarise(obj[0], depth + 1)
            return [first, f"…({len(obj)} items)"] if len(obj) > 1 else [first]
        return obj

    return {
        "url":               url,
        "page_props_keys":   list(page_props.keys()),
        "page_props_summary": _summarise(page_props),
        "price_history_attempt": _extract_price_history(page_props),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def get_scraper_comparable_sales(
    suburb: str,
    state: str,
    postcode: str,
    street: str | None = None,
) -> dict:
    """
    Fetch comparable sold properties via Scrapfly.

    Currently Domain-only. REA disabled — migrated to GraphQL/urql cache
    which requires a new parser; re-enable once fixed.

    Same-street properties (when street is provided) are sorted first.
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

    # REA disabled — ArgonautExchange now contains urqlClientCache (GraphQL),
    # parser needs rewriting before REA can be re-enabled.
    # suburb_rea = suburb.lower().replace(" ", "+")
    # rea_url = (
    #     f"https://www.realestate.com.au/sold/in-{suburb_rea}+"
    #     f"{state_lower}+{postcode}/list-1"
    #     f"?includeSurrounding=false&source=refinement&soldIn=12"
    # )

    suburb_domain = suburb.lower().replace(" ", "-")
    state_lower   = state.lower()

    domain_url = (
        f"https://www.domain.com.au/sold-listings/{suburb_domain}-{state_lower}-{postcode}/"
        f"?excludepricewithheld=1&ssubs=0&dateRange[min]={domain_min}"
    )

    domain_html = await _fetch(domain_url)

    all_results: list[dict] = []

    # REA disabled — re-enable by un-commenting rea_url above and adding:
    # rea_html = await _fetch(rea_url)
    # if rea_html:
    #     for r in _parse_rea(rea_html):
    #         r["proximity_tier"] = "same_suburb"
    #         all_results.append(r)

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

    # Tag same-street properties and sort them first
    if street:
        street_lower = street.lower()
        for r in filtered:
            if street_lower in r.get("address", "").lower():
                r["proximity_tier"] = "same_street"
        filtered.sort(key=lambda r: 0 if r.get("proximity_tier") == "same_street" else 1)

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
        "data_source":       "domain.com.au (via Scrapfly)",
    }


# ---------------------------------------------------------------------------
# Debug helper — exposes parse internals without filtering
# ---------------------------------------------------------------------------

async def debug_scraper_comparable_sales(suburb: str, state: str, postcode: str) -> dict:
    """
    Diagnostic endpoint: fetch REA + Domain and return raw parse details.
    Use /debug-comparable-sales to diagnose parser failures on Railway.
    """
    suburb_rea    = suburb.lower().replace(" ", "+")
    suburb_domain = suburb.lower().replace(" ", "-")
    state_lower   = state.lower()
    prv_yr        = datetime.datetime.now().year - 1
    domain_min    = f"{prv_yr}-07-01"

    rea_url = (
        f"https://www.realestate.com.au/sold/in-{suburb_rea}+"
        f"{state_lower}+{postcode}/list-1"
        f"?includeSurrounding=false&source=refinement&soldIn=12"
    )
    domain_url = (
        f"https://www.domain.com.au/sold-listings/{suburb_domain}-{state_lower}-{postcode}/"
        f"?excludepricewithheld=1&ssubs=0&dateRange[min]={domain_min}"
    )

    rea_html, domain_html = await asyncio.gather(_fetch(rea_url), _fetch(domain_url))

    out: dict = {"rea_url": rea_url, "domain_url": domain_url}

    # ---- REA diagnostics ----
    rea: dict = {"fetched": rea_html is not None, "html_size": len(rea_html) if rea_html else 0}
    if rea_html:
        m = re.search(r"window\.ArgonautExchange\s*=\s*(\{.+?\});\s*(?:window\.|</script>)", rea_html, re.DOTALL)
        if not m:
            m = re.search(r"window\.ArgonautExchange\s*=\s*(\{.+)", rea_html)
        rea["argonaut_found"] = bool(m)
        if m:
            raw_json = m.group(1)
            data = None
            for candidate in (raw_json, raw_json.rsplit("};", 1)[0] + "}"):
                try:
                    data = json.loads(candidate)
                    break
                except json.JSONDecodeError:
                    pass
            rea["argonaut_parseable"] = data is not None
            if data:
                rea["argonaut_top_keys"] = list(data.keys())[:12]
                rea["argonaut_second_level"] = {
                    k: (list(v.keys())[:8] if isinstance(v, dict) else type(v).__name__)
                    for k, v in list(data.items())[:6]
                }
                # Try known paths first
                listings_raw: list | None = None
                found_path: str | None = None
                for key, value in data.items():
                    if not isinstance(value, dict):
                        continue
                    d = value.get("data", {})
                    if isinstance(d, dict):
                        for subkey in ("results", "listings", "data", "listingResponseData",
                                       "propertySaleActivityResults", "soldResults"):
                            c = d.get(subkey)
                            if isinstance(c, list) and c:
                                listings_raw = c
                                found_path = f"{key}.data.{subkey}"
                                break
                    if listings_raw:
                        break
                if not listings_raw:
                    listings_raw = _find_listings_recursive(data)
                    found_path = "recursive" if listings_raw else None

                rea["listings_path"] = found_path
                if listings_raw:
                    rea["raw_count"] = len(listings_raw)
                    rea["sample_item_keys"] = list(listings_raw[0].keys())
                    rea["sample_item_snippet"] = dict(list(listings_raw[0].items())[:6])
                else:
                    rea["raw_count"] = 0
                    # Expose urql cache keys so we can understand REA's new structure
                    urql_key = next(iter(data.keys()), None)
                    if urql_key:
                        urql_cache = data[urql_key].get("urqlClientCache", {})
                        if isinstance(urql_cache, dict):
                            cache_keys = list(urql_cache.keys())[:20]
                            rea["urql_cache_key_count"] = len(urql_cache)
                            rea["urql_cache_sample_keys"] = cache_keys
                            # Show first non-trivial value
                            for ck in cache_keys:
                                cv = urql_cache[ck]
                                if isinstance(cv, dict) and len(cv) > 2:
                                    rea["urql_cache_sample_entity"] = {
                                        "key": ck,
                                        "value_keys": list(cv.keys())[:12],
                                        "snippet": dict(list(cv.items())[:4]),
                                    }
                                    break

        extracted_rea = _parse_rea(rea_html)
        rea["extracted_count"] = len(extracted_rea)
        rea["sample_extracted"] = extracted_rea[:2]
    out["rea"] = rea

    # ---- Domain diagnostics ----
    dom: dict = {"fetched": domain_html is not None, "html_size": len(domain_html) if domain_html else 0}
    if domain_html:
        pat = r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>'
        m2 = re.search(pat, domain_html, re.DOTALL)
        dom["next_data_found"] = bool(m2)
        if m2:
            try:
                nd = json.loads(m2.group(1))
                page_props = nd.get("props", {}).get("pageProps", {})
                dom["page_props_keys"] = list(page_props.keys())[:15]
                comp_props = page_props.get("componentProps", {})
                if isinstance(comp_props, dict):
                    dom["component_props_keys"] = list(comp_props.keys())[:15]

                matched_path: str | None = None
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
                        matched_path = ".".join(path)
                        break

                dom["listings_path"] = matched_path or "not_found"
                if listings_map:
                    items = list(listings_map.values()) if isinstance(listings_map, dict) else list(listings_map)
                    dom["raw_count"] = len(items)
                    if items:
                        dom["sample_item_keys"] = list(items[0].keys())[:15]
                        dom["sample_item_snippet"] = dict(list(items[0].items())[:6])
                else:
                    dom["raw_count"] = 0
            except json.JSONDecodeError as e:
                dom["next_data_parseable"] = False
                dom["parse_error"] = str(e)[:200]

        extracted_dom = _parse_domain(domain_html)
        dom["extracted_count"] = len(extracted_dom)
        dom["sample_extracted"] = extracted_dom[:2]
    out["domain"] = dom

    return out
