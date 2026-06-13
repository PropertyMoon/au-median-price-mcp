"""
Victoria median house prices — quarterly XLS from the Valuer-General Victoria
via discover.data.vic.gov.au (CKAN API).

Package: "Victorian Property Sales Report - Median House by Suburb Quarterly"
Package ID: 86b545d3-1d0b-4069-bce0-5ce813473759
File format: .xls (old binary Excel — requires xlrd, NOT openpyxl)

Typical sheet structure:
  Title/header rows at top → first row containing "suburb" is the column header
  Columns: Suburb | Municipality | Median | No. of Sales | Prior-period Median | Change %
  Data for a single quarter (the period is encoded in the filename / title cell)

Data is cached in-process for 24 hours; first request triggers download (~1 MB).
"""

import asyncio
import re
import time

import httpx
import xlrd

_CKAN_API    = "https://discover.data.vic.gov.au/api/3/action/package_show"
_PACKAGE_ID  = "86b545d3-1d0b-4069-bce0-5ce813473759"
_DATASET_PAGE = (
    "https://discover.data.vic.gov.au/dataset/86b545d3-1d0b-4069-bce0-5ce813473759"
)
# Known base path on land.vic.gov.au — probe recent quarters in descending order
_LAND_VIC_BASE = "https://www.land.vic.gov.au/__data/assets/excel_doc"
_PROBE_QUARTERS = [
    # (doc_id_fragment, filename) — add new quarters here as they're published
    ("0023/762143", "median-house-q2-2025.xls"),  # confirmed working URL
]

_cache: dict = {}
_cache_ts: float = 0.0
_cache_lock = asyncio.Lock()

# land.vic.gov.au blocks requests without browser-like headers
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "application/vnd.ms-excel,*/*;q=0.8"
    ),
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": _DATASET_PAGE,
}


_CKAN_PORTAL = "https://discover.data.vic.gov.au"

async def _ckan_candidates() -> list[str]:
    """Return XLS download URLs from CKAN, newest first.

    Yields CKAN portal proxy URLs first (routed via discover.data.vic.gov.au,
    bypassing the land.vic.gov.au WAF), then the direct land.vic.gov.au URLs
    as a fallback. Excludes web.archive.org links which return HTML wrappers.
    """
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(_CKAN_API, params={"id": _PACKAGE_ID})
            r.raise_for_status()
            resources = r.json().get("result", {}).get("resources", [])
        xls = [
            res for res in resources
            if (res.get("format") or "").upper() in ("XLS", "XLSX")
            and "web.archive.org" not in (res.get("url") or "")
        ]
        xls.sort(key=lambda x: x.get("created", ""), reverse=True)

        # Build CKAN portal proxy URLs first — these route through
        # discover.data.vic.gov.au and avoid the land.vic.gov.au WAF.
        proxy_urls = [
            f"{_CKAN_PORTAL}/dataset/{_PACKAGE_ID}/resource/{res['id']}/download"
            for res in xls if res.get("id")
        ]
        direct_urls = [res["url"] for res in xls if res.get("url")]
        urls = list(dict.fromkeys(proxy_urls + direct_urls))
        if urls:
            print(f"  📥 VIC CKAN candidates ({len(proxy_urls)} proxy, {len(direct_urls)} direct): {urls}")
        return urls
    except Exception as e:
        print(f"  ⚠️  VIC CKAN discovery failed ({e})")
        return []



def _quarter_from_url(url: str) -> str | None:
    """Try to extract quarter string from the filename, e.g. 'median-house-q2-2025.xls'."""
    m = re.search(r'q(\d)-(\d{4})', url, re.I)
    if m:
        return f"{m.group(2)} Q{m.group(1)}"
    m = re.search(r'(\d{4}).*q(\d)', url, re.I)
    if m:
        return f"{m.group(1)} Q{m.group(2)}"
    return None


def _find_header_row(sheet) -> tuple[int, dict]:
    """
    Scan rows until we find one whose cells include 'suburb'.
    Returns (row_index, col_map) where col_map maps role → col index.
    """
    for row_idx in range(min(20, sheet.nrows)):
        row = [str(c or "").strip().lower() for c in sheet.row_values(row_idx)]
        if any("suburb" in cell for cell in row):
            col_map = {}
            for i, h in enumerate(row):
                if "suburb" in h and "suburb" not in col_map:
                    col_map["suburb"] = i
                elif "median" in h and "suburb" not in col_map:
                    col_map.setdefault("median", i)
                elif "median" in h:
                    col_map.setdefault("median", i)
                elif "sale" in h and "number" in h:
                    col_map.setdefault("sales_count", i)
                elif "number" in h or "no." in h or "count" in h:
                    col_map.setdefault("sales_count", i)
            return row_idx, col_map
    return -1, {}


def _parse_xls_bytes(raw: bytes, data_period: str) -> dict[str, dict]:
    """Parse an XLS file and return suburb → {median_price, data_period}."""
    wb = xlrd.open_workbook(file_contents=raw)
    suburb_data: dict[str, dict] = {}

    for sheet_name in wb.sheet_names():
        sheet = wb.sheet_by_name(sheet_name)
        header_row, col_map = _find_header_row(sheet)
        if header_row < 0 or "suburb" not in col_map or "median" not in col_map:
            continue

        sub_col = col_map["suburb"]
        med_col = col_map["median"]

        for row_idx in range(header_row + 1, sheet.nrows):
            row = sheet.row_values(row_idx)
            if len(row) <= max(sub_col, med_col):
                continue

            suburb_raw = str(row[sub_col] or "").strip()
            if not suburb_raw or suburb_raw.lower() in ("suburb", "total", "all suburbs", ""):
                continue

            median_raw = row[med_col]
            try:
                # xlrd returns numbers as float
                if isinstance(median_raw, (int, float)):
                    median = int(median_raw)
                else:
                    median = int(str(median_raw).replace(",", "").replace("$", "").strip())
                if median < 50_000 or median > 50_000_000:
                    continue
            except (ValueError, TypeError):
                continue

            suburb_key = suburb_raw.upper()
            suburb_data[suburb_key] = {
                "median_price": median,
                "data_period":  data_period,
            }

    return suburb_data


async def _load() -> dict:
    # Build ordered list of URLs to try: CKAN-discovered first, then hardcoded fallbacks.
    candidates = await _ckan_candidates()
    hardcoded = [
        f"{_LAND_VIC_BASE}/{frag}/{fname}"
        for frag, fname in _PROBE_QUARTERS
    ]
    urls_to_try = list(dict.fromkeys(candidates + hardcoded))  # deduplicate, preserve order

    last_err: Exception = RuntimeError("No VIC median URLs to try")
    async with httpx.AsyncClient(timeout=90, follow_redirects=True, headers=_HEADERS) as client:
        for url in urls_to_try:
            try:
                print(f"  📥 VIC median attempting: {url}")
                r = await client.get(url)
                r.raise_for_status()
                data_period = _quarter_from_url(url) or "latest"
                house_data = _parse_xls_bytes(r.content, data_period)
                print(f"  ✅ VIC median: {len(house_data)} suburbs loaded for {data_period}")
                return {"house": house_data, "data_period": data_period}
            except httpx.HTTPStatusError as e:
                print(f"  ⚠️  VIC download {url} → HTTP {e.response.status_code}, trying next")
                last_err = e
            except Exception as e:
                print(f"  ⚠️  VIC download {url} → {e}, trying next")
                last_err = e

    raise last_err


async def _data() -> dict:
    global _cache, _cache_ts
    async with _cache_lock:
        if _cache and (time.time() - _cache_ts) < 86_400:
            return _cache
        _cache = await _load()
        _cache_ts = time.time()
        return _cache


async def get_vic_median(suburb: str) -> dict:
    suburb_upper = suburb.strip().upper()
    data = await _data()
    house = data["house"].get(suburb_upper)

    if not house:
        return {
            "suburb":   suburb,
            "state":    "VIC",
            "coverage": "no_data",
            "message":  f"No median price record for '{suburb}' in VIC dataset.",
        }

    return {
        "suburb":              suburb,
        "state":               "VIC",
        "coverage":            "available",
        "median_house_price":  house["median_price"],
        "data_period":         house["data_period"],
        "data_source":         "Victorian Property Sales Report — Median House by Suburb Quarterly (land.vic.gov.au / Valuer-General Victoria)",
    }
