"""
au-median-price-mcp — MCP server providing Australian suburb median property prices
and comparable sales data.

Median price coverage:
  VIC: Victorian Property Sales Report quarterly XLS (data.vic.gov.au)
  NSW: NSW Valuer General bulk sales .DAT files (weekly, CC BY 4.0)
  All other states: unavailable — caller falls back to web search

Comparable sales coverage:
  NSW: NSW Valuer General (authoritative, supports radius/type/price filters)
  All other states: REA + Domain via Scrapfly (last 12 months, requires postcode)

Transport: streamable-http (Railway deployment + Anthropic API integration)
Health:    GET /health
REST:      GET /suburb-median?suburb=X&state=Y
           GET /comparable-sales?suburb=X&state=Y&postcode=Z[&street=S&limit=20]
MCP:       POST /mcp
"""

import os

from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from sources.vic import get_vic_median
from sources.nsw import get_nsw_median, get_nsw_comparable_sales
from sources.scraper import get_scraper_comparable_sales, debug_scraper_comparable_sales

mcp = FastMCP("au-median-price", stateless_http=True)


@mcp.tool()
async def get_suburb_median(suburb: str, state: str) -> dict:
    """
    Get median property sale prices for an Australian suburb.

    Returns median_house_price (int, AUD), median_unit_price (int, AUD),
    data_period, coverage ("available" | "no_data" | "unavailable"), and data_source.

    Coverage:
    - VIC: Victorian Property Sales Report quarterly (data.vic.gov.au)
    - NSW: NSW Valuer General bulk sales data (weekly, CC BY 4.0)
    - All other states: unavailable — use web search as fallback

    Args:
        suburb: Suburb name (e.g. "Taylors Lakes", "Paddington")
        state:  State abbreviation (e.g. "VIC", "NSW", "QLD")
    """
    state_norm = state.strip().upper()

    if state_norm in ("VIC", "VICTORIA"):
        return await get_vic_median(suburb)

    if state_norm in ("NSW", "NEW SOUTH WALES"):
        return await get_nsw_median(suburb)

    return {
        "suburb":   suburb,
        "state":    state,
        "coverage": "unavailable",
        "message":  (
            f"Median price data not available for {state}. "
            "Use web search as fallback."
        ),
    }


@mcp.tool()
async def get_comparable_sales(
    suburb: str,
    state: str,
    postcode: str = "",
    street: str = "",
    property_type: str = "",
    min_price: int = 0,
    max_price: int = 0,
    limit: int = 20,
    address: str = "",
    radius_km: float = 1.0,
) -> dict:
    """
    Get recent comparable property sales near a property.

    Coverage:
    - NSW: NSW Valuer General bulk sales data (authoritative, CC BY 4.0).
           Supports radius search, property type filter, price range.
    - All other states (VIC, SA, QLD, WA, TAS, NT, ACT): REA + Domain via Scrapfly.
           Returns last 12 months. Requires postcode. street triggers same-street results first.
           Falls back to "unavailable" if SCRAPFLY_API_KEY is not set.

    Args:
        suburb:        Suburb name (e.g. "Hazelwood Park", "Taylors Lakes")
        state:         State abbreviation (e.g. "SA", "VIC", "NSW")
        postcode:      Postcode (e.g. "5066") — required for non-NSW states
        street:        Street name only (e.g. "Devereux Road") — optional, triggers
                       same-street search first for non-NSW states
        address:       Full address for NSW radius search (e.g. "15 Smith St Castle Hill NSW 2154")
        radius_km:     Radius in km for NSW address search (default 1.0)
        property_type: Filter by type — "house", "unit", "land", "other", or "" for all (NSW only)
        min_price:     Minimum sale price AUD (0 = no minimum, NSW only)
        max_price:     Maximum sale price AUD (0 = no maximum, NSW only)
        limit:         Max results (default 20, max 100, NSW only)
    """
    state_norm = state.strip().upper()

    if state_norm in ("NSW", "NEW SOUTH WALES"):
        return await get_nsw_comparable_sales(
            suburb,
            property_type=property_type or None,
            min_price=min_price or None,
            max_price=max_price or None,
            limit=min(limit, 100),
            address=address or None,
            radius_km=radius_km,
        )

    # All other states — use Scrapfly (REA + Domain)
    if not postcode:
        return {
            "suburb":   suburb,
            "state":    state,
            "coverage": "unavailable",
            "message":  "postcode is required for non-NSW comparable sales lookup.",
        }

    return await get_scraper_comparable_sales(
        suburb=suburb,
        state=state_norm,
        postcode=postcode,
        street=street or None,
    )


async def _health(request: Request):
    return JSONResponse({"status": "ok", "service": "au-median-price-mcp"})


async def _debug_comparable_sales(request: Request):
    suburb   = request.query_params.get("suburb",   "").strip()
    state    = request.query_params.get("state",    "").strip()
    postcode = request.query_params.get("postcode", "").strip()
    if not suburb or not state or not postcode:
        return JSONResponse(
            {"error": "suburb, state, and postcode are required"},
            status_code=400,
        )
    try:
        result = await debug_scraper_comparable_sales(suburb, state, postcode)
        return JSONResponse(result)
    except Exception as e:
        import traceback
        return JSONResponse({"error": str(e), "traceback": traceback.format_exc()}, status_code=500)


async def _suburb_median(request: Request):
    suburb = request.query_params.get("suburb", "").strip()
    state  = request.query_params.get("state",  "").strip()
    if not suburb or not state:
        return JSONResponse(
            {"error": "suburb and state query params are required"},
            status_code=400,
        )
    try:
        result = await get_suburb_median(suburb, state)
        return JSONResponse(result)
    except Exception as e:
        import traceback
        print(f"  ❌ /suburb-median error for {suburb} {state}: {e}\n{traceback.format_exc()}")
        return JSONResponse(
            {"suburb": suburb, "state": state, "coverage": "no_data",
             "message": f"Data load failed: {type(e).__name__}: {e}"},
            status_code=200,
        )


async def _comparable_sales(request: Request):
    suburb        = request.query_params.get("suburb",        "").strip()
    state         = request.query_params.get("state",         "").strip()
    postcode      = request.query_params.get("postcode",      "").strip()
    street        = request.query_params.get("street",        "").strip()
    property_type = request.query_params.get("property_type", "").strip()
    limit         = int(request.query_params.get("limit",     "20")  or "20")
    min_price     = int(request.query_params.get("min_price", "0")   or "0")
    max_price     = int(request.query_params.get("max_price", "0")   or "0")
    address       = request.query_params.get("address",       "").strip()
    radius_km     = float(request.query_params.get("radius_km", "1.0") or "1.0")

    if not suburb or not state:
        return JSONResponse(
            {"error": "suburb and state query params are required"},
            status_code=400,
        )
    try:
        result = await get_comparable_sales(
            suburb, state, postcode, street,
            property_type, min_price, max_price, limit, address, radius_km,
        )
        return JSONResponse(result)
    except Exception as e:
        import traceback
        print(f"  ❌ /comparable-sales error for {suburb} {state}: {e}\n{traceback.format_exc()}")
        return JSONResponse(
            {"suburb": suburb, "state": state, "coverage": "no_data",
             "message": f"Data load failed: {type(e).__name__}: {e}"},
            status_code=200,
        )


_mcp_app = mcp.streamable_http_app()
app = Starlette(
    routes=[
        Route("/health",                  _health),
        Route("/suburb-median",           _suburb_median),
        Route("/comparable-sales",        _comparable_sales),
        Route("/debug-comparable-sales",  _debug_comparable_sales),
        Mount("/", _mcp_app),
    ]
)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8002"))
    uvicorn.run(app, host="0.0.0.0", port=port)
