"""
au-median-price-mcp — MCP server providing Australian suburb median property prices
and comparable sales data.

Coverage:
  VIC: Victorian Property Sales Report quarterly XLS (data.vic.gov.au)
  NSW: NSW Valuer General bulk sales .DAT files (weekly, CC BY 4.0)
       Comparable sales: individual transactions with address, price, date, type
  All other states: coverage=unavailable — caller should fall back to web search

Transport: streamable-http (Railway deployment + Anthropic API integration)
Health:    GET /health
REST:      GET /suburb-median?suburb=X&state=Y
           GET /comparable-sales?suburb=X&state=Y[&property_type=house&min_price=500000&max_price=1000000&limit=20]
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
    property_type: str = "",
    min_price: int = 0,
    max_price: int = 0,
    limit: int = 20,
    address: str = "",
    radius_km: float = 1.0,
) -> dict:
    """
    Get recent comparable property sales near a property.

    When an address is supplied, returns sales within radius_km of that address
    (geocoded via OSM Nominatim). Without an address, returns all sales in the suburb.
    Results come from the most recent 24 months, sorted most-recent-first.

    Coverage:
    - NSW: NSW Valuer General bulk sales data (individual transactions, CC BY 4.0)
    - All other states: unavailable — use web search as fallback

    Args:
        suburb:        Suburb name (e.g. "Paddington", "Castle Hill")
        state:         State abbreviation (e.g. "NSW")
        address:       Full property address for radius search
                       (e.g. "15 Smith Street Castle Hill NSW 2154").
                       Leave blank to search the entire suburb.
        radius_km:     Search radius in km when address is provided (default 1.0)
        property_type: Filter by type — "house", "unit", "land", "other", or "" for all
        min_price:     Minimum sale price in AUD (0 = no minimum)
        max_price:     Maximum sale price in AUD (0 = no maximum)
        limit:         Maximum results to return (default 20, max 100)
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

    return {
        "suburb":   suburb,
        "state":    state,
        "coverage": "unavailable",
        "message":  (
            f"Comparable sales data not available for {state}. "
            "Use web search as fallback."
        ),
    }


async def _health(request: Request):
    return JSONResponse({"status": "ok", "service": "au-median-price-mcp"})


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
        result = await get_comparable_sales(suburb, state, property_type, min_price, max_price, limit, address, radius_km)
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
        Route("/health",           _health),
        Route("/suburb-median",    _suburb_median),
        Route("/comparable-sales", _comparable_sales),
        Mount("/", _mcp_app),
    ]
)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8002"))
    uvicorn.run(app, host="0.0.0.0", port=port)
