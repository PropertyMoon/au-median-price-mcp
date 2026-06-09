"""
au-median-price-mcp — MCP server providing Australian suburb median property prices.

Coverage:
  VIC: Victorian Property Sales Report quarterly XLS (data.vic.gov.au)
  NSW: NSW Valuer General bulk sales .DAT files (weekly, CC BY 4.0)
  All other states: coverage=unavailable — caller should fall back to web search

Transport: streamable-http (Railway deployment + Anthropic API integration)
Health:    GET /health
REST:      GET /suburb-median?suburb=X&state=Y
MCP:       POST /mcp
"""

import os

from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from sources.vic import get_vic_median
from sources.nsw import get_nsw_median

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


async def _health(request: Request):
    return JSONResponse({"status": "ok", "service": "au-median-price-mcp"})


async def _suburb_median(request: Request):
    """
    REST endpoint for PropertyReport to call directly.
    GET /suburb-median?suburb=Taylors+Lakes&state=VIC
    Returns the same payload as the get_suburb_median MCP tool.
    """
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
            status_code=200,  # return 200 so orchestrator handles it gracefully
        )


_mcp_app = mcp.streamable_http_app()
app = Starlette(
    routes=[
        Route("/health",         _health),
        Route("/suburb-median",  _suburb_median),
        Mount("/", _mcp_app),
    ]
)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8002"))
    uvicorn.run(app, host="0.0.0.0", port=port)
