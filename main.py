"""
PassportDPP API — Digital Product Passport for EU Circular Economy
FastAPI application ready for RapidAPI deployment.
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from data_pipeline import (
    build_dataset,
    get_passport_by_id,
    search_passports,
    verify_passport,
    get_standards,
    SECTORS,
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("passportdpp")

# ── Rate limiter ─────────────────────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address, default_limits=["100/day"])

# ── API key validation ───────────────────────────────────────────────────────
API_KEYS = set()

def load_api_keys():
    """Load API keys from env var (comma-separated) or use default test key."""
    raw = os.environ.get("PASSPORTDPP_API_KEYS", "")
    if raw.strip():
        return {k.strip() for k in raw.split(",") if k.strip()}
    return {"test-key-123", "demo-key-456", "rapidapi-pro"}

API_KEYS.update(load_api_keys())

def get_api_key(request: Request) -> str:
    """Extract and validate x-api-key header."""
    api_key = request.headers.get("x-api-key", "")
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing x-api-key header")
    if api_key not in API_KEYS:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return api_key


# ── Lifespan ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown events."""
    logger.info("PassportDPP API starting...")
    build_dataset(force_refresh=True)
    logger.info(f"Dataset loaded. Keys configured: {len(API_KEYS)}")
    yield
    logger.info("PassportDPP API shutting down.")

# ── App instance ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="PassportDPP API",
    description="Digital Product Passport API for EU Circular Economy — "
                "Batteries, Textiles, Electronics & Construction. "
                "Compliant with EU Battery Regulation 2023/1542, ESPR 2023, "
                "and EU Textile Strategy.",
    version="1.0.0",
    contact={
        "name": "PassportDPP Team",
        "url": "https://passportdpp.nousresearch.com",
        "email": "support@passportdpp.com",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
    lifespan=lifespan,
)

# ── Middleware ───────────────────────────────────────────────────────────────
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Models ───────────────────────────────────────────────────────────────────

class MaterialInfo(BaseModel):
    name: str
    percentage: float
    recycled_content_pct: float
    critical_raw_material: bool

class ManufacturerInfo(BaseModel):
    name: str
    country: str
    eu_registered: bool

class Passport(BaseModel):
    id: str
    uid: str
    product_name: str
    sector: str
    manufacturer: ManufacturerInfo
    materials: list[MaterialInfo]
    recyclability_pct: float
    carbon_footprint_kg_co2e: float
    lifespan_years: int
    compliance: str
    certifications: list[str]
    issue_date: str
    expiry_date: str
    status: str

class PassportSummary(BaseModel):
    id: str
    product_name: str
    sector: str
    manufacturer: str
    manufacturer_country: str
    recyclability_pct: float
    carbon_footprint_kg_co2e: float
    status: str

class VerifyResult(BaseModel):
    passport_id: str
    verified: bool
    status: str
    uid_match: bool
    issued_by: str
    issue_date: str
    expiry_date: str

class HealthStatus(BaseModel):
    status: str = "healthy"
    version: str = "1.0.0"
    dataset_size: int = 0
    sectors: list[str] = []
    uptime_seconds: float = 0.0


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health_check():
    """Unauthenticated health check for Railway."""
    return {"status": "healthy", "service": "passportdpp-api"}

@app.get("/v1/health", tags=["System"])
@limiter.limit("100/minute")
async def health(request: Request, api_key: str = Depends(get_api_key)):
    """Health check endpoint."""
    ds = build_dataset()
    return HealthStatus(
        status="healthy",
        version="1.0.0",
        dataset_size=len(ds),
        sectors=SECTORS,
    )


@app.get("/v1/passports", tags=["Passports"])
@limiter.limit("30/minute")
async def list_passports(
    request: Request,
    sector: Optional[str] = Query(None, description="Filter by sector"),
    manufacturer: Optional[str] = Query(None, description="Search manufacturer name"),
    query: Optional[str] = Query(None, description="Search product name or ID"),
    status: Optional[str] = Query(None, description="Filter by status: active, expired, revoked"),
    min_recyclability: Optional[float] = Query(None, ge=0, le=100, description="Minimum recyclability %"),
    max_carbon: Optional[float] = Query(None, ge=0, description="Maximum carbon footprint kg CO2e"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    api_key: str = Depends(get_api_key),
):
    """Search and list Digital Product Passports with filters."""
    if sector and sector.lower() not in SECTORS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sector '{sector}'. Must be one of: {', '.join(SECTORS)}",
        )

    results, total = search_passports(
        sector=sector,
        manufacturer=manufacturer,
        query=query,
        status=status,
        min_recyclability=min_recyclability,
        max_carbon=max_carbon,
        limit=limit,
        offset=offset,
    )

    summaries = [
        PassportSummary(
            id=p["id"],
            product_name=p["product_name"],
            sector=p["sector"],
            manufacturer=p["manufacturer"]["name"],
            manufacturer_country=p["manufacturer"]["country"],
            recyclability_pct=p["recyclability_pct"],
            carbon_footprint_kg_co2e=p["carbon_footprint_kg_co2e"],
            status=p["status"],
        )
        for p in results
    ]

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": [s.model_dump() for s in summaries],
    }


@app.get("/v1/passport/{passport_id}", tags=["Passports"])
@limiter.limit("30/minute")
async def get_passport(
    request: Request,
    passport_id: str,
    api_key: str = Depends(get_api_key),
):
    """Get full DPP details for a specific passport."""
    dpp = get_passport_by_id(passport_id)
    if not dpp:
        raise HTTPException(status_code=404, detail=f"Passport '{passport_id}' not found")
    return Passport(**dpp).model_dump()


@app.get("/v1/verify/{passport_id}", tags=["Verification"])
@limiter.limit("30/minute")
async def verify(
    request: Request,
    passport_id: str,
    api_key: str = Depends(get_api_key),
):
    """Verify a Digital Product Passport's authenticity and status."""
    result = verify_passport(passport_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Passport '{passport_id}' not found")
    return VerifyResult(**result).model_dump()


@app.get("/v1/standards", tags=["Standards"])
@limiter.limit("20/minute")
async def standards(
    request: Request,
    sector: Optional[str] = Query(None, description="Filter standards by sector"),
    api_key: str = Depends(get_api_key),
):
    """Get EU DPP regulatory standards and requirements."""
    if sector and sector.lower() not in SECTORS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sector '{sector}'. Must be one of: {', '.join(SECTORS)}",
        )
    return {"standards": get_standards(sector=sector.lower() if sector else None)}


# ── Root ─────────────────────────────────────────────────────────────────────
@app.get("/", tags=["System"])
async def root():
    """Root redirect to docs."""
    return {
        "service": "PassportDPP API",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "/v1/health",
    }


# ── CLI runner ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
