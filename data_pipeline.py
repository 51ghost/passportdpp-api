"""
PassportDPP - Data Pipeline
Curated dataset of 50+ Digital Product Passports across EU-regulated sectors
"""

import json
import hashlib
import random
from datetime import datetime, timedelta
from typing import Optional

# ── In-memory cache ──────────────────────────────────────────────────────────
_cache = {}
CACHE_TTL = timedelta(hours=24)

SECTORS = ["battery", "textile", "electronics", "construction"]

# ── Synthetic dataset generator ─────────────────────────────────────────────

MANUFACTURERS = {
    "battery": [
        ("VoltCore GmbH", "Germany"), ("EnerCell Ltd", "Sweden"),
        ("PowerNova SAS", "France"), ("Ionix Inc", "Netherlands"),
        ("GreenBatt SpA", "Italy"), ("NordicCell AB", "Finland"),
        ("EcoVolt BV", "Belgium"), ("LiTech GmbH", "Austria"),
    ],
    "textile": [
        ("EcoWeave Ltd", "Portugal"), ("CircuTex SAS", "France"),
        ("GreenFiber GmbH", "Germany"), ("ReThread SpA", "Italy"),
        ("BioCloth BV", "Netherlands"), ("NordicWool AB", "Sweden"),
        ("SustainDenim Ltd", "Turkey"), ("ReVibe Textiles", "Spain"),
    ],
    "electronics": [
        ("ChipCirc SAS", "France"), ("EcoBoard GmbH", "Germany"),
        ("GreenLogic Ltd", "Ireland"), ("ReTech SpA", "Italy"),
        ("NordicChip AB", "Sweden"), ("VoltLogic BV", "Netherlands"),
        ("SustainBoard Inc", "Finland"), ("EcoDisplay Ltd", "Austria"),
    ],
    "construction": [
        ("EcoBuild GmbH", "Germany"), ("GreenCem SAS", "France"),
        ("ReConcrete Ltd", "Netherlands"), ("BioBrick SpA", "Italy"),
        ("NordicBuild AB", "Sweden"), ("SustainSteel BV", "Belgium"),
        ("EcoPanel Ltd", "Portugal"), ("GreenRoof GmbH", "Austria"),
    ],
}

PRODUCT_NAMES = {
    "battery": [
        "Lithium-Ion HV Battery Pack 400V", "Solid-State EV Battery 800V",
        "LFP Battery Module 48V", "Sodium-Ion Grid Storage Battery",
        "NMC Automotive Battery 350V", "Lithium-Sulfur Aviation Battery",
        "Nickel-Iron Industrial Battery", "Recycled Li-Ion Power Bank 100Ah",
    ],
    "textile": [
        "Organic Cotton Denim Jacket", "Recycled Polyester Sportswear Set",
        "Hemp-Blend Workwear Trousers", "Linen-Cotton Summer Dress",
        "Tencel Lyocell Casual Shirt", "Wool-Cashmere Blend Sweater",
        "Recycled Nylon Raincoat", "Bamboo Fiber Bed Sheet Set",
    ],
    "electronics": [
        "Smartphone Motherboard Gen5", "Laptop Logic Board Pro",
        "IoT Sensor Module V3", "Server Grade SSD 2TB",
        "Smart Display Panel 27\"", "USB-C Power Delivery Hub",
        "Wireless Charging Station", "E-Ink Reader Mainboard",
    ],
    "construction": [
        "Recycled Steel I-Beam 6m", "Low-Carbon Concrete C30/37",
        "Cross-Laminated Timber Panel", "Recycled Plastic Brick",
        "Thermal Insulation Wool Board", "Green Roof System Module",
        "Recycled Aluminum Window Frame", "Hempcrete Building Block",
    ],
}

MATERIALS_DB = {
    "battery": [
        {"name": "Lithium", "percentage": 2.5, "recycled_content": 15.0, "critical_raw_material": True},
        {"name": "Cobalt", "percentage": 5.0, "recycled_content": 10.0, "critical_raw_material": True},
        {"name": "Nickel", "percentage": 12.0, "recycled_content": 20.0, "critical_raw_material": True},
        {"name": "Graphite", "percentage": 15.0, "recycled_content": 5.0, "critical_raw_material": True},
        {"name": "Copper", "percentage": 8.0, "recycled_content": 30.0, "critical_raw_material": True},
        {"name": "Aluminum", "percentage": 22.0, "recycled_content": 40.0, "critical_raw_material": False},
        {"name": "Steel", "percentage": 25.0, "recycled_content": 35.0, "critical_raw_material": False},
        {"name": "Plastic/Polymer", "percentage": 10.5, "recycled_content": 8.0, "critical_raw_material": False},
    ],
    "textile": [
        {"name": "Cotton (Organic)", "percentage": 45.0, "recycled_content": 0.0, "critical_raw_material": False},
        {"name": "Polyester (Recycled)", "percentage": 30.0, "recycled_content": 100.0, "critical_raw_material": False},
        {"name": "Elastane", "percentage": 5.0, "recycled_content": 0.0, "critical_raw_material": False},
        {"name": "Nylon (Recycled)", "percentage": 10.0, "recycled_content": 100.0, "critical_raw_material": False},
        {"name": "Linen", "percentage": 5.0, "recycled_content": 0.0, "critical_raw_material": False},
        {"name": "Wool", "percentage": 3.0, "recycled_content": 15.0, "critical_raw_material": False},
        {"name": "Tencel Lyocell", "percentage": 2.0, "recycled_content": 0.0, "critical_raw_material": False},
    ],
    "electronics": [
        {"name": "Gold", "percentage": 0.01, "recycled_content": 25.0, "critical_raw_material": True},
        {"name": "Copper", "percentage": 5.0, "recycled_content": 30.0, "critical_raw_material": True},
        {"name": "Tin", "percentage": 1.0, "recycled_content": 20.0, "critical_raw_material": True},
        {"name": "Tungsten", "percentage": 0.5, "recycled_content": 15.0, "critical_raw_material": True},
        {"name": "Silicon", "percentage": 15.0, "recycled_content": 10.0, "critical_raw_material": False},
        {"name": "Copper Alloy", "percentage": 8.0, "recycled_content": 35.0, "critical_raw_material": False},
        {"name": "Aluminum", "percentage": 20.0, "recycled_content": 40.0, "critical_raw_material": False},
        {"name": "Plastic/ABS", "percentage": 25.0, "recycled_content": 15.0, "critical_raw_material": False},
        {"name": "Glass", "percentage": 15.0, "recycled_content": 20.0, "critical_raw_material": False},
        {"name": "Steel", "percentage": 10.49, "recycled_content": 30.0, "critical_raw_material": False},
    ],
    "construction": [
        {"name": "Steel (Recycled)", "percentage": 25.0, "recycled_content": 90.0, "critical_raw_material": False},
        {"name": "Cement", "percentage": 15.0, "recycled_content": 5.0, "critical_raw_material": False},
        {"name": "Recycled Aggregate", "percentage": 30.0, "recycled_content": 100.0, "critical_raw_material": False},
        {"name": "Timber (FSC)", "percentage": 10.0, "recycled_content": 0.0, "critical_raw_material": False},
        {"name": "Glass Wool", "percentage": 5.0, "recycled_content": 40.0, "critical_raw_material": False},
        {"name": "Polyurethane Foam", "percentage": 3.0, "recycled_content": 10.0, "critical_raw_material": False},
        {"name": "Aluminum (Recycled)", "percentage": 7.0, "recycled_content": 85.0, "critical_raw_material": False},
        {"name": "Hemp Fiber", "percentage": 5.0, "recycled_content": 0.0, "critical_raw_material": False},
    ],
}

CERTIFICATIONS = {
    "battery": [
        "UN 38.3", "IEC 62133", "ISO 14001", "EU Battery Directive 2023/1542",
        "CE Marking", "RoHS Compliant", "REACH Compliant", "UL 2580",
    ],
    "textile": [
        "OEKO-TEX Standard 100", "GOTS", "EU Ecolabel", "Bluesign",
        "ISO 14001", "REACH Compliant", "Fair Trade Certified", "PEFC",
    ],
    "electronics": [
        "CE Marking", "RoHS Compliant", "WEEE Compliant", "REACH Compliant",
        "ISO 14001", "IEC 62474", "Energy Star", "EPEAT Gold",
    ],
    "construction": [
        "CE Marking", "ISO 14001", "BREEAM Compliant", "Cradle to Cradle",
        "FSC Certified", "EPD (Environmental Product Declaration)",
        "EU CE Mark 305/2011", "PEFC",
    ],
}

COMPLIANCE_STANDARDS = {
    "battery": "EU Battery Regulation 2023/1542, UN ECE R100, IEC 62660",
    "textile": "EU Textile Strategy 2022, EU Ecolabel Regulation 66/2010, GOTS v7.0",
    "electronics": "EU WEEE Directive 2012/19/EU, RoHS Directive 2011/65/EU, Energy-related Products Directive 2009/125/EC",
    "construction": "EU Construction Products Regulation 305/2011, EN 15804:2012+A2:2019, Level(s) Framework",
}


def _generate_passport(sector: str, idx: int) -> dict:
    """Generate one synthetic DPP entry for the given sector."""
    ts = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
    mfr, mfr_country = random.choice(MANUFACTURERS[sector])
    name = PRODUCT_NAMES[sector][idx % len(PRODUCT_NAMES[sector])]
    raw_materials = MATERIALS_DB[sector][:]

    # shuffle and adjust percentages to sum to ~100
    random.shuffle(raw_materials)
    total = sum(m["percentage"] for m in raw_materials)
    mat_list = []
    for m in raw_materials:
        adj = round(m["percentage"] / total * 100, 2)
        mat_list.append({
            "name": m["name"],
            "percentage": adj,
            "recycled_content_pct": m["recycled_content"],
            "critical_raw_material": m["critical_raw_material"],
        })

    certs = random.sample(CERTIFICATIONS[sector], k=min(4, len(CERTIFICATIONS[sector])))

    carbon = round(random.uniform(2.5, 85.0), 2)
    recyclability = round(random.uniform(40.0, 98.0), 1)
    lifespan = random.randint(2, 25)

    passport_id = f"DPP-{sector[:3].upper()}-{idx:04d}-{random.randint(1000,9999)}"
    uid_raw = f"{passport_id}-{mfr}-{name}-{ts.isoformat()}"
    uid_hash = hashlib.sha256(uid_raw.encode()).hexdigest()[:16]

    return {
        "id": passport_id,
        "uid": uid_hash,
        "product_name": name,
        "sector": sector,
        "manufacturer": {
            "name": mfr,
            "country": mfr_country,
            "eu_registered": True,
        },
        "materials": mat_list,
        "recyclability_pct": recyclability,
        "carbon_footprint_kg_co2e": carbon,
        "lifespan_years": lifespan,
        "compliance": COMPLIANCE_STANDARDS[sector],
        "certifications": certs,
        "issue_date": ts.date().isoformat(),
        "expiry_date": (ts + timedelta(days=365 * 5)).date().isoformat(),
        "status": random.choices(["active", "active", "active", "expired", "revoked"], weights=[70, 15, 10, 3, 2])[0],
    }


def build_dataset(sectors: list[str] | None = None, force_refresh: bool = False) -> list[dict]:
    """
    Build and return the curated DPP dataset.
    Results are cached for CACHE_TTL. Pass force_refresh=True to bypass cache.
    """
    global _cache
    now = datetime.now()

    if not force_refresh and "dataset" in _cache:
        cached_ts = _cache.get("timestamp", datetime.min)
        if now - cached_ts < CACHE_TTL:
            return _cache["dataset"]

    sectors = sectors or SECTORS
    passports = []
    target_per_sector = max(12, 55 // len(sectors))

    for sec in sectors:
        for i in range(target_per_sector):
            passports.append(_generate_passport(sec, i + 1))

    # Shuffle so they're not grouped by sector
    random.seed(42)
    random.shuffle(passports)
    random.seed()

    _cache["dataset"] = passports
    _cache["timestamp"] = now
    return passports


def get_passport_by_id(passport_id: str) -> dict | None:
    """Lookup a single passport by ID."""
    ds = build_dataset()
    for p in ds:
        if p["id"] == passport_id:
            return p
    return None


def search_passports(
    sector: str | None = None,
    manufacturer: str | None = None,
    query: str | None = None,
    status: str | None = None,
    min_recyclability: float | None = None,
    max_carbon: float | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Search/filter the dataset. Returns (results, total_count)."""
    ds = build_dataset()
    results = ds[:]

    if sector:
        results = [p for p in results if p["sector"] == sector.lower()]
    if manufacturer:
        mfr_lower = manufacturer.lower()
        results = [p for p in results if mfr_lower in p["manufacturer"]["name"].lower()]
    if query:
        q = query.lower()
        results = [p for p in results if q in p["product_name"].lower() or q in p["id"].lower()]
    if status:
        results = [p for p in results if p["status"] == status.lower()]
    if min_recyclability is not None:
        results = [p for p in results if p["recyclability_pct"] >= min_recyclability]
    if max_carbon is not None:
        results = [p for p in results if p["carbon_footprint_kg_co2e"] <= max_carbon]

    total = len(results)
    return results[offset:offset + limit], total


def verify_passport(passport_id: str) -> dict | None:
    """
    Verify a passport's integrity using its UID hash and status.
    Returns verification result dict or None if not found.
    """
    passport = get_passport_by_id(passport_id)
    if not passport:
        return None

    # Recompute hash
    recomputed = hashlib.sha256(
        f"{passport['id']}-{passport['manufacturer']['name']}-{passport['product_name']}-{passport['issue_date']}".encode()
    ).hexdigest()[:16]

    is_valid = (
        passport["status"] == "active"
        and passport["uid"] == recomputed
    )

    return {
        "passport_id": passport_id,
        "verified": is_valid,
        "status": passport["status"],
        "uid_match": passport["uid"] == recomputed,
        "issued_by": passport["manufacturer"]["name"],
        "issue_date": passport["issue_date"],
        "expiry_date": passport["expiry_date"],
    }


# ── Standards reference data ────────────────────────────────────────────────

STANDARDS_DATA = {
    "battery": {
        "regulation": "EU Battery Regulation 2023/1542",
        "effective_date": "2024-02-18",
        "scope": "All batteries placed on EU market (portable, automotive, EV, industrial)",
        "requirements": [
            "Carbon footprint declaration per lifecycle stage",
            "Minimum recycled content: 16% Co, 85% Pb, 6% Li, 6% Ni by 2031",
            "Performance and durability labelling",
            "Digital Product Passport with unique identifier",
            "CE marking via notified body assessment",
            "Waste collection target: 73% by 2030, 85% by 2035",
        ],
        "passport_fields": [
            "Battery model and batch info",
            "Manufacturer identification",
            "Materials and chemical composition",
            "Carbon footprint data per kg/Wh",
            "Recycled content percentages",
            "Safety and hazard information",
            "Repair and dismantling instructions",
        ],
    },
    "textile": {
        "regulation": "EU Textile Strategy 2022 + ESPR 2023",
        "effective_date": "2025-01-01",
        "scope": "Apparel, home textiles, and technical textiles sold in EU",
        "requirements": [
            "Digital Product Passport mandatory by 2026",
            "Ban on destruction of unsold textiles (2024)",
            "Ecodesign requirements for durability and recyclability",
            "Microplastic shedding labelling",
            "Mandatory recycled content for certain categories",
            "Extended Producer Responsibility schemes",
        ],
        "passport_fields": [
            "Fiber composition and origin",
            "Manufacturing process and supply chain",
            "Care and durability information",
            "Recyclability and end-of-life options",
            "Chemical substance declarations",
            "Social compliance indicators",
        ],
    },
    "electronics": {
        "regulation": "EU Ecodesign for Sustainable Products Regulation (ESPR) 2023",
        "effective_date": "2025-07-01",
        "scope": "Smartphones, tablets, laptops, servers, and consumer electronics",
        "requirements": [
            "Right to repair: spare parts availability 5-10 years",
            "Software update commitment period",
            "Energy efficiency class labelling",
            "Battery removability for handheld devices",
            "Product durability and reliability scoring",
            "Critical raw material content declaration",
        ],
        "passport_fields": [
            "Product identification and model number",
            "Critical raw material inventory",
            "Repairability score and instructions",
            "Energy efficiency metrics",
            "Software lifecycle and security updates",
            "End-of-life recyclability guidance",
        ],
    },
    "construction": {
        "regulation": "EU Construction Products Regulation 305/2011 + Level(s) Framework",
        "effective_date": "2024-01-01",
        "scope": "Structural materials, insulation, glazing, flooring, and finishing products",
        "requirements": [
            "Environmental Product Declaration (EPD) for all product groups",
            "Lifecycle assessment per EN 15804+A2",
            "Level(s) sustainability indicators",
            "Declaration of hazardous substances (SVHC)",
            "Recycled content and recyclability declaration",
            "Digital product passport phased in from 2024",
        ],
        "passport_fields": [
            "Product identification and performance class",
            "Lifecycle assessment data (A1-D stages)",
            "Hazardous substance declaration",
            "Recycled content and circularity info",
            "Installation and disassembly instructions",
            "Maintenance and lifetime expectancy",
        ],
    },
}


def get_standards(sector: str | None = None) -> dict:
    """Return EU standards reference data."""
    if sector:
        return {sector: STANDARDS_DATA.get(sector, {})}
    return STANDARDS_DATA
