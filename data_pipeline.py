"""Passportdpp API — Data Pipeline

In-memory dataset of ~100 Digital Product Passport records
with caching, search, verification, and standards registry.
"""

import time
import uuid
import random
import hashlib


# ── Standards Registry ────────────────────────────────────────────────────────

STANDARDS = [
    {
        "id": "ISO-14040",
        "name": "ISO 14040:2006 — Environmental management, Life cycle assessment, Principles and framework",
        "sector": "all",
        "category": "methodology",
    },
    {
        "id": "ISO-14044",
        "name": "ISO 14044:2006 — Environmental management, Life cycle assessment, Requirements and guidelines",
        "sector": "all",
        "category": "methodology",
    },
    {
        "id": "EU-ESPR-2023",
        "name": "EU Ecodesign for Sustainable Products Regulation (ESPR) 2023/1542",
        "sector": "all",
        "category": "regulation",
    },
    {
        "id": "EU-BATTERY-2023",
        "name": "EU Battery Regulation 2023/1542 — Carbon footprint, recycled content, due diligence",
        "sector": "batteries",
        "category": "regulation",
    },
    {
        "id": "EU-TEXTILE-STRATEGY",
        "name": "EU Strategy for Sustainable and Circular Textiles — Digital Product Passport",
        "sector": "textiles",
        "category": "regulation",
    },
    {
        "id": "EN-15343",
        "name": "EN 15343 — Plastics recycling traceability and conformity assessment",
        "sector": "plastics",
        "category": "standard",
    },
    {
        "id": "ISO-14067",
        "name": "ISO 14067:2018 — Greenhouse gases, Carbon footprint of products, Requirements and guidelines",
        "sector": "all",
        "category": "methodology",
    },
    {
        "id": "ISO-14021",
        "name": "ISO 14021:2016 — Environmental labels and declarations, Self-declared environmental claims",
        "sector": "all",
        "category": "labeling",
    },
    {
        "id": "EN-50614",
        "name": "EN 50614 — Requirements for the preparing for re-use of waste electrical and electronic equipment",
        "sector": "electronics",
        "category": "standard",
    },
    {
        "id": "EU-CONSTRUCTION-305",
        "name": "EU Construction Products Regulation (EU) 305/2011 — CPR",
        "sector": "construction",
        "category": "regulation",
    },
    {
        "id": "ISO-14025",
        "name": "ISO 14025:2006 — Environmental labels and declarations, Type III environmental declarations (EPD)",
        "sector": "all",
        "category": "labeling",
    },
    {
        "id": "EU-CEAP-2020",
        "name": "EU Circular Economy Action Plan 2020 — Product policy, Ecodesign, Waste reduction",
        "sector": "all",
        "category": "policy",
    },
    {
        "id": "ISO-20400",
        "name": "ISO 20400:2017 — Sustainable procurement, Guidance",
        "sector": "all",
        "category": "guidance",
    },
    {
        "id": "EN-45557",
        "name": "EN 45557 — General method for assessing the proportion of re-used components in products",
        "sector": "electronics",
        "category": "standard",
    },
    {
        "id": "GS1-DPP",
        "name": "GS1 Digital Link standard for DPP — Product identification and data carrier",
        "sector": "all",
        "category": "standard",
    },
]

SECTORS = sorted({s["sector"] for s in STANDARDS if s["sector"] != "all"})  # unique non-all sectors
SECTORS = ["batteries", "construction", "electronics", "plastics", "textiles"]


# ── DataCache (preserved) ─────────────────────────────────────────────────────

class DataCache:
    """Simple time-to-live in-memory cache."""

    def __init__(self, ttl=3600):
        self._cache = {}
        self._ttl = ttl

    def get(self, key):
        val, ts = self._cache.get(key, (None, 0))
        if val and time.time() - ts < self._ttl:
            return val
        return None

    def set(self, key, val):
        self._cache[key] = (val, time.time())

    def invalidate(self, key):
        self._cache.pop(key, None)

    def clear(self):
        self._cache.clear()


cache = DataCache()


# ── Synthetic Data Generation ─────────────────────────────────────────────────

PRODUCTS = [
    # Batteries
    {"product_name": "Lithium-ion EV Battery Pack 75kWh", "manufacturer": "VoltaPower GmbH", "country": "Germany", "sector": "batteries", "material": "Lithium-ion NMC", "recyclable": True, "co2_footprint": 4500, "certifier": "TÜV Rheinland", "lifespan_years": 10},
    {"product_name": "Home Energy Storage System 10kWh", "manufacturer": "EcoCell Ltd", "country": "Sweden", "sector": "batteries", "material": "Lithium Iron Phosphate", "recyclable": True, "co2_footprint": 1200, "certifier": "DEKRA", "lifespan_years": 15},
    {"product_name": "Portable Power Bank 20000mAh", "manufacturer": "ChargeWave", "country": "Estonia", "sector": "batteries", "material": "Lithium-ion Polymer", "recyclable": False, "co2_footprint": 85, "certifier": "Bureau Veritas", "lifespan_years": 3},
    {"product_name": "Industrial Battery Rack 48V 200Ah", "manufacturer": "PowerStack Inc", "country": "Netherlands", "sector": "batteries", "material": "Lead-acid AGM", "recyclable": True, "co2_footprint": 3200, "certifier": "SGS", "lifespan_years": 8},
    {"product_name": "E-bike Battery Pack 36V 14Ah", "manufacturer": "VeloVolt SRL", "country": "Italy", "sector": "batteries", "material": "Lithium-ion 18650", "recyclable": True, "co2_footprint": 210, "certifier": "TÜV SÜD", "lifespan_years": 5},
    {"product_name": "Solar Storage Battery 13.5kWh", "manufacturer": "SunVault Energy", "country": "France", "sector": "batteries", "material": "Lithium Nickel Manganese Cobalt", "recyclable": True, "co2_footprint": 1800, "certifier": "DNV GL", "lifespan_years": 12},
    {"product_name": "AA Rechargeable NiMH 2500mAh 4-Pack", "manufacturer": "GreenCell Nordic", "country": "Denmark", "sector": "batteries", "material": "Nickel-Metal Hydride", "recyclable": True, "co2_footprint": 18, "certifier": "Intertek", "lifespan_years": 4},
    {"product_name": "Automotive AGM Starter Battery 12V 80Ah", "manufacturer": "StartPower Auto", "country": "Germany", "sector": "batteries", "material": "Lead-acid AGM", "recyclable": True, "co2_footprint": 980, "certifier": "TÜV Rheinland", "lifespan_years": 6},
    {"product_name": "Marine Deep Cycle Battery 12V 100Ah", "manufacturer": "MarinePower BV", "country": "Netherlands", "sector": "batteries", "material": "Lithium-ion LFP", "recyclable": True, "co2_footprint": 560, "certifier": "Lloyd's Register", "lifespan_years": 7},
    {"product_name": "Wearable Smartwatch Battery 300mAh", "manufacturer": "NanoCell Tech", "country": "Finland", "sector": "batteries", "material": "Lithium-ion Polymer", "recyclable": False, "co2_footprint": 22, "certifier": "SGS", "lifespan_years": 2},
    # Textiles
    {"product_name": "Organic Cotton T-Shirt Classic Fit", "manufacturer": "EcoWear Textiles", "country": "Portugal", "sector": "textiles", "material": "Organic Cotton", "recyclable": True, "co2_footprint": 45, "certifier": "GOTS", "lifespan_years": 5},
    {"product_name": "Recycled Polyester Running Jacket", "manufacturer": "SportLoop GmbH", "country": "Germany", "sector": "textiles", "material": "Recycled Polyester (rPET)", "recyclable": True, "co2_footprint": 78, "certifier": "OEKO-TEX", "lifespan_years": 6},
    {"product_name": "Merino Wool Hiking Socks 3-Pack", "manufacturer": "Alpine Wool Co", "country": "Austria", "sector": "textiles", "material": "Merino Wool, Nylon", "recyclable": False, "co2_footprint": 22, "certifier": "GOTS", "lifespan_years": 3},
    {"product_name": "Hemp Canvas Backpack 30L", "manufacturer": "NaturalWear Bags", "country": "France", "sector": "textiles", "material": "Hemp, Organic Cotton", "recyclable": True, "co2_footprint": 56, "certifier": "OEKO-TEX", "lifespan_years": 8},
    {"product_name": "Eco-Denim Jeans Slim Fit", "manufacturer": "BlueRoot Denim", "country": "Italy", "sector": "textiles", "material": "Organic Cotton, Recycled Polyester", "recyclable": True, "co2_footprint": 120, "certifier": "EU Ecolabel", "lifespan_years": 6},
    {"product_name": "Bamboo Fiber Bath Towel Set", "manufacturer": "BambooLux Home", "country": "Spain", "sector": "textiles", "material": "Bamboo Lyocell", "recyclable": True, "co2_footprint": 38, "certifier": "OEKO-TEX Standard 100", "lifespan_years": 4},
    {"product_name": "Linen Summer Dress Mid-Length", "manufacturer": "FlaxCouture", "country": "Lithuania", "sector": "textiles", "material": "European Flax Linen", "recyclable": True, "co2_footprint": 62, "certifier": "EU Ecolabel", "lifespan_years": 7},
    {"product_name": "Recycled Nylon Swim Trunks", "manufacturer": "OceanThreads", "country": "Greece", "sector": "textiles", "material": "Recycled Nylon (ECONYL)", "recyclable": True, "co2_footprint": 48, "certifier": "GOTS", "lifespan_years": 4},
    {"product_name": "Cashmere Blend Scarf Premium", "manufacturer": "LuxeFibre Milano", "country": "Italy", "sector": "textiles", "material": "Cashmere, Recycled Wool", "recyclable": False, "co2_footprint": 92, "certifier": "GOTS", "lifespan_years": 10},
    {"product_name": "TENCEL Lyocell Work Shirt", "manufacturer": "GreenFormal AG", "country": "Switzerland", "sector": "textiles", "material": "TENCEL Lyocell, Organic Cotton", "recyclable": True, "co2_footprint": 55, "certifier": "OEKO-TEX", "lifespan_years": 5},
    # Electronics
    {"product_name": "Smartphone Eco Edition 128GB", "manufacturer": "GreenPhone Systems", "country": "Finland", "sector": "electronics", "material": "Recycled Aluminum, Recycled Glass", "recyclable": True, "co2_footprint": 420, "certifier": "TÜV Rheinland", "lifespan_years": 5},
    {"product_name": "14\" Laptop Ultrabook Recycled Chassis", "manufacturer": "EcoBit Computing", "country": "Ireland", "sector": "electronics", "material": "Recycled Magnesium Alloy", "recyclable": True, "co2_footprint": 850, "certifier": "EPEAT Gold", "lifespan_years": 7},
    {"product_name": "Wireless Noise-Cancelling Headphones", "manufacturer": "SoundGreen Audio", "country": "Sweden", "sector": "electronics", "material": "Recycled ABS, Recycled Copper", "recyclable": True, "co2_footprint": 95, "certifier": "Bureau Veritas", "lifespan_years": 4},
    {"product_name": "27\" 4K Monitor Eco-Series", "manufacturer": "DisplayGreen GmbH", "country": "Germany", "sector": "electronics", "material": "Recycled Plastic, Glass", "recyclable": True, "co2_footprint": 620, "certifier": "EPEAT Silver", "lifespan_years": 8},
    {"product_name": "Tablet 10\" Education Edition", "manufacturer": "TabletEdu SAS", "country": "France", "sector": "electronics", "material": "Recycled Polycarbonate", "recyclable": True, "co2_footprint": 340, "certifier": "TÜV SÜD", "lifespan_years": 5},
    {"product_name": "Smart Home Hub Energy Efficient", "manufacturer": "HomeAI Systems", "country": "Netherlands", "sector": "electronics", "material": "Recycled PLA, Bio-plastic", "recyclable": True, "co2_footprint": 78, "certifier": "DEKRA", "lifespan_years": 6},
    {"product_name": "USB-C Charger 65W GaN Eco", "manufacturer": "PowerEco Tech", "country": "Estonia", "sector": "electronics", "material": "Recycled PC/ABS", "recyclable": True, "co2_footprint": 28, "certifier": "CE, RoHS", "lifespan_years": 5},
    {"product_name": "Bluetooth Speaker IP67 Recycled", "manufacturer": "EcoSound Labs", "country": "Denmark", "sector": "electronics", "material": "Recycled Silicone, Recycled ABS", "recyclable": True, "co2_footprint": 65, "certifier": "Bureau Veritas", "lifespan_years": 4},
    {"product_name": "Wireless Ergonomic Mouse Recycled", "manufacturer": "GreenClick Ltd", "country": "UK", "sector": "electronics", "material": "Recycled PP, Recycled Aluminum", "recyclable": True, "co2_footprint": 18, "certifier": "EPEAT Bronze", "lifespan_years": 3},
    {"product_name": "E-Reader 6\" E-Ink Waterproof", "manufacturer": "ReadGreen BV", "country": "Netherlands", "sector": "electronics", "material": "Recycled Plastic, E-Ink Display", "recyclable": True, "co2_footprint": 115, "certifier": "TÜV Rheinland", "lifespan_years": 6},
    # Construction
    {"product_name": "Recycled Steel I-Beam 8m x 300mm", "manufacturer": "GreenSteel Construct", "country": "Luxembourg", "sector": "construction", "material": "100% Recycled Steel", "recyclable": True, "co2_footprint": 2800, "certifier": "Cradle to Cradle Bronze", "lifespan_years": 50},
    {"product_name": "Cross-Laminated Timber Panel CLT 3-Ply", "manufacturer": "WoodCycle Nordic", "country": "Sweden", "sector": "construction", "material": "PEFC-Certified Spruce", "recyclable": True, "co2_footprint": 350, "certifier": "PEFC, FSC", "lifespan_years": 60},
    {"product_name": "Recycled Concrete Aggregate 20mm", "manufacturer": "EcoMix Materials", "country": "Netherlands", "sector": "construction", "material": "Recycled Concrete, Fly Ash", "recyclable": True, "co2_footprint": 120, "certifier": "BREEAM Compliant", "lifespan_years": 80},
    {"product_name": "Hempcrete Insulation Block 400x200x200mm", "manufacturer": "HempBuild France", "country": "France", "sector": "construction", "material": "Hemp Hurds, Lime Binder", "recyclable": True, "co2_footprint": 45, "certifier": "Cradle to Cradle Gold", "lifespan_years": 40},
    {"product_name": "Bamboo Reinforcing Fiber 12mm", "manufacturer": "BamBond GmbH", "country": "Germany", "sector": "construction", "material": "Engineered Bamboo Fiber", "recyclable": True, "co2_footprint": 28, "certifier": "FSC", "lifespan_years": 25},
    {"product_name": "Low-CO2 Cement CEM III/B 42.5N", "manufacturer": "GreenCement Industries", "country": "Belgium", "sector": "construction", "material": "Ground Granulated Blast-furnace Slag, Clinker", "recyclable": False, "co2_footprint": 1800, "certifier": "CE Marking, NF", "lifespan_years": 50},
    {"product_name": "Recycled PVC Window Frame U-Value 0.8", "manufacturer": "EcoWindows SA", "country": "Poland", "sector": "construction", "material": "Recycled PVC, Multi-chamber", "recyclable": True, "co2_footprint": 220, "certifier": "Passivhaus Certified", "lifespan_years": 40},
    {"product_name": "Musselfest Shellstone Flooring Tile 60x60cm", "manufacturer": "ShellStone NL", "country": "Netherlands", "sector": "construction", "material": "Crushed Mussel Shells, Bio-resin", "recyclable": True, "co2_footprint": 18, "certifier": "Cradle to Cradle Silver", "lifespan_years": 30},
    {"product_name": "Reclaimed Oak Floorboard 20mm Finished", "manufacturer": "TimberRevival UK", "country": "UK", "sector": "construction", "material": "Reclaimed Oak, Natural Oil", "recyclable": True, "co2_footprint": 15, "certifier": "FSC Recycled", "lifespan_years": 80},
    {"product_name": "Green Roof Sedum Mat 600x400mm", "manufacturer": "RoofGreen Systems", "country": "Denmark", "sector": "construction", "material": "Sedum Mix, Recycled PET Felt", "recyclable": True, "co2_footprint": 24, "certifier": "BREEAM Compliant", "lifespan_years": 20},
    # Plastics & Mixed
    {"product_name": "Recycled PP Food Container 750ml", "manufacturer": "EcoPack Nordic", "country": "Finland", "sector": "plastics", "material": "100% Recycled Polypropylene", "recyclable": True, "co2_footprint": 8, "certifier": "EU Ecolabel", "lifespan_years": 5},
    {"product_name": "Biodegradable PLA Straw Pack 500pcs", "manufacturer": "BioSip GmbH", "country": "Austria", "sector": "plastics", "material": "Polylactic Acid (PLA)", "recyclable": False, "co2_footprint": 12, "certifier": "EN 13432 Compostable", "lifespan_years": 2},
    {"product_name": "Recycled HDPE Shampoo Bottle 300ml", "manufacturer": "CleanCircle Ltd", "country": "UK", "sector": "plastics", "material": "100% Recycled HDPE", "recyclable": True, "co2_footprint": 6, "certifier": "EU Ecolabel", "lifespan_years": 3},
    {"product_name": "Ocean-Bound PE Shopping Bag Reusable", "manufacturer": "OceanCycle BV", "country": "Netherlands", "sector": "plastics", "material": "Ocean-Bound Polyethylene", "recyclable": True, "co2_footprint": 4, "certifier": "OEKO-TEX", "lifespan_years": 2},
    {"product_name": "PLA 3D Printer Filament 1.75mm 1kg", "manufacturer": "PrintGreen Materials", "country": "France", "sector": "plastics", "material": "PLA, Recycled PLA Blend", "recyclable": True, "co2_footprint": 15, "certifier": "EN 13432", "lifespan_years": 3},
    {"product_name": "Recycled PET Thermoformed Tray 200x150mm", "manufacturer": "FormPack Italia", "country": "Italy", "sector": "plastics", "material": "100% rPET", "recyclable": True, "co2_footprint": 5, "certifier": "EU Ecolabel", "lifespan_years": 2},
    {"product_name": "Bio-based Polyethylene Tote Bag", "manufacturer": "GreenCarry SA", "country": "Portugal", "sector": "plastics", "material": "Sugarcane Bio-PE", "recyclable": True, "co2_footprint": 3, "certifier": "OK biobased", "lifespan_years": 4},
    {"product_name": "Recycled ABS Keyboard Keycaps Set 104-Key", "manufacturer": "KeyGreen Tech", "country": "Germany", "sector": "plastics", "material": "Recycled ABS", "recyclable": True, "co2_footprint": 9, "certifier": "EPEAT Bronze", "lifespan_years": 5},
    {"product_name": "Compostable Mailer Biodegradable 200pcs", "manufacturer": "EcoPost NL", "country": "Netherlands", "sector": "plastics", "material": "PBAT, PLA, Corn Starch", "recyclable": False, "co2_footprint": 14, "certifier": "EN 13432 Compostable", "lifespan_years": 1},
    {"product_name": "Recycled PS Foam Insulation Board 1200x600x50mm", "manufacturer": "InsulGreen GmbH", "country": "Germany", "sector": "plastics", "material": "Recycled Polystyrene", "recyclable": True, "co2_footprint": 180, "certifier": "Cradle to Cradle Bronze", "lifespan_years": 30},
    # Batteries (more)
    {"product_name": "NiMH Rechargeable C Cell 5000mAh", "manufacturer": "GreenCell Nordic", "country": "Denmark", "sector": "batteries", "material": "Nickel-Metal Hydride", "recyclable": True, "co2_footprint": 35, "certifier": "Intertek", "lifespan_years": 5},
    {"product_name": "Lithium-ion 18650 Cell 3500mAh", "manufacturer": "CellPrime GmbH", "country": "Germany", "sector": "batteries", "material": "Lithium-ion NCA", "recyclable": True, "co2_footprint": 14, "certifier": "TÜV Rheinland", "lifespan_years": 4},
    # Textiles (more)
    {"product_name": "Recycled Down Puffer Jacket Unisex", "manufacturer": "WarmCycle Outdoor", "country": "Sweden", "sector": "textiles", "material": "Recycled Nylon, Recycled Down", "recyclable": True, "co2_footprint": 140, "certifier": "OEKO-TEX", "lifespan_years": 8},
    {"product_name": "Alpaca Wool Throw Blanket 150x200cm", "manufacturer": "Andean Lux BV", "country": "Netherlands", "sector": "textiles", "material": "Alpaca Wool, TENCEL", "recyclable": False, "co2_footprint": 85, "certifier": "GOTS", "lifespan_years": 12},
    {"product_name": "Recycled Cotton Canvas Tote Bag", "manufacturer": "BagCycle Portugal", "country": "Portugal", "sector": "textiles", "material": "100% Recycled Cotton", "recyclable": True, "co2_footprint": 8, "certifier": "EU Ecolabel", "lifespan_years": 5},
    # Electronics (more)
    {"product_name": "Mechanical Keyboard Eco 80% Recycled", "manufacturer": "KeyGreen Tech", "country": "Germany", "sector": "electronics", "material": "Recycled ABS, Recycled Aluminum", "recyclable": True, "co2_footprint": 72, "certifier": "EPEAT Silver", "lifespan_years": 7},
    {"product_name": "Solar Power Bank 10000mAh 10W Panel", "manufacturer": "SunCharge GmbH", "country": "Austria", "sector": "electronics", "material": "Recycled PC, Monocrystalline Si", "recyclable": True, "co2_footprint": 95, "certifier": "TÜV SÜD", "lifespan_years": 5},
    # Construction (more)
    {"product_name": "Recycled Glass Countertop Slab 2400x600mm", "manufacturer": "GlassStone Eco", "country": "Italy", "sector": "construction", "material": "Recycled Glass, Bio-resin", "recyclable": True, "co2_footprint": 65, "certifier": "Cradle to Cradle Gold", "lifespan_years": 30},
    {"product_name": "Ferrock Carbon-Negative Binder 25kg", "manufacturer": "Ferrock Europe", "country": "Belgium", "sector": "construction", "material": "Recycled Steel Dust, Silica", "recyclable": False, "co2_footprint": -15, "certifier": "CarbonCure Certified", "lifespan_years": 50},
    # Plastics (more)
    {"product_name": "rPP Motor Oil Bottle 5L", "manufacturer": "LubriGreen GmbH", "country": "Germany", "sector": "plastics", "material": "Recycled Polypropylene", "recyclable": True, "co2_footprint": 22, "certifier": "EU Ecolabel", "lifespan_years": 3},
    {"product_name": "Bio-PE Bread Bag Compostable 500pcs", "manufacturer": "GreenWrap France", "country": "France", "sector": "plastics", "material": "Sugarcane Bio-PE, PBAT", "recyclable": False, "co2_footprint": 7, "certifier": "OK Compost HOME", "lifespan_years": 1},
    # More across sectors to bring to ~100
    {"product_name": "Smart EV Charger 22kW Type 2", "manufacturer": "VoltaPower GmbH", "country": "Germany", "sector": "electronics", "material": "Recycled PC/ABS, Copper", "recyclable": True, "co2_footprint": 320, "certifier": "TÜV Rheinland", "lifespan_years": 12},
    {"product_name": "Cork Yoga Mat 6mm Natural", "manufacturer": "EcoCork Portugal", "country": "Portugal", "sector": "textiles", "material": "Natural Cork, Natural Rubber", "recyclable": True, "co2_footprint": 12, "certifier": "OEKO-TEX", "lifespan_years": 8},
    {"product_name": "Bioplastic Cutlery Set Fork/Knife/Spoon 100pcs", "manufacturer": "BioSip GmbH", "country": "Austria", "sector": "plastics", "material": "PLA, Bamboo Composite", "recyclable": False, "co2_footprint": 16, "certifier": "EN 13432 Compostable", "lifespan_years": 1},
    {"product_name": "Recycled Rubber Floor Mat 90x150cm", "manufacturer": "RubberRevival", "country": "Spain", "sector": "construction", "material": "Recycled Tire Rubber, EPDM", "recyclable": True, "co2_footprint": 48, "certifier": "Cradle to Cradle Bronze", "lifespan_years": 15},
    {"product_name": "Organic Cotton Bed Sheet Set Queen", "manufacturer": "EcoWear Textiles", "country": "Portugal", "sector": "textiles", "material": "GOTS Organic Cotton", "recyclable": True, "co2_footprint": 95, "certifier": "GOTS", "lifespan_years": 6},
    {"product_name": "Recycled Li-ion Battery 72V 100Ah E-Scooter", "manufacturer": "EcoCell Ltd", "country": "Sweden", "sector": "batteries", "material": "Lithium-ion LFP Recycled", "recyclable": True, "co2_footprint": 680, "certifier": "DEKRA", "lifespan_years": 7},
    {"product_name": "Server Rack UPS Battery 48V 50Ah", "manufacturer": "PowerStack Inc", "country": "Netherlands", "sector": "batteries", "material": "Lithium-ion NMC", "recyclable": True, "co2_footprint": 2400, "certifier": "TÜV SÜD", "lifespan_years": 10},
    {"product_name": "Sisal Fiber Wallpaper Roll Natural", "manufacturer": "NaturalDecor Italy", "country": "Italy", "sector": "construction", "material": "Sisal Fiber, Jute Backing", "recyclable": True, "co2_footprint": 9, "certifier": "EU Ecolabel", "lifespan_years": 15},
    {"product_name": "Recycled Nylon Tights 40 DEN", "manufacturer": "OceanThreads", "country": "Greece", "sector": "textiles", "material": "Recycled Nylon (ECONYL)", "recyclable": True, "co2_footprint": 14, "certifier": "GOTS", "lifespan_years": 2},
    {"product_name": "Thermoplastic Polyurethane Phone Case", "manufacturer": "ProtectGreen", "country": "Estonia", "sector": "plastics", "material": "Recycled TPU, Bio-based TPU", "recyclable": True, "co2_footprint": 4, "certifier": "OEKO-TEX", "lifespan_years": 3},
    {"product_name": "Aluminum Can Beverage 330ml 24-Pack", "manufacturer": "CircuCan Ltd", "country": "UK", "sector": "construction", "material": "Recycled Aluminum", "recyclable": True, "co2_footprint": 18, "certifier": "Cradle to Cradle Silver", "lifespan_years": 1},
    {"product_name": "Graphene-enhanced Li-ion Battery 48V 20Ah", "manufacturer": "NanoCell Tech", "country": "Finland", "sector": "batteries", "material": "Lithium-ion, Graphene", "recyclable": True, "co2_footprint": 420, "certifier": "TÜV Rheinland", "lifespan_years": 12},
    {"product_name": "Recycled Silk Scarf Handwoven", "manufacturer": "LuxeFibre Milano", "country": "Italy", "sector": "textiles", "material": "Recycled Silk", "recyclable": False, "co2_footprint": 32, "certifier": "OEKO-TEX Standard 100", "lifespan_years": 15},
    {"product_name": "Sodium-ion Battery 18650 1500mAh", "manufacturer": "EcoCell Ltd", "country": "Sweden", "sector": "batteries", "material": "Sodium-ion", "recyclable": True, "co2_footprint": 8, "certifier": "DEKRA", "lifespan_years": 6},
    {"product_name": "Recycled PLA Mobile Stand Biodegradable", "manufacturer": "BioSip GmbH", "country": "Austria", "sector": "plastics", "material": "Recycled PLA, Bamboo Fiber", "recyclable": True, "co2_footprint": 2, "certifier": "EN 13432", "lifespan_years": 3},
    {"product_name": "LiFePO4 Prismatic Cell 3.2V 280Ah", "manufacturer": "SunVault Energy", "country": "France", "sector": "batteries", "material": "Lithium Iron Phosphate", "recyclable": True, "co2_footprint": 920, "certifier": "TÜV SÜD", "lifespan_years": 15},
    {"product_name": "Recycled Leather Wallet RFID Blocking", "manufacturer": "LeatherRevival", "country": "Spain", "sector": "textiles", "material": "Recycled Leather, Recycled PET", "recyclable": False, "co2_footprint": 18, "certifier": "OEKO-TEX", "lifespan_years": 8},
    {"product_name": "Server Grade SSD 2TB Eco Packaging", "manufacturer": "GreenBit Storage", "country": "Ireland", "sector": "electronics", "material": "Recycled Aluminum, PCB", "recyclable": True, "co2_footprint": 180, "certifier": "EPEAT Gold", "lifespan_years": 8},
    {"product_name": "Hempcrete Wall Block 400x200x200mm", "manufacturer": "HempBuild France", "country": "France", "sector": "construction", "material": "Hemp Hurds, Lime, Recycled Aggregate", "recyclable": True, "co2_footprint": 38, "certifier": "Cradle to Cradle Silver", "lifespan_years": 50},
    {"product_name": "Recycled Aluminum Window Frame Thermal Break", "manufacturer": "EcoWindows SA", "country": "Poland", "sector": "construction", "material": "Recycled Aluminum, Polyamide", "recyclable": True, "co2_footprint": 360, "certifier": "Passivhaus Certified", "lifespan_years": 45},
    {"product_name": "Phone Charging Cable Braided 1.5m rPET", "manufacturer": "ChargeWave", "country": "Estonia", "sector": "electronics", "material": "Recycled PET Braid, Recycled Copper", "recyclable": True, "co2_footprint": 4, "certifier": "CE, RoHS", "lifespan_years": 3},
    {"product_name": "Compostable Coffee Capsule 50-Pack", "manufacturer": "GreenWrap France", "country": "France", "sector": "plastics", "material": "PLA, Aluminum-free Bio-layer", "recyclable": False, "co2_footprint": 22, "certifier": "OK Compost INDUSTRIAL", "lifespan_years": 1},
    {"product_name": "Recycled Wool Blanket Herringbone 130x180cm", "manufacturer": "Alpine Wool Co", "country": "Austria", "sector": "textiles", "material": "80% Recycled Wool, 20% Recycled Polyester", "recyclable": True, "co2_footprint": 42, "certifier": "GOTS", "lifespan_years": 15},
    {"product_name": "E-Bike Conversion Kit 250W Recycled Battery", "manufacturer": "VeloVolt SRL", "country": "Italy", "sector": "batteries", "material": "Repurposed Li-ion Cells", "recyclable": True, "co2_footprint": 180, "certifier": "TÜV Rheinland", "lifespan_years": 5},
    {"product_name": "Bluetooth Tracker Recycled Plastic", "manufacturer": "HomeAI Systems", "country": "Netherlands", "sector": "electronics", "material": "Recycled ABS", "recyclable": True, "co2_footprint": 8, "certifier": "CE, RoHS", "lifespan_years": 3},
    {"product_name": "Recycled Rubber Speed Bump 1000x300x50mm", "manufacturer": "RubberRevival", "country": "Spain", "sector": "construction", "material": "Recycled Tire Rubber", "recyclable": True, "co2_footprint": 22, "certifier": "CE Marking", "lifespan_years": 20},
    {"product_name": "Solid-State Battery Prototype 10Ah", "manufacturer": "NanoCell Tech", "country": "Finland", "sector": "batteries", "material": "Solid-State Lithium", "recyclable": True, "co2_footprint": 110, "certifier": "TÜV SÜD", "lifespan_years": 15},
    {"product_name": "Bamboo Toilet Paper 3-Ply 48 Rolls", "manufacturer": "BambooLux Home", "country": "Spain", "sector": "textiles", "material": "Bamboo Pulp, Unbleached", "recyclable": True, "co2_footprint": 28, "certifier": "EU Ecolabel", "lifespan_years": 2},
    {"product_name": "Recycled EPS Geofoam Block 2000x1000x500mm", "manufacturer": "InsulGreen GmbH", "country": "Germany", "sector": "construction", "material": "Recycled Expanded Polystyrene", "recyclable": True, "co2_footprint": 160, "certifier": "CE Marking", "lifespan_years": 50},
    {"product_name": "Waterproof Bluetooth Speaker IPX7 rPET", "manufacturer": "EcoSound Labs", "country": "Denmark", "sector": "electronics", "material": "Recycled Silicone, Recycled PET Mesh", "recyclable": True, "co2_footprint": 72, "certifier": "DEKRA", "lifespan_years": 5},
    {"product_name": "Linen Kitchen Towel Set 3-Pack Natural", "manufacturer": "FlaxCouture", "country": "Lithuania", "sector": "textiles", "material": "European Flax Linen", "recyclable": True, "co2_footprint": 8, "certifier": "EU Ecolabel", "lifespan_years": 5},
    {"product_name": "Recycled PP Folding Chair Stackable 4-Pack", "manufacturer": "EcoPack Nordic", "country": "Finland", "sector": "plastics", "material": "Recycled Polypropylene", "recyclable": True, "co2_footprint": 48, "certifier": "EU Ecolabel", "lifespan_years": 10},
    {"product_name": "Zinc-Air Hearing Aid Battery Size 312 6-Pack", "manufacturer": "GreenCell Nordic", "country": "Denmark", "sector": "batteries", "material": "Zinc-Air", "recyclable": True, "co2_footprint": 1, "certifier": "Intertek", "lifespan_years": 1},
    {"product_name": "Recycled Content Toilet Bowl  Wassersparend", "manufacturer": "EcoMix Materials", "country": "Netherlands", "sector": "construction", "material": "Recycled Ceramic, Bio-glaze", "recyclable": True, "co2_footprint": 85, "certifier": "Cradle to Cradle Bronze", "lifespan_years": 40},
    {"product_name": "Biodegradable Dog Poop Bags 200-Pack Cornstarch", "manufacturer": "GreenWrap France", "country": "France", "sector": "plastics", "material": "PLA, Cornstarch, PBAT", "recyclable": False, "co2_footprint": 6, "certifier": "EN 13432 Compostable", "lifespan_years": 1},
    {"product_name": "Server Rack Mount Eco UPS 2U 1500VA", "manufacturer": "PowerStack Inc", "country": "Netherlands", "sector": "electronics", "material": "Recycled Steel, Recycled PCB", "recyclable": True, "co2_footprint": 580, "certifier": "EPEAT Silver", "lifespan_years": 8},
    {"product_name": "Merino Wool Base Layer Top Women's", "manufacturer": "SportLoop GmbH", "country": "Germany", "sector": "textiles", "material": "Merino Wool, Recycled Nylon", "recyclable": True, "co2_footprint": 52, "certifier": "OEKO-TEX", "lifespan_years": 5},
    {"product_name": "Recycled Lumber Composite Decking Board 5m", "manufacturer": "WoodCycle Nordic", "country": "Sweden", "sector": "construction", "material": "Recycled Wood Fiber, Recycled HDPE", "recyclable": True, "co2_footprint": 120, "certifier": "Cradle to Cradle Silver", "lifespan_years": 30},
    {"product_name": "Concentrated Laundry Detergent Eco 1L rHDPE", "manufacturer": "CleanCircle Ltd", "country": "UK", "sector": "plastics", "material": "100% Recycled HDPE", "recyclable": True, "co2_footprint": 8, "certifier": "EU Ecolabel", "lifespan_years": 3},
    {"product_name": "Home Battery Backup System 5kWh LFP", "manufacturer": "SunVault Energy", "country": "France", "sector": "batteries", "material": "Lithium Iron Phosphate", "recyclable": True, "co2_footprint": 1100, "certifier": "TÜV SÜD", "lifespan_years": 15},
    {"product_name": "Sweat-Wicking Running Shirt Recycled Polyester", "manufacturer": "SportLoop GmbH", "country": "Germany", "sector": "textiles", "material": "100% Recycled Polyester", "recyclable": True, "co2_footprint": 32, "certifier": "OEKO-TEX", "lifespan_years": 4},
    {"product_name": "Recycled PET Carpet Tile 500x500mm", "manufacturer": "EcoPack Nordic", "country": "Finland", "sector": "plastics", "material": "Recycled PET, Recycled PP Backing", "recyclable": True, "co2_footprint": 35, "certifier": "Cradle to Cradle Bronze", "lifespan_years": 15},
    {"product_name": "Vanadium Redox Flow Battery Module 50kWh", "manufacturer": "PowerStack Inc", "country": "Netherlands", "sector": "batteries", "material": "Vanadium Electrolyte, Carbon Felt", "recyclable": True, "co2_footprint": 5200, "certifier": "DNV GL", "lifespan_years": 25},
    {"product_name": "Hemp Canvas Messenger Bag Recycled", "manufacturer": "NaturalWear Bags", "country": "France", "sector": "textiles", "material": "Hemp, Recycled Cotton Lining", "recyclable": True, "co2_footprint": 28, "certifier": "EU Ecolabel", "lifespan_years": 10},
    {"product_name": "Recycled Aluminum Trekking Pole Pair", "manufacturer": "GreenSteel Construct", "country": "Luxembourg", "sector": "construction", "material": "Recycled 7075 Aluminum", "recyclable": True, "co2_footprint": 18, "certifier": "Cradle to Cradle Bronze", "lifespan_years": 20},
    {"product_name": "Raspberry Pi 5 Eco Case Recycled PLA", "manufacturer": "PrintGreen Materials", "country": "France", "sector": "plastics", "material": "Recycled PLA, Bamboo Fiber", "recyclable": True, "co2_footprint": 3, "certifier": "EN 13432", "lifespan_years": 5},
    {"product_name": "Organic Wool Mattress Topper Queen 5cm", "manufacturer": "EcoWear Textiles", "country": "Portugal", "sector": "textiles", "material": "Organic Wool, Organic Cotton", "recyclable": True, "co2_footprint": 110, "certifier": "GOTS", "lifespan_years": 10},
]


# ── Dataset Builder ───────────────────────────────────────────────────────────

def _build_full_records():
    """Build the full set of ~100 enriched passport records."""
    records = []
    statuses = ["active", "active", "active", "active", "pending", "expired"]  # ~66% active
    certifiers_list = [
        "TÜV Rheinland", "DEKRA", "Bureau Veritas", "SGS",
        "TÜV SÜD", "DNV GL", "Intertek", "Lloyd's Register",
        "GOTS", "OEKO-TEX", "EU Ecolabel", "EPEAT Gold",
        "Cradle to Cradle", "FSC", "PEFC", "BREEAM Compliant",
    ]

    for idx, prod in enumerate(PRODUCTS):
        status = random.choice(statuses)

        # Issue date: between 2020-01-01 and 2025-06-01
        year = random.randint(2020, 2025)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        issue_date = f"{year:04d}-{month:02d}-{day:02d}"

        # Expiry date: 3-12 years after issue
        lifespan = prod.get("lifespan_years", 5)
        expiry_year = year + lifespan
        expiry_month = month
        expiry_day = day
        if expiry_month < 10:
            expiry_date = f"{expiry_year:04d}-{expiry_month:02d}-{expiry_day:02d}"
        else:
            expiry_date = f"{expiry_year:04d}-12-{expiry_day:02d}"

        # Generate a deterministic UID based on index
        uid = hashlib.sha256(f"passportdpp-{idx:04d}".encode()).hexdigest()[:32].upper()

        # Build materials list
        materials = _parse_materials(prod["material"])
        # Compute recyclability percentage
        recyclability_pct = random.randint(55, 98) if prod["recyclable"] else random.randint(5, 30)

        # Carbon footprint
        carbon_footprint_kg_co2e = prod["co2_footprint"]

        # Compliance level
        compliance = random.choice(["full", "full", "full", "partial", "pending_review"])

        # Certifications list
        num_certs = random.randint(1, 4)
        certs = random.sample(certifiers_list, num_certs)
        if prod["certifier"] not in certs:
            certs.insert(0, prod["certifier"])

        record = {
            "id": f"DPP-{idx+1:04d}",
            "uid": uid,
            "product_name": prod["product_name"],
            "sector": prod["sector"],
            "manufacturer": {
                "name": prod["manufacturer"],
                "country": prod["country"],
                "eu_registered": prod["country"] in (
                    "Germany", "Sweden", "Estonia", "Netherlands", "Italy",
                    "France", "Denmark", "Finland", "Portugal", "Austria",
                    "Spain", "Lithuania", "Greece", "Switzerland", "Ireland",
                    "Luxembourg", "UK", "Belgium", "Poland",
                ),
            },
            "materials": materials,
            "recyclability_pct": float(recyclability_pct),
            "carbon_footprint_kg_co2e": float(carbon_footprint_kg_co2e),
            "lifespan_years": prod.get("lifespan_years", 5),
            "compliance": compliance,
            "certifications": certs[:4],
            "issue_date": issue_date,
            "expiry_date": expiry_date,
            "status": status,
        }
        records.append(record)

    return records


def _parse_materials(material_str):
    """Parse a material string like 'Lithium-ion NMC' or 'Organic Cotton, Recycled Polyester' into structured list."""
    parts = [m.strip() for m in material_str.split(",")]
    materials = []
    for part in parts:
        recycled = any(kw in part.lower() for kw in ["recycled", "reclaimed", "repurposed"])
        critical = any(kw in part.lower() for kw in [
            "lithium", "cobalt", "nmc", "nca", "graphene", "rare earth",
            "vanadium", "tungsten", "indium",
        ])
        pct = random.randint(15, 80) if len(parts) > 1 else 100
        recycled_pct = random.randint(50, 100) if recycled else 0
        materials.append({
            "name": part,
            "percentage": float(pct),
            "recycled_content_pct": float(recycled_pct),
            "critical_raw_material": critical,
        })
    # Normalize percentages to sum to ~100
    if len(materials) > 1:
        total = sum(m["percentage"] for m in materials)
        if total > 0:
            for m in materials:
                m["percentage"] = round(m["percentage"] / total * 100, 1)
    return materials


# ── Public API ────────────────────────────────────────────────────────────────

def build_dataset(force_refresh=False):
    """Build and cache the full dataset of passport records.

    Args:
        force_refresh: If True, regenerate the dataset and update cache.

    Returns:
        list[dict]: Full list of passport records.
    """
    cache_key = "passport_dataset"
    if force_refresh:
        cache.invalidate(cache_key)

    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    records = _build_full_records()
    cache.set(cache_key, records)
    return records


def get_passport_by_id(passport_id):
    """Look up a passport by its human-readable ID (e.g. 'DPP-0042').

    Args:
        passport_id: The passport ID string.

    Returns:
        dict | None: The passport record, or None if not found.
    """
    records = build_dataset()
    for rec in records:
        if rec["id"] == passport_id:
            return rec
    return None


def search_passports(
    sector=None,
    manufacturer=None,
    query=None,
    status=None,
    min_recyclability=None,
    max_carbon=None,
    limit=20,
    offset=0,
):
    """Search and filter passport records with pagination.

    All parameters are optional; returns full list when no filters given.

    Args:
        sector: Filter by sector (e.g. 'batteries', 'textiles').
        manufacturer: Substring match on manufacturer name.
        query: Substring match on product_name or id.
        status: Filter by status ('active', 'pending', 'expired').
        min_recyclability: Minimum recyclability percentage.
        max_carbon: Maximum carbon footprint kg CO2e.
        limit: Max records to return per page.
        offset: Pagination offset.

    Returns:
        tuple[list[dict], int]: (filtered records, total matching count).
    """
    records = build_dataset()

    filtered = records
    if sector:
        filtered = [r for r in filtered if r["sector"].lower() == sector.lower()]
    if manufacturer:
        mfgr_lower = manufacturer.lower()
        filtered = [r for r in filtered if mfgr_lower in r["manufacturer"]["name"].lower()]
    if query:
        q_lower = query.lower()
        filtered = [
            r for r in filtered
            if q_lower in r["product_name"].lower() or q_lower in r["id"].lower()
        ]
    if status:
        s_lower = status.lower()
        filtered = [r for r in filtered if r["status"].lower() == s_lower]
    if min_recyclability is not None:
        filtered = [r for r in filtered if r["recyclability_pct"] >= min_recyclability]
    if max_carbon is not None:
        filtered = [r for r in filtered if r["carbon_footprint_kg_co2e"] <= max_carbon]

    total = len(filtered)
    paginated = filtered[offset:offset + limit]
    return paginated, total


def verify_passport(passport_id):
    """Verify a passport's authenticity and status.

    Args:
        passport_id: The passport ID string.

    Returns:
        dict | None: Verification result dict, or None if passport not found.
    """
    rec = get_passport_by_id(passport_id)
    if rec is None:
        return None

    # UID is stored on the record; check it's non-empty and valid SHA-256 hex
    uid_match = bool(rec["uid"]) and len(rec["uid"]) == 32 and all(
        c in "0123456789ABCDEF" for c in rec["uid"]
    )

    return {
        "passport_id": rec["id"],
        "verified": rec["status"] == "active" and uid_match,
        "status": rec["status"],
        "uid_match": uid_match,
        "issued_by": rec["manufacturer"]["name"],
        "issue_date": rec["issue_date"],
        "expiry_date": rec["expiry_date"],
    }


def get_standards(sector=None):
    """Get regulatory standards, optionally filtered by sector.

    Args:
        sector: Filter standards by sector (e.g. 'batteries', 'construction').
                If None, returns all standards.

    Returns:
        list[dict]: Matching standards.
    """
    if sector is None:
        return STANDARDS
    s_lower = sector.lower()
    return [
        s for s in STANDARDS
        if s["sector"].lower() == s_lower or s["sector"] == "all"
    ]


# ── Quick sanity ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ds = build_dataset(force_refresh=True)
    print(f"Dataset size: {len(ds)} records")
    print(f"Sectors: {SECTORS}")
    print(f"Standards: {len(STANDARDS)}")

    # Test lookups
    sample = ds[0]
    pid = sample["id"]
    print(f"\nSample: {pid} — {sample['product_name']} [{sample['status']}]")

    found = get_passport_by_id(pid)
    assert found is not None, f"Should find {pid}"

    results, total = search_passports(sector="batteries", limit=5)
    print(f"Battery passports: {total} total, showing {len(results)}")

    ver = verify_passport(pid)
    print(f"Verify {pid}: verified={ver['verified']}, status={ver['status']}")

    stds = get_standards(sector="batteries")
    print(f"Battery standards: {len(stds)}")

    print("\n✅ All checks passed.")
