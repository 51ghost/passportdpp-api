# PassportDPP API

**Digital Product Passport API for EU Circular Economy**

A production-ready API providing verified Digital Product Passport (DPP) data across four EU-regulated sectors: **Batteries**, **Textiles**, **Electronics**, and **Construction**. Built for the EU Battery Regulation 2023/1542, ESPR 2023, and EU Textile Strategy compliance requirements.

---

## 🚀 Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
python main.py

# Or with uvicorn directly
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## 🔑 Authentication

All endpoints require an `x-api-key` header. Default test keys:
- `test-key-123`
- `demo-key-456`
- `rapidapi-pro`

Set custom keys via environment variable:
```bash
export PASSPORTDPP_API_KEYS="key1,key2,key3"
```

## 📡 API Endpoints

### System

| Method | Path | Description | Plans |
|--------|------|-------------|-------|
| GET | `/v1/health` | Health check with dataset stats | All |
| GET | `/` | Root info with docs link | — |

### Passports

| Method | Path | Description | Plans |
|--------|------|-------------|-------|
| GET | `/v1/passports` | List/search DPPs with filters | Basic+ |
| GET | `/v1/passport/{id}` | Get full DPP details | All |
| GET | `/v1/verify/{id}` | Verify passport authenticity | Basic+ |
| GET | `/v1/standards` | EU regulatory standards ref | Basic+ |

### Query Parameters (`/v1/passports`)

| Parameter | Type | Description |
|-----------|------|-------------|
| `sector` | string | `battery`, `textile`, `electronics`, `construction` |
| `manufacturer` | string | Search by manufacturer name |
| `query` | string | Search product name or passport ID |
| `status` | string | `active`, `expired`, `revoked` |
| `min_recyclability` | float | Min recyclability % (0–100) |
| `max_carbon` | float | Max carbon footprint kg CO2e |
| `limit` | int | Results per page (max 100, default 20) |
| `offset` | int | Pagination offset (default 0) |

## 📊 Pricing Plans (RapidAPI)

| Plan | Price | Requests/Day | Rate/Min |
|------|-------|-------------|----------|
| **Free** | $0 | 100 | 10/min |
| **Basic** | $9.99 | 5,000 | 60/min |
| **Pro** | $49.99 | 50,000 | 300/min |
| **Enterprise** | $499.99 | 1,000,000 | 5,000/min |

## 📦 Dataset

The API ships with a built-in curated dataset of **55 Digital Product Passports** across 4 sectors. Data is cached for 24 hours and can be refreshed via `build_dataset(force_refresh=True)`.

Each passport includes:
- Unique identifier & UID hash
- Product name, sector, manufacturer details
- Full materials breakdown with recycled content %
- Recyclability percentage (40–98%)
- Carbon footprint (kg CO2e)
- Lifespan, compliance standards, certifications
- Issue/expiry dates and status tracking

### Extending the Dataset

```python
from data_pipeline import build_dataset

# Force refresh with new random data
ds = build_dataset(force_refresh=True)
print(f"Dataset size: {len(ds)}")
```

## 🏗 Deployment

### Railway

```bash
# Deploy via Railway CLI
railway login
railway init
railway up
```

The `railway.json` is pre-configured with health checks and Nixpacks builder.

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Server port | `8000` |
| `PASSPORTDPP_API_KEYS` | Comma-separated API keys | test keys |

## 🧪 Testing

```bash
# Smoke test
python -c 'from main import app; print("OK")'

# Test via curl
curl -H "x-api-key: test-key-123" http://localhost:8000/v1/health

# List passports
curl -H "x-api-key: test-key-123" \
  "http://localhost:8000/v1/passports?sector=battery&limit=5"

# Get passport details
curl -H "x-api-key: test-key-123" \
  "http://localhost:8000/v1/passport/DPP-BAT-0001-1234"

# Verify passport
curl -H "x-api-key: test-key-123" \
  "http://localhost:8000/v1/verify/DPP-BAT-0001-1234"

# Get standards
curl -H "x-api-key: test-key-123" \
  "http://localhost:8000/v1/standards?sector=textile"
```

## 📋 EU Regulatory Compliance

- **EU Battery Regulation 2023/1542** — Full DPP requirements for batteries
- **ESPR 2023 (Ecodesign)** — Digital passport for electronics
- **EU Textile Strategy 2022** — DPP for apparel and textiles
- **CPR 305/2011** — Construction products regulation

## 🛠 Tech Stack

- **FastAPI** — High-performance async Python web framework
- **SlowAPI** — Rate limiting
- **Pydantic v2** — Data validation
- **Uvicorn** — ASGI server
- **Railway** — Deployment platform

## 📄 License

MIT © Nous Research
