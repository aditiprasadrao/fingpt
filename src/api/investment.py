from fastapi import APIRouter
from pydantic import BaseModel
from src.storage import db as dbmod

router = APIRouter()

class InvestmentRequest(BaseModel):
    symbol: str
    usd: float

@router.post("/calculate")
def calculate_units(req: InvestmentRequest):
    rows = dbmod.get_recent_aggregates(req.symbol, limit=1)
    last = rows[-1]

    price = last["close_price"]
    units = req.usd / price if price else 0

    trend = "up" if last["price_change_pct"] > 0 else "down"

    return {
        "usd": req.usd,
        "price": price,
        "units": units,
        "trend": trend
    }
