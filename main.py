from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.market import router as market_router
from src.api.investment import router as investment_router
from src.api.chat import router as chat_router

# ---------------------------------------------------
# App init
# ---------------------------------------------------
app = FastAPI(
    title="Crypto Sentiment Backend API",
    description="Market data, investment insights, and RAG-powered crypto analysis",
    version="1.0.0"
)

# ---------------------------------------------------
# CORS (required for frontend)
# ---------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # restrict later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------
# Routers
# ---------------------------------------------------
app.include_router(
    market_router,
    prefix="/api/market",
    tags=["Market"]
)

app.include_router(
    investment_router,
    prefix="/api/investment",
    tags=["Investment"]
)

app.include_router(
    chat_router,
    prefix="/api/chat",
    tags=["RAG Chat"]
)

# ---------------------------------------------------
# Health check
# ---------------------------------------------------
@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Crypto Sentiment Backend is running"
    }

