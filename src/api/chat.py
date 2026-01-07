from fastapi import APIRouter
from pydantic import BaseModel
from src.rag.retriever import retrieve_context
from src.rag.generator import generate_answer

router = APIRouter()

class ChatRequest(BaseModel):
    symbol: str
    question: str

@router.post("/chat")
def chat(req: ChatRequest):
    context = retrieve_context(req.question, symbol=req.symbol)
    answer = generate_answer(context, req.question)

    return {
        "answer": answer,
        "disclaimer": "This is informational only and not financial advice."
    }
