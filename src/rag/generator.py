import requests
import json

OLLAMA_URL = "http://localhost:11434/api/chat"
MODEL = "gemma2:9b"   # use exactly what `ollama list` shows

SYSTEM_PROMPT = (
    "You are a crypto market analyst. "
    "Answer strictly using the provided context. "
    "If the context is insufficient, say so clearly."
)

def generate_answer(context_docs, question):
    # Safety check
    if not context_docs:
        return "Not enough relevant market context available."

    # Build context string
    context_text = "\n".join(context_docs[:5])

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"Context:\n{context_text}\n\nQuestion:\n{question}"
            }
        ],
        "stream": False
    }

    try:
        response = requests.post(
            OLLAMA_URL,
            json=payload,
            timeout=120   # IMPORTANT for Streamlit
        )
        response.raise_for_status()

        data = response.json()

        # 👇 THIS IS THE KEY FIX
        if "message" in data and "content" in data["message"]:
            return data["message"]["content"]

        # Fallback (safety)
        return str(data)

    except requests.exceptions.RequestException as e:
        return f"⚠️ Ollama request failed: {e}"
