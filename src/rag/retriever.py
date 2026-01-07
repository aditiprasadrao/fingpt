import os
import faiss
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------
# Paths
# ---------------------------------------------------
BASE_DIR = os.path.dirname(__file__)
INDEX_PATH = os.path.join(BASE_DIR, "index.faiss")
META_PATH = os.path.join(BASE_DIR, "meta.pkl")

# ---------------------------------------------------
# Load FAISS + metadata once (on import)
# ---------------------------------------------------
embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
index = faiss.read_index(INDEX_PATH)

with open(META_PATH, "rb") as f:
    metadata = pickle.load(f)

print(f"[RAG] Retriever loaded {len(metadata)} documents")

# ---------------------------------------------------
# REQUIRED FUNCTION
# ---------------------------------------------------
def retrieve_context(query: str, symbol: str | None = None, k: int = 5):
    """
    Retrieve relevant documents from FAISS.
    Optionally filter by crypto symbol (BTC / ETH).
    """
    query_vec = embedder.encode([query]).astype("float32")
    _, indices = index.search(query_vec, k)

    results = []
    for idx in indices[0]:
        doc = metadata[idx]

        if symbol and doc.get("metadata", {}).get("symbol") != symbol:
            continue

        results.append(doc["text"])

    return results
