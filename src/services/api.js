const BASE_URL = process.env.REACT_APP_BACKEND_URL;

// Common headers for ngrok
const NGROK_HEADERS = {
  "Content-Type": "application/json",
  "ngrok-skip-browser-warning": "true",
};

// ---------------- MARKET DATA ----------------
export async function getMarketData(symbol) {
  const res = await fetch(
    `${BASE_URL}/api/market/api/market/${symbol}`,
    {
      headers: NGROK_HEADERS,
    }
  );

  const text = await res.text();

  // If ngrok / backend returns HTML, this will catch it
  try {
    return JSON.parse(text);
  } catch (err) {
    console.error("Market API returned non-JSON:", text);
    throw new Error("Market API unavailable");
  }
}

// ---------------- INVESTMENT ----------------
export async function calculateInvestment(symbol, usd) {
  const res = await fetch(`${BASE_URL}/api/investment`, {
    method: "POST",
    headers: NGROK_HEADERS,
    body: JSON.stringify({
      symbol,
      investment: usd,
    }),
  });

  return res.json();
}

// ---------------- CHATBOT ----------------
export async function sendChatQuery(symbol, question) {
  const res = await fetch(`${BASE_URL}/api/chat/chat`, {
    method: "POST",
    headers: NGROK_HEADERS,
    body: JSON.stringify({
      symbol: symbol,
      question: question,
    }),
  });

  return res.json(); // returns an OBJECT { answer, disclaimer }
}
