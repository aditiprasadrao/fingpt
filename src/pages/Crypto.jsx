import { useEffect, useState } from "react";
import {
  getMarketData,
  calculateInvestment,
  sendChatQuery,
} from "../services/api";
import Header from "../components/Common/Header";
import Loader from "../components/Common/Loader";
import LineChart from "../components/CoinPage/LineChart";
import { useNavigate } from "react-router-dom";

const Crypto = ({ symbol, title }) => {
  const navigate = useNavigate();

  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  const [usdAmount, setUsdAmount] = useState("");
  const [investmentResult, setInvestmentResult] = useState(null);
  const [investLoading, setInvestLoading] = useState(false);

  const [chatInput, setChatInput] = useState("");
  const [chatResponse, setChatResponse] = useState("");
  const [chatLoading, setChatLoading] = useState(false);

  // ---------------- FETCH MARKET DATA ----------------
  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 60000);
    return () => clearInterval(interval);
  }, [symbol]);

  async function fetchData() {
    try {
      setLoading(true);
      const res = await getMarketData(symbol);
      setData(res);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  }

  // ---------------- INVESTMENT ----------------
  async function handleInvestment() {
    if (!usdAmount) return;

    try {
      setInvestLoading(true);
      const res = await calculateInvestment(symbol, Number(usdAmount));
      setInvestmentResult(res);
    } catch (err) {
      console.error(err);
    } finally {
      setInvestLoading(false);
    }
  }

  // ---------------- CHATBOT ----------------
  async function handleChat() {
    if (!chatInput) return;

    try {
      setChatLoading(true);
      const res = await sendChatQuery(symbol, chatInput);
      setChatResponse(res.answer);
    } catch (err) {
      console.error(err);
      setChatResponse("Something went wrong. Please try again.");
    } finally {
      setChatLoading(false);
    }
  }

  if (loading) return <Loader />;
  if (!data) return <p>No data available</p>;

  // ---------------- CHART DATA ----------------
  const formattedChartData = {
    labels: data.series.map((p) =>
      new Date(p.ts).toLocaleTimeString()
    ),
    datasets: [
      {
        label: `${title} Price`,
        data: data.series.map((p) => p.price),
        borderColor: "#3a80e9",
        backgroundColor: "rgba(58,128,233,0.1)",
        tension: 0.25,
      },
    ],
  };

  return (
    <>
      <Header />

      {/* ---------- NAVIGATION ---------- */}
      <div style={{ display: "flex", gap: "12px", marginBottom: "1.5rem" }}>
        <button onClick={() => navigate("/btc")} style={navBtnStyle}>
          BTC
        </button>
        <button onClick={() => navigate("/eth")} style={navBtnStyle}>
          ETH
        </button>
        <button onClick={() => navigate("/usdt")} style={navBtnStyle}>
          USDT
        </button>
      </div>

      <h1>{title}</h1>
      <h2>Current Price: ${data.latest.price}</h2>

      {/* ---------- CHART ---------- */}
      <LineChart chartData={formattedChartData} />

      {/* ---------- TABLE ---------- */}
      <h3 style={{ marginTop: "2rem" }}>Recent Prices</h3>

      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          marginTop: "1rem",
        }}
      >
        <thead>
          <tr>
            <th style={thStyle}>Time</th>
            <th style={thStyle}>Price (USD)</th>
          </tr>
        </thead>
        <tbody>
          {data.series
            .slice(-10)
            .reverse()
            .map((point, index) => (
              <tr key={index}>
                <td style={tdStyle}>
                  {new Date(point.ts).toLocaleTimeString()}
                </td>
                <td style={tdStyle}>
                  ${point.price.toFixed(2)}
                </td>
              </tr>
            ))}
        </tbody>
      </table>

      {/* ---------- INVESTMENT ---------- */}
      <h3 style={{ marginTop: "2.5rem" }}>Investment Calculator</h3>

      <input
        type="number"
        placeholder="Enter USD amount"
        value={usdAmount}
        onChange={(e) => setUsdAmount(e.target.value)}
        style={{ padding: "8px", width: "200px", marginRight: "10px" }}
      />

      <button
        onClick={handleInvestment}
        disabled={investLoading}
        style={primaryBtn}
      >
        {investLoading ? "Calculating..." : "Calculate"}
      </button>

      {investmentResult && (
        <p style={{ marginTop: "1rem" }}>
          With <strong>${investmentResult.usd}</strong>, you can buy{" "}
          <strong>{investmentResult.units}</strong>{" "}
          {symbol.split("-")[0]}
        </p>
      )}

      {/* ---------- CHATBOT ---------- */}
      <h3 style={{ marginTop: "2.5rem" }}>Ask the AI Assistant</h3>

      <textarea
        rows={3}
        placeholder={`Ask something about ${
          symbol.split("-")[0]
        } market or sentiment`}
        value={chatInput}
        onChange={(e) => setChatInput(e.target.value)}
        style={{ width: "100%", padding: "10px", marginBottom: "10px" }}
      />

      <button onClick={handleChat} disabled={chatLoading} style={primaryBtn}>
        {chatLoading ? "Thinking..." : "Ask"}
      </button>

      {chatResponse && (
        <div
          style={{
            marginTop: "1rem",
            padding: "12px",
            background: "#f5f7fb",
            borderRadius: "6px",
          }}
        >
          <strong>AI:</strong>
          <p>{chatResponse}</p>
        </div>
      )}
    </>
  );
};

export default Crypto;

// ---------------- STYLES ----------------
const navBtnStyle = {
  padding: "8px 16px",
  border: "1px solid #3a80e9",
  background: "transparent",
  color: "#3a80e9",
  cursor: "pointer",
  borderRadius: "4px",
};

const primaryBtn = {
  padding: "8px 16px",
  backgroundColor: "#3a80e9",
  color: "white",
  border: "none",
  cursor: "pointer",
};

const thStyle = {
  borderBottom: "1px solid #ddd",
  padding: "10px",
  textAlign: "left",
  color: "#3a80e9",
};

const tdStyle = {
  borderBottom: "1px solid #eee",
  padding: "8px",
};
