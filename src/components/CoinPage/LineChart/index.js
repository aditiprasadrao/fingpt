import React from "react";
import { Line } from "react-chartjs-2";
import { Chart as ChartJS } from "chart.js/auto"; //Dont get rid of this

function LineChart({ chartData, multiAxis }) {
const options = {
  responsive: true,
  plugins: {
    legend: {
      display: false,
    },
  },
  interaction: {
    mode: "index",
    intersect: false,
  },
  scales: {
    y: {
      beginAtZero: false,
    },
    x: {
      ticks: {
        maxTicksLimit: 10,
      },
    },
  },
};


  return <Line data={chartData} options={options} />;
}

export default LineChart;
