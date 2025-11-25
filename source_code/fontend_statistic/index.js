// API Configuration
const API_BASE = "http://localhost:8080";

// Chart instances
let charts = {};

// Color scheme
const colors = {
  primary: "#6366f1",
  secondary: "#8b5cf6",
  accent: "#ec4899",
  success: "#10b981",
  warning: "#f59e0b",
  danger: "#ef4444",
  gradients: [
    "rgba(99, 102, 241, 0.8)",
    "rgba(139, 92, 246, 0.8)",
    "rgba(236, 72, 153, 0.8)",
    "rgba(16, 185, 129, 0.8)",
    "rgba(245, 158, 11, 0.8)",
    "rgba(239, 68, 68, 0.8)",
    "rgba(59, 130, 246, 0.8)",
    "rgba(168, 85, 247, 0.8)",
  ],
};

// Default chart options
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.color = "#64748b";

// Initialize
document.addEventListener("DOMContentLoaded", async () => {
  await loadAllData();
});

// Load all data
async function loadAllData() {
  const loadingOverlay = document.getElementById("loadingOverlay");

  try {
    const [genreData, seasonData, topMonthsData, bestWorstData] =
      await Promise.all([
        fetchData("/genresummary/getGenresByTotalGamesDesc"),
        fetchData("/genremonthly/season-summary"),
        fetchData("/genremonthly/top10release"),
        fetchData("/genremonthly/bestandworst-month"),
      ]);

    // Update navbar stats
    updateNavStats(genreData);

    // Create charts
    createGenreChart(genreData);
    createSeasonCharts(seasonData);
    createTopMonthsChart(topMonthsData);
    createBestWorstTable(bestWorstData);

    // Hide loading overlay
    setTimeout(() => {
      loadingOverlay.classList.add("hidden");
    }, 800);
  } catch (error) {
    console.error("Error loading data:", error);
    alert("Failed to load data. Please check if the API server is running.");
    loadingOverlay.classList.add("hidden");
  }
}

// Fetch data helper
async function fetchData(endpoint) {
  const response = await fetch(`${API_BASE}${endpoint}`);
  if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
  return response.json();
}

// Update navbar statistics
function updateNavStats(genreData) {
  const totalGames = genreData.reduce((sum, item) => sum + item.totalGames, 0);
  const totalGenres = genreData.length;

  animateNumber("totalGames", totalGames);
  animateNumber("totalGenres", totalGenres);
}

// Animate number
function animateNumber(elementId, targetValue) {
  const element = document.getElementById(elementId);
  const duration = 1000;
  const steps = 60;
  const increment = targetValue / steps;
  let current = 0;

  const timer = setInterval(() => {
    current += increment;
    if (current >= targetValue) {
      element.textContent = targetValue;
      clearInterval(timer);
    } else {
      element.textContent = Math.floor(current);
    }
  }, duration / steps);
}

// 1. Genre Distribution Chart (Doughnut)
function createGenreChart(data) {
  const ctx = document.getElementById("genreChart").getContext("2d");

  charts.genre = new Chart(ctx, {
    type: "doughnut",
    data: {
      labels: data.map((item) => item.genreName),
      datasets: [
        {
          data: data.map((item) => item.totalGames),
          backgroundColor: colors.gradients,
          borderWidth: 0,
          hoverOffset: 10,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: {
          position: "right",
          labels: {
            padding: 15,
            font: { size: 12 },
            generateLabels: (chart) => {
              const data = chart.data;
              return data.labels.map((label, i) => ({
                text: `${label} (${data.datasets[0].data[i]})`,
                fillStyle: data.datasets[0].backgroundColor[i],
                hidden: false,
                index: i,
              }));
            },
          },
        },
        tooltip: {
          backgroundColor: "rgba(15, 23, 42, 0.9)",
          padding: 12,
          titleFont: { size: 14, weight: "600" },
          bodyFont: { size: 13 },
          borderColor: "rgba(99, 102, 241, 0.5)",
          borderWidth: 1,
          callbacks: {
            label: (context) => {
              const total = context.dataset.data.reduce((a, b) => a + b, 0);
              const percentage = ((context.parsed / total) * 100).toFixed(1);
              return `${context.parsed} games (${percentage}%)`;
            },
          },
        },
      },
    },
  });
}

// 2. Season Chart (Polar Area)
function createSeasonCharts(data) {
  // Aggregate data by season
  const seasonMap = {};
  data.forEach((item) => {
    if (!seasonMap[item.season]) {
      seasonMap[item.season] = { totalGames: 0, totalPrice: 0, count: 0 };
    }
    seasonMap[item.season].totalGames += item.totalGames;
    seasonMap[item.season].totalPrice += item.avgPrice * item.totalGames;
    seasonMap[item.season].count += item.totalGames;
  });

  const seasons = Object.keys(seasonMap);
  const gamesData = seasons.map((s) => seasonMap[s].totalGames);
  const priceData = seasons.map(
    (s) => seasonMap[s].totalPrice / seasonMap[s].count
  );

  // Season Games Chart
  const ctx1 = document.getElementById("seasonChart").getContext("2d");
  charts.season = new Chart(ctx1, {
    type: "polarArea",
    data: {
      labels: seasons,
      datasets: [
        {
          data: gamesData,
          backgroundColor: colors.gradients.slice(0, 4),
          borderWidth: 2,
          borderColor: "#fff",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: {
          position: "bottom",
          labels: { padding: 15, font: { size: 12 } },
        },
        tooltip: {
          backgroundColor: "rgba(15, 23, 42, 0.9)",
          padding: 12,
          callbacks: {
            label: (context) => `${context.parsed.r} games`,
          },
        },
      },
      scales: {
        r: {
          ticks: { backdropColor: "transparent" },
          grid: { color: "rgba(203, 213, 225, 0.3)" },
        },
      },
    },
  });

  // Price Chart
  const ctx2 = document.getElementById("priceChart").getContext("2d");
  charts.price = new Chart(ctx2, {
    type: "radar",
    data: {
      labels: seasons,
      datasets: [
        {
          label: "Avg Price ($)",
          data: priceData,
          backgroundColor: "rgba(236, 72, 153, 0.2)",
          borderColor: colors.accent,
          borderWidth: 2,
          pointBackgroundColor: colors.accent,
          pointBorderColor: "#fff",
          pointBorderWidth: 2,
          pointRadius: 5,
          pointHoverRadius: 7,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          backgroundColor: "rgba(15, 23, 42, 0.9)",
          padding: 12,
          callbacks: {
            label: (context) => `$${context.parsed.r.toFixed(2)}`,
          },
        },
      },
      scales: {
        r: {
          beginAtZero: true,
          ticks: {
            callback: (value) => "$" + value,
            backdropColor: "transparent",
          },
          grid: { color: "rgba(203, 213, 225, 0.3)" },
        },
      },
    },
  });
}

// 3. Top Months Chart (Bar with gradient)
function createTopMonthsChart(data) {
  const ctx = document.getElementById("topMonthsChart").getContext("2d");

  const gradient = ctx.createLinearGradient(0, 0, 0, 300);
  gradient.addColorStop(0, colors.primary);
  gradient.addColorStop(1, colors.secondary);

  charts.topMonths = new Chart(ctx, {
    type: "bar",
    data: {
      labels: data.map((item) => item.month),
      datasets: [
        {
          label: "Total Games",
          data: data.map((item) => item.totalGames),
          backgroundColor: gradient,
          borderRadius: 8,
          barThickness: 40,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "rgba(15, 23, 42, 0.9)",
          padding: 12,
          callbacks: {
            afterLabel: (context) => {
              const item = data[context.dataIndex];
              return `Active Genres: ${
                item.activeGenres
              }\nAvg Price: $${item.avgPrice.toFixed(2)}`;
            },
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: { font: { size: 10 } },
        },
        y: {
          beginAtZero: true,
          grid: { color: "rgba(203, 213, 225, 0.3)" },
          ticks: { font: { size: 11 } },
        },
      },
    },
  });
}

// 4. Best & Worst Month Table
function createBestWorstTable(data) {
  const tbody = document.getElementById("bestWorstTable");
  tbody.innerHTML = "";

  data.forEach((item) => {
    const row = document.createElement("tr");
    row.innerHTML = `
            <td>${item.genreName}</td>
            <td>
                <span class="badge-best">
                    <span>✅</span>
                    <span>${item.bestMonth}</span>
                </span>
            </td>
            <td>
                <span class="badge-worst">
                    <span>❌</span>
                    <span>${item.worstMonth}</span>
                </span>
            </td>
        `;
    tbody.appendChild(row);
  });
}
