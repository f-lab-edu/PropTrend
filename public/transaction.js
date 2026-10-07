import { fetchMe, readErrorMessage } from "./auth.js";
import {
  COLUMNS,
  DEAL_DATE,
  PROPERTY_TYPE_LABELS,
  cancelBadge,
  formatArea,
  formatDate,
  formatWon,
  isJeonse,
  showMessage,
  transactionTitle,
} from "./common.js";

// years가 null이면 전체 기간이다.
const PERIODS = [
  { label: "1년", years: 1 },
  { label: "3년", years: 3 },
  { label: "전체", years: null },
];
const DEFAULT_YEARS = 3;

const LINE_COLOR = "#2563eb";
const BASE_POINT_COLOR = "#dc2626";

// 추이를 묶는 기준이 유형마다 달라서, 무엇과 비교한 추이인지 차트 위에 적는다.
// 오피스텔은 전용면적이 빈 거래끼리도 묶이므로 면적이 없을 수 있다.
function areaText(row) {
  return row.exclusive_use_area === null ? "" : ` · 전용 ${formatArea(row.exclusive_use_area)}`;
}

const TREND_CAPTIONS = {
  APT: (row) => `같은 단지${areaText(row)} 거래`,
  OFFICETEL: (row) => `같은 건물${areaText(row)} 거래`,
  ROW_HOUSE: () => "같은 건물(지번·건축년도) 거래",
};

const message = document.getElementById("message");
const trendMessage = document.getElementById("trend-message");
const chartWrapper = document.getElementById("chart-wrapper");

function floorText(point) {
  return point.floor === null ? "" : ` · ${point.floor}층`;
}

function badge(text, className) {
  const element = document.createElement("span");
  element.className = `rounded px-1.5 py-0.5 ${className}`;
  element.textContent = text;
  return element;
}

function renderSummary(dealType, row) {
  const kind = dealType === "sales" ? "매매" : isJeonse(row) ? "전세" : "월세";
  const badges = [
    badge(PROPERTY_TYPE_LABELS[row.property_type], "bg-slate-100 text-slate-700"),
    badge(kind, "bg-blue-50 text-blue-700"),
  ];
  if (row.cancel_deal_date) badges.push(cancelBadge(row.cancel_deal_date));
  document.getElementById("badges").replaceChildren(...badges);

  const title = transactionTitle(row);
  document.getElementById("title").textContent = title;
  document.getElementById("address").textContent = row.address;
  document.title = `${title} | 실거래 상세`;

  const price = document.getElementById("price");
  if (dealType === "sales") {
    price.textContent = formatWon(row.deal_amount);
    price.classList.toggle("line-through", Boolean(row.cancel_deal_date));
  } else {
    price.textContent = isJeonse(row)
      ? `전세 ${formatWon(row.deposit)}`
      : `월세 ${formatWon(row.deposit)} / ${formatWon(row.monthly_rent)}`;
  }

  // 가격은 위에서 크게 보여주므로 빼고, 값이 없는 항목은 그리지 않는다.
  const { byType, terms } = COLUMNS[dealType];
  const items = [DEAL_DATE, ...byType[row.property_type], ...terms]
    .map(({ label, render }) => [label, render(row)])
    .filter(([, value]) => value !== "-");
  if (row.cancel_deal_date) items.push(["해제일", formatDate(row.cancel_deal_date)]);
  document.getElementById("info").replaceChildren(
    ...items.map(([label, value]) => {
      const item = document.createElement("div");
      const dt = document.createElement("dt");
      dt.className = "text-slate-500";
      dt.textContent = label;
      const dd = document.createElement("dd");
      dd.className = "font-medium";
      dd.append(value);
      item.append(dt, dd);
      return item;
    }),
  );
}

// 차트에 그릴 시리즈. 전월세는 보증금끼리 비교되도록 전세와 월세를 나누고, 월세는 월세 선에 보증금을 툴팁으로 붙인다.
function buildSeries(dealType, body) {
  if (dealType === "sales") {
    return [
      {
        label: "매매",
        points: body.trend.map((point) => ({ x: point.deal_date, y: point.deal_amount, ...point })),
        tooltip: (point) => `${formatWon(point.y)}${floorText(point)}`,
      },
    ];
  }
  return [
    {
      label: "전세",
      points: body.jeonse_trend.map((point) => ({ x: point.deal_date, y: point.deposit, ...point })),
      tooltip: (point) => `보증금 ${formatWon(point.y)}${floorText(point)}`,
    },
    {
      label: "월세",
      points: body.monthly_rent_trend.map((point) => ({ x: point.deal_date, y: point.monthly_rent, ...point })),
      tooltip: (point) => `월세 ${formatWon(point.y)} · 보증금 ${formatWon(point.deposit)}${floorText(point)}`,
    },
  ];
}

function createChart(baseId, tooltipLabel) {
  const isBase = (context) => context.raw?.id === baseId;
  return new Chart(document.getElementById("chart"), {
    type: "line",
    data: {
      datasets: [
        {
          data: [],
          borderColor: LINE_COLOR,
          borderWidth: 1.5,
          pointRadius: (context) => (isBase(context) ? 6 : 2.5),
          pointBackgroundColor: (context) => (isBase(context) ? BASE_POINT_COLOR : LINE_COLOR),
          pointBorderColor: (context) => (isBase(context) ? BASE_POINT_COLOR : LINE_COLOR),
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      interaction: { mode: "nearest", intersect: false },
      scales: {
        x: {
          type: "time",
          time: {
            tooltipFormat: "yyyy.MM.dd",
            displayFormats: { day: "MM.dd", week: "MM.dd", month: "yyyy.MM", quarter: "yyyy.MM", year: "yyyy" },
          },
        },
        y: { ticks: { callback: (value) => formatWon(value) } },
      },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: (context) => tooltipLabel(context.raw) } },
      },
    },
  });
}

function toggleButtons(container, activeIndex) {
  [...container.children].forEach((button, index) => {
    const active = index === activeIndex;
    button.className = `rounded-lg border px-3 py-1 text-sm ${
      active ? "border-blue-600 bg-blue-600 text-white" : "border-slate-300 bg-white hover:bg-slate-100"
    }`;
  });
}

function renderTrend(dealType, body) {
  const base = body.base_transaction;
  const trend = dealType === "sales" ? body.trend : body.jeonse_trend;
  // null은 같은 매물을 특정할 수 없는 단독다가구, 빈 목록은 묶는 기준 값이 비어 있는 거래다.
  if (trend === null) {
    showMessage(trendMessage, "단독다가구는 같은 매물을 특정할 수 없어 추이를 제공하지 않습니다.");
    return;
  }
  const series = buildSeries(dealType, body);
  if (series.every(({ points }) => points.length === 0)) {
    showMessage(trendMessage, "단지·지번 등 비교 기준 정보가 없어 추이를 보여줄 수 없습니다.");
    return;
  }

  document.getElementById("trend-caption").textContent = TREND_CAPTIONS[base.property_type](base);
  if (base.cancel_deal_date) {
    showMessage(document.getElementById("trend-note"), "해제된 거래는 추이에 포함되지 않습니다.");
  }

  let seriesIndex = dealType === "rents" && !isJeonse(base) ? 1 : 0;
  let years = DEFAULT_YEARS;
  const chart = createChart(base.id, (point) => series[seriesIndex].tooltip(point));

  function update() {
    const { points } = series[seriesIndex];
    let visible = points;
    if (years !== null) {
      const cutoff = new Date();
      cutoff.setFullYear(cutoff.getFullYear() - years);
      const cutoffDate = cutoff.toISOString().slice(0, 10);
      visible = points.filter((point) => point.x >= cutoffDate);
    }
    toggleButtons(document.getElementById("series-tabs"), seriesIndex);
    toggleButtons(document.getElementById("period-buttons"), PERIODS.findIndex((period) => period.years === years));

    if (visible.length === 0) {
      chartWrapper.classList.add("hidden");
      showMessage(trendMessage, "선택한 기간에 거래가 없습니다.");
      return;
    }
    trendMessage.classList.add("hidden");
    chartWrapper.classList.remove("hidden");
    chart.data.datasets[0].data = visible;
    chart.update();
  }

  // 매매는 시리즈가 하나라 탭을 그리지 않는다.
  if (series.length > 1) {
    document.getElementById("series-tabs").replaceChildren(
      ...series.map(({ label, points }, index) => {
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = `${label} ${points.length}건`;
        button.addEventListener("click", () => {
          seriesIndex = index;
          update();
        });
        return button;
      }),
    );
  }
  document.getElementById("period-buttons").replaceChildren(
    ...PERIODS.map((period) => {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = period.label;
      button.addEventListener("click", () => {
        years = period.years;
        update();
      });
      return button;
    }),
  );
  document.getElementById("trend-controls").classList.replace("hidden", "flex");
  update();
}

// 즐겨찾기 추가 버튼. 제거는 내 정보 페이지에서 하므로 이미 즐겨찾기한 단지면 채운 별로 두고 끈다.
async function renderFavorite(complexId) {
  // 연립다세대·단독다가구나 단지를 찾지 못한 거래는 즐겨찾기할 단지가 없다.
  if (complexId === null) return;
  const button = document.getElementById("favorite");
  const favoriteMessage = document.getElementById("favorite-message");

  function update(favorite) {
    button.disabled = favorite;
    button.title = favorite ? "즐겨찾기한 단지입니다" : "즐겨찾기 추가";
    button.setAttribute("aria-label", button.title);
    button.querySelector("svg").setAttribute("fill", favorite ? "currentColor" : "none");
    button.classList.toggle("text-amber-400", favorite);
    button.classList.toggle("text-slate-400", !favorite);
  }

  // 로그인했으면 내 즐겨찾기 목록에서 이 단지를 찾는다. 확인에 실패해도 추가는 멱등이라 빈 별로 둔다.
  let favorite = false;
  try {
    if (await fetchMe()) {
      const response = await fetch("/api/favorites/complexes");
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      favorite = (await response.json()).items.some((item) => item.complex_id === complexId);
    }
  } catch (error) {
    console.error(error);
  }
  update(favorite);
  button.classList.remove("hidden");

  button.addEventListener("click", async () => {
    button.disabled = true;
    favoriteMessage.classList.add("hidden");
    try {
      const response = await fetch("/api/favorites/complexes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ complex_id: complexId }),
      });
      // 로그인하지 않았거나 세션이 끝났으면 로그인한 뒤 이 페이지로 돌아온다.
      if (response.status === 401) {
        location.href = `/login.html?${new URLSearchParams({ next: location.pathname + location.search })}`;
        return;
      }
      if (!response.ok) {
        showMessage(favoriteMessage, await readErrorMessage(response));
        button.disabled = false;
        return;
      }
      update(true);
    } catch (error) {
      console.error(error);
      showMessage(favoriteMessage, "즐겨찾기에 추가하지 못했습니다. 잠시 후 다시 시도하세요.");
      button.disabled = false;
    }
  });
}

async function main() {
  // 목록에서 들어왔으면 조건과 페이지가 담긴 목록 URL로 돌아가고, 링크로 바로 들어왔으면 검색 페이지로 간다.
  document.getElementById("back").addEventListener("click", () => {
    if (document.referrer.includes("/transactions.html")) history.back();
    else location.href = "/";
  });

  const params = new URLSearchParams(location.search);
  const dealType = params.get("deal_type");
  const id = params.get("id");
  if (!Object.hasOwn(COLUMNS, dealType) || !/^\d+$/.test(id ?? "")) {
    showMessage(message, "잘못된 주소입니다. 목록에서 거래를 다시 선택하세요.");
    return;
  }

  try {
    const response = await fetch(`/api/prop-transactions/${dealType}/${id}`);
    const body = await response.json();
    if (!response.ok) {
      showMessage(message, body.message ?? "거래 정보를 불러오지 못했습니다.");
      return;
    }
    renderSummary(dealType, body.base_transaction);
    renderFavorite(body.base_transaction.complex_id);
    document.getElementById("detail").classList.remove("hidden");
    renderTrend(dealType, body);
  } catch (error) {
    console.error(error);
    showMessage(message, "거래 정보를 불러오지 못했습니다. 잠시 후 다시 시도하세요.");
  }
}

await main();
