import {
  ADDRESS,
  COLUMNS,
  DEAL_DATE,
  DEAL_TYPE_LABELS,
  PROPERTY_TYPE_LABELS,
  showMessage,
} from "/common.js";

const PAGE_SIZE = 100;
const REQUIRED_PARAMS = ["property_type", "deal_type", "sido_code", "sigungu_code", "deal_ymd"];

const summary = document.getElementById("summary");
const message = document.getElementById("message");
const tableWrapper = document.getElementById("table-wrapper");
const tableHead = document.getElementById("table-head");
const tableBody = document.getElementById("table-body");
const pagination = document.getElementById("pagination");

function renderTable(dealType, columns, rows) {
  tableHead.replaceChildren(
    ...columns.map(({ label, align }) => {
      const th = document.createElement("th");
      th.className = `whitespace-nowrap px-3 py-2 font-medium text-${align}`;
      th.textContent = label;
      return th;
    }),
  );
  tableBody.replaceChildren(
    ...rows.map((row) => {
      const tr = document.createElement("tr");
      tr.className = `cursor-pointer hover:bg-slate-50 ${row.cancel_deal_date ? "text-slate-400" : ""}`;
      tr.addEventListener("click", () => {
        location.href = `/transaction.html?${new URLSearchParams({ deal_type: dealType, id: row.id })}`;
      });
      for (const { render, align } of columns) {
        const td = document.createElement("td");
        td.className = `whitespace-nowrap px-3 py-2 text-${align}`;
        // render는 문자열이나 노드를 돌려준다. 문자열은 append가 텍스트 노드로 넣으므로 HTML로 해석되지 않는다.
        td.append(render(row));
        tr.append(td);
      }
      return tr;
    }),
  );
  tableWrapper.classList.remove("hidden");
}

function setPageLink(link, page, enabled) {
  if (enabled) {
    const params = new URLSearchParams(location.search);
    params.set("page", page);
    link.href = `?${params}`;
    link.classList.add("hover:bg-slate-100");
    link.classList.remove("pointer-events-none", "opacity-40");
  } else {
    link.removeAttribute("href");
    link.classList.add("pointer-events-none", "opacity-40");
  }
}

function renderPagination(page, hasNext) {
  setPageLink(document.getElementById("prev-page"), page - 1, page > 1);
  setPageLink(document.getElementById("next-page"), page + 1, hasNext);
  document.getElementById("page-number").textContent = `${page} 페이지`;
  pagination.classList.replace("hidden", "flex");
}

async function renderSummary(filters) {
  const { property_type, deal_type, sido_code, sigungu_code, deal_ymd } = filters;
  let region = `${sido_code}${sigungu_code}`;
  try {
    const response = await fetch("/api/regions");
    const sido = response.ok ? (await response.json()).items.find((item) => item.sido_code === sido_code) : undefined;
    const sigungu = sido?.sigungus.find((item) => item.sigungu_code === sigungu_code);
    // 세종특별자치시처럼 시군구명이 시도명과 같으면 한 번만 쓴다.
    if (sigungu) {
      region = sigungu.sigungu_name === sido.sido_name ? sido.sido_name : `${sido.sido_name} ${sigungu.sigungu_name}`;
    }
  } catch (error) {
    // 지역명은 제목에만 쓰므로 실패해도 코드로 보여주고 목록은 계속 그린다.
    console.error(error);
  }
  const month = `${deal_ymd.slice(0, 4)}년 ${deal_ymd.slice(4)}월`;
  summary.textContent = `${region} · ${PROPERTY_TYPE_LABELS[property_type]} ${DEAL_TYPE_LABELS[deal_type]} · ${month}`;
  document.title = `${summary.textContent} | 실거래 목록`;
}

async function loadTransactions(filters, page) {
  const { property_type, deal_type, sido_code, sigungu_code, deal_ymd } = filters;
  const query = new URLSearchParams({
    property_type,
    sido_code,
    sigungu_code,
    deal_ymd,
    limit: PAGE_SIZE,
    offset: (page - 1) * PAGE_SIZE,
  });
  try {
    const response = await fetch(`/api/prop-transactions/${deal_type}?${query}`);
    const body = await response.json();
    if (!response.ok) {
      showMessage(message, body.message ?? "실거래 목록을 불러오지 못했습니다.");
      return;
    }
    if (body.items.length === 0) {
      showMessage(message, "조건에 맞는 거래가 없습니다.");
    } else {
      const { byType, price, terms } = COLUMNS[deal_type];
      const columns = [DEAL_DATE, ADDRESS, ...byType[property_type], ...price, ...terms];
      renderTable(deal_type, columns, body.items);
    }
    renderPagination(page, page * PAGE_SIZE < body.total);
  } catch (error) {
    console.error(error);
    showMessage(message, "실거래 목록을 불러오지 못했습니다. 잠시 후 다시 시도하세요.");
  }
}

function main() {
  const params = new URLSearchParams(location.search);
  const filters = Object.fromEntries(REQUIRED_PARAMS.map((key) => [key, params.get(key)]));
  // 값 형식은 API가 검증하므로 여기서는 열 구성을 고를 수 있는지만 본다.
  const missing = REQUIRED_PARAMS.some((key) => !filters[key]);
  if (missing || !COLUMNS[filters.deal_type] || !PROPERTY_TYPE_LABELS[filters.property_type]) {
    showMessage(message, "검색 조건이 올바르지 않습니다. 조건 변경에서 다시 선택하세요.");
    return;
  }
  const page = Math.max(1, Number.parseInt(params.get("page"), 10) || 1);

  renderSummary(filters);
  loadTransactions(filters, page);
}

main();
