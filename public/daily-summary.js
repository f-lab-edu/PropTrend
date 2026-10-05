import { PROPERTY_TYPE_LABELS, element, formatArea, formatDate, formatWon, transactionTitle } from "/common.js";

const container = document.getElementById("daily-summary");
const CARD_CLASS = "flex flex-col rounded-xl bg-white p-5 shadow-sm";

function countCard(count) {
  const card = element("div", CARD_CLASS);
  card.append(
    element("p", "text-sm text-slate-500", "거래건수"),
    element("p", "mt-2 text-3xl font-bold", `${count.toLocaleString()}건`),
    element("p", "mt-auto pt-3 text-xs text-slate-500", "매매 + 전월세, 해제된 매매 제외"),
  );
  return card;
}

// 유형 구분 없이 고른 거래라 최저가는 소형 단독다가구인 경우가 많다. 유형과 면적을 함께 보여줘 어떤 거래인지 알게 한다.
function saleCard(label, sale) {
  if (sale === null) {
    const card = element("div", CARD_CLASS);
    card.append(
      element("p", "text-sm text-slate-500", label),
      element("p", "my-auto py-6 text-center text-sm text-slate-400", "이 날 신고된 매매가 없습니다."),
    );
    return card;
  }

  const card = element("a", `${CARD_CLASS} hover:ring-2 hover:ring-blue-200`);
  card.href = `/transaction.html?${new URLSearchParams({ deal_type: "sales", id: sale.id })}`;

  const header = element("div", "flex items-center justify-between");
  const typeBadge = "rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-700";
  header.append(
    element("p", "text-sm text-slate-500", label),
    element("span", typeBadge, PROPERTY_TYPE_LABELS[sale.property_type]),
  );

  const area =
    sale.exclusive_use_area !== null
      ? `전용 ${formatArea(sale.exclusive_use_area)}`
      : sale.total_floor_area !== null
        ? `연면적 ${formatArea(sale.total_floor_area)}`
        : null;
  const details = [area, sale.floor === null ? null : `${sale.floor}층`].filter(Boolean).join(" · ");

  card.append(
    header,
    element("p", "mt-2 text-2xl font-bold text-blue-700", formatWon(sale.deal_amount)),
    element("p", "mt-2 font-medium", transactionTitle(sale)),
    element("p", "text-xs text-slate-500", sale.address),
  );
  if (details) card.append(element("p", "mt-auto pt-3 text-xs text-slate-500", details));
  return card;
}

async function loadDailySummary() {
  try {
    const response = await fetch("/api/market/daily-summary");
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const summary = await response.json();
    document.getElementById("daily-summary-title").textContent =
      `하루 실거래 요약 · ${formatDate(summary.deal_date)} 계약`;
    container.replaceChildren(
      countCard(summary.transaction_count),
      saleCard("최고가 매매", summary.highest_sale),
      saleCard("최저가 매매", summary.lowest_sale),
    );
  } catch (error) {
    // 요약은 부가 정보라 실패해도 검색 폼은 그대로 쓸 수 있게 이 영역에만 안내한다.
    console.error(error);
    container.replaceChildren(
      element(
        "p",
        "col-span-full rounded-xl bg-white p-6 text-center text-sm text-slate-500 shadow-sm",
        "시장 요약을 불러오지 못했습니다.",
      ),
    );
  }
}

loadDailySummary();
