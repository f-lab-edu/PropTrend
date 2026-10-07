import { element, formatArea, formatDate, formatWon, transactionTitle } from "/common.js";

const container = document.getElementById("price-movers");

// 단지 한 줄. 직전 거래가 최대 5년 전일 수 있어 두 거래의 금액과 계약일을 함께 보여준다.
function moverItem(mover, rank) {
  const { latest_sale: latest, previous_sale: previous } = mover;
  const rising = mover.change_rate > 0;

  // 아파트만 순위에 오르므로 유형 배지 없이 단지명만 보여준다.
  const name = element("p", "min-w-0 truncate font-medium", transactionTitle(latest));
  // 국내 시세 표기 관례대로 상승은 빨강, 하락은 파랑으로 보여준다.
  const rate = element(
    "p",
    `shrink-0 font-bold ${rising ? "text-red-600" : "text-blue-600"}`,
    `${rising ? "+" : ""}${mover.change_rate.toFixed(2)}%`,
  );
  const header = element("div", "flex items-center justify-between gap-3");
  header.append(name, rate);

  const area = latest.exclusive_use_area === null ? null : `전용 ${formatArea(latest.exclusive_use_area)}`;
  const body = element("div", "min-w-0 flex-1");
  body.append(
    header,
    element("p", "mt-1 truncate text-xs text-slate-500", [latest.address, area].filter(Boolean).join(" · ")),
    element(
      "p",
      "mt-1 text-sm text-slate-700",
      `${formatWon(previous.deal_amount)}(${formatDate(previous.deal_date)}) → ` +
        `${formatWon(latest.deal_amount)}(${formatDate(latest.deal_date)})`,
    ),
  );

  const link = element("a", "-mx-2 flex gap-3 rounded-lg px-2 py-3 hover:bg-slate-50");
  link.href = `/transaction.html?${new URLSearchParams({ deal_type: "sales", id: latest.id })}`;
  link.append(element("span", "w-4 shrink-0 text-sm font-bold text-slate-400", String(rank)), body);

  const item = element("li", "");
  item.append(link);
  return item;
}

function moverPanel(title, movers, emptyText) {
  const card = element("div", "rounded-xl bg-white p-5 shadow-sm");
  card.append(element("p", "text-sm text-slate-500", title));
  if (movers.length === 0) {
    card.append(element("p", "py-10 text-center text-sm text-slate-400", emptyText));
    return card;
  }
  const list = element("ol", "mt-2 divide-y divide-slate-100");
  list.append(...movers.map((mover, index) => moverItem(mover, index + 1)));
  card.append(list);
  return card;
}

async function loadPriceMovers() {
  try {
    const response = await fetch("/api/market/price-movers");
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const movers = await response.json();
    document.getElementById("price-movers-title").textContent =
      `거래액 급등·급락 단지 TOP 5 · ${formatDate(movers.base_date)} 기준`;
    container.replaceChildren(
      moverPanel("급등", movers.surge, "최근 1년 안에 가격이 오른 아파트 단지가 없습니다."),
      moverPanel("급락", movers.plunge, "최근 1년 안에 가격이 내린 아파트 단지가 없습니다."),
    );
  } catch (error) {
    // 하루 요약과 따로 불러오므로 실패해도 이 영역에만 안내한다.
    console.error(error);
    container.replaceChildren(
      element(
        "p",
        "col-span-full rounded-xl bg-white p-6 text-center text-sm text-slate-500 shadow-sm",
        "급등·급락 단지를 불러오지 못했습니다.",
      ),
    );
  }
}

loadPriceMovers();
