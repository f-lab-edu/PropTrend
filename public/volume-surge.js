import { element, formatDate } from "/common.js";

const container = document.getElementById("volume-surge");

// 지역 하나를 순위 타일로 그린다. 증가율은 API에 없어 이전 건수가 있을 때만 여기서 계산한다.
function regionTile(region, rank) {
  const tile = element("div", "flex flex-col rounded-xl bg-white p-5 shadow-sm");
  tile.append(
    element("p", "text-sm font-bold text-slate-400", String(rank)),
    // 법정동코드에 없는 지역은 이름이 null이라 코드로 대신 보여준다.
    element("p", "mt-1 font-medium", region.region_name ?? region.region_code),
    element("p", "mt-2 text-2xl font-bold text-red-600", `+${region.count_change.toLocaleString()}건`),
  );
  if (region.previous_count > 0) {
    const rate = (region.count_change / region.previous_count) * 100;
    tile.append(element("p", "text-sm font-medium text-red-600", `+${rate.toFixed(1)}%`));
  }
  tile.append(
    element(
      "p",
      "mt-auto pt-3 text-xs text-slate-500",
      `${region.previous_count.toLocaleString()} → ${region.recent_count.toLocaleString()}건`,
    ),
  );
  return tile;
}

async function loadVolumeSurge() {
  try {
    const response = await fetch("/api/market/volume-surge-regions");
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const surge = await response.json();
    document.getElementById("volume-surge-title").textContent =
      `거래량 급등 지역 TOP 5 · ${formatDate(surge.base_date)} 기준`;
    if (surge.regions.length === 0) {
      container.replaceChildren(
        element(
          "p",
          "col-span-full rounded-xl bg-white p-6 text-center text-sm text-slate-400 shadow-sm",
          "이전 구간보다 거래가 늘어난 지역이 없습니다.",
        ),
      );
      return;
    }
    container.replaceChildren(...surge.regions.map((region, index) => regionTile(region, index + 1)));
  } catch (error) {
    // 다른 시장 요약 블록과 따로 불러오므로 실패해도 이 영역에만 안내한다.
    console.error(error);
    container.replaceChildren(
      element(
        "p",
        "col-span-full rounded-xl bg-white p-6 text-center text-sm text-slate-500 shadow-sm",
        "거래량 급등 지역을 불러오지 못했습니다.",
      ),
    );
  }
}

loadVolumeSurge();
