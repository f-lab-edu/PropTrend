// 갱신 파이프라인이 거슬러 올라가는 최대 개월 수(api/src/jobs/pipeline.py MAX_MONTHS)와 맞춘다.
const DEAL_MONTH_COUNT = 24;

const sidoSelect = document.getElementById("sido");
const sigunguSelect = document.getElementById("sigungu");
const dealMonthSelect = document.getElementById("deal-month");
const message = document.getElementById("message");

// 시도 코드별 시군구 목록. 시도를 바꿀 때마다 API를 다시 부르지 않도록 처음 받은 응답을 둔다.
const sigungusBySido = new Map();

function showMessage(text) {
  message.textContent = text;
  message.classList.toggle("hidden", !text);
}

function fillOptions(select, placeholder, items) {
  select.replaceChildren(new Option(placeholder, ""));
  for (const [value, label] of items) {
    select.add(new Option(label, value));
  }
}

function fillDealMonths() {
  const today = new Date();
  const months = [];
  for (let i = 0; i < DEAL_MONTH_COUNT; i++) {
    // 1일로 두어야 31일 같은 날짜에서 월을 빼도 다음 달로 넘어가지 않는다.
    const date = new Date(today.getFullYear(), today.getMonth() - i, 1);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    months.push([`${year}${month}`, `${year}년 ${month}월`]);
  }
  dealMonthSelect.replaceChildren(...months.map(([value, label]) => new Option(label, value)));
}

async function loadRegions() {
  try {
    const response = await fetch("/api/regions");
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const sidos = await response.json();
    if (sidos.length === 0) {
      fillOptions(sidoSelect, "지역 없음", []);
      showMessage("아직 지역 목록이 적재되지 않았습니다.");
      return;
    }
    for (const sido of sidos) {
      sigungusBySido.set(sido.sido_code, sido.sigungus);
    }
    fillOptions(
      sidoSelect,
      "시도 선택",
      sidos.map((sido) => [sido.sido_code, sido.sido_name]),
    );
  } catch (error) {
    console.error(error);
    fillOptions(sidoSelect, "불러오기 실패", []);
    showMessage("지역 목록을 불러오지 못했습니다. 잠시 후 다시 시도하세요.");
  }
}

sidoSelect.addEventListener("change", () => {
  const sigungus = sigungusBySido.get(sidoSelect.value);
  if (!sigungus) {
    fillOptions(sigunguSelect, "시도를 먼저 선택하세요", []);
    sigunguSelect.disabled = true;
    return;
  }
  fillOptions(
    sigunguSelect,
    "시군구 선택",
    sigungus.map((sigungu) => [sigungu.sigungu_code, sigungu.sigungu_name]),
  );
  sigunguSelect.disabled = false;
});

fillDealMonths();
loadRegions();
