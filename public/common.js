// 목록·상세 페이지가 함께 쓰는 라벨, 표시 형식, 유형별 필드 정의.

export const PROPERTY_TYPE_LABELS = {
  APT: "아파트",
  OFFICETEL: "오피스텔",
  ROW_HOUSE: "연립다세대",
  SINGLE_MULTI: "단독다가구",
};
export const DEAL_TYPE_LABELS = { sales: "매매", rents: "전월세" };

// 금액은 원 단위로 오지만 원천 데이터가 만원 단위라 만원 아래는 버린다.
export function formatWon(won) {
  const man = Math.round(won / 10000);
  const eok = Math.floor(man / 10000);
  const rest = man % 10000;
  if (eok && rest) return `${eok}억 ${rest.toLocaleString()}만원`;
  if (eok) return `${eok}억원`;
  return `${rest.toLocaleString()}만원`;
}

export function formatDate(value) {
  return value.replaceAll("-", ".");
}

export function formatArea(value) {
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}㎡`;
}

// 건물명이 없는 단독다가구 등은 주택유형, 그것도 없으면 유형명으로 부른다.
export function transactionTitle(row) {
  return row.building_name ?? row.house_type ?? PROPERTY_TYPE_LABELS[row.property_type];
}

export function isJeonse(row) {
  return row.monthly_rent === 0;
}

// format은 값이 있을 때만 부른다. 비어 있으면 열과 상관없이 "-"로 보여준다.
function column(label, key, format = String, align = "left") {
  return {
    label,
    align,
    render: (row) => (row[key] === null || row[key] === undefined ? "-" : format(row[key])),
  };
}

function renderSaleAmount(row) {
  const amount = document.createElement("span");
  amount.textContent = formatWon(row.deal_amount);
  if (!row.cancel_deal_date) return amount;

  amount.className = "line-through";
  const cell = document.createDocumentFragment();
  cell.append(amount, cancelBadge(row.cancel_deal_date));
  return cell;
}

export function cancelBadge(cancelDealDate) {
  const badge = document.createElement("span");
  badge.className = "ml-1 rounded bg-red-100 px-1.5 py-0.5 text-xs text-red-700";
  badge.textContent = "해제";
  badge.title = `해제일 ${formatDate(cancelDealDate)}`;
  return badge;
}

export const DEAL_DATE = column("계약일", "deal_date", formatDate);
export const ADDRESS = column("주소", "address");
const BUILDING_NAME = column("건물명", "building_name");
const COMPLEX_NAME = column("단지명", "building_name");
const HOUSE_TYPE = column("주택유형", "house_type");
const FLOOR = column("층", "floor", (value) => `${value}층`, "right");
const EXCLUSIVE_AREA = column("전용면적", "exclusive_use_area", formatArea, "right");
const TOTAL_FLOOR_AREA = column("연면적", "total_floor_area", formatArea, "right");
const BUILD_YEAR = column("건축년도", "build_year", (value) => `${value}년`, "right");

// 거래구분마다 유형별 열, 가격 열, 계약 조건 열을 따로 둔다. 유형에 해당하지 않아 항상 null인 필드는 열로 만들지 않는다.
// 상세 페이지는 가격을 따로 크게 보여주므로 price를 빼고 쓴다.
export const COLUMNS = {
  sales: {
    byType: {
      APT: [COMPLEX_NAME, column("동", "apartment_dong"), FLOOR, EXCLUSIVE_AREA, BUILD_YEAR],
      OFFICETEL: [BUILDING_NAME, FLOOR, EXCLUSIVE_AREA, BUILD_YEAR],
      ROW_HOUSE: [
        HOUSE_TYPE,
        BUILDING_NAME,
        FLOOR,
        EXCLUSIVE_AREA,
        column("대지권면적", "land_area", formatArea, "right"),
        BUILD_YEAR,
      ],
      SINGLE_MULTI: [
        HOUSE_TYPE,
        TOTAL_FLOOR_AREA,
        column("대지면적", "plottage_area", formatArea, "right"),
        BUILD_YEAR,
      ],
    },
    price: [{ label: "거래금액", align: "right", render: renderSaleAmount }],
    terms: [column("거래방식", "dealing_type")],
  },
  rents: {
    byType: {
      APT: [COMPLEX_NAME, FLOOR, EXCLUSIVE_AREA, BUILD_YEAR],
      OFFICETEL: [BUILDING_NAME, FLOOR, EXCLUSIVE_AREA, BUILD_YEAR],
      ROW_HOUSE: [HOUSE_TYPE, BUILDING_NAME, FLOOR, EXCLUSIVE_AREA, BUILD_YEAR],
      SINGLE_MULTI: [HOUSE_TYPE, TOTAL_FLOOR_AREA, BUILD_YEAR],
    },
    price: [
      { label: "구분", align: "left", render: (row) => (isJeonse(row) ? "전세" : "월세") },
      column("보증금", "deposit", formatWon, "right"),
      { label: "월세", align: "right", render: (row) => (isJeonse(row) ? "-" : formatWon(row.monthly_rent)) },
    ],
    terms: [column("계약기간", "contract_term"), column("신규/갱신", "contract_type")],
  },
};

export function showMessage(target, text) {
  target.textContent = text;
  target.classList.remove("hidden");
}

export function element(tag, className, text) {
  const node = document.createElement(tag);
  node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}
