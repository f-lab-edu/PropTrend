import { fetchMe, readErrorMessage } from "/auth.js";
import { PROPERTY_TYPE_LABELS, formatDate, showMessage, transactionTitle } from "/common.js";

const message = document.getElementById("message");
const profile = document.getElementById("profile");
const logoutButton = document.getElementById("logout");
const logoutMessage = document.getElementById("logout-message");
const favorites = document.getElementById("favorites");
const favoriteList = document.getElementById("favorite-list");
const favoritesMessage = document.getElementById("favorites-message");
const favoritesError = document.getElementById("favorites-error");

// 가입 시각을 서비스 기준(KST) 날짜로 보여준다. sv-SE 형식이 YYYY-MM-DD라 formatDate에 그대로 넘긴다.
const KST_DATE = new Intl.DateTimeFormat("sv-SE", { timeZone: "Asia/Seoul" });

function showEmptyFavorites() {
  showMessage(favoritesMessage, "즐겨찾기한 단지가 없습니다. 실거래 상세에서 별을 눌러 추가하세요.");
}

// 단지 한 줄. 왼쪽에 유형·이름·주소, 오른쪽에 제거 버튼을 둔다.
function favoriteItem(favorite) {
  const badge = document.createElement("span");
  badge.className = "mr-1.5 rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-700";
  badge.textContent = PROPERTY_TYPE_LABELS[favorite.property_type];
  const name = document.createElement("p");
  name.className = "font-medium";
  name.append(badge, transactionTitle(favorite));
  const detail = document.createElement("p");
  detail.className = "mt-1 text-xs text-slate-500";
  detail.textContent = [favorite.address, favorite.build_year && `${favorite.build_year}년`].filter(Boolean).join(" · ");
  const info = document.createElement("div");
  info.className = "min-w-0";
  info.append(name, detail);

  const button = document.createElement("button");
  button.type = "button";
  button.textContent = "제거";
  button.className =
    "shrink-0 rounded-lg border border-slate-300 bg-white px-3 py-1 text-sm hover:bg-slate-100 disabled:text-slate-400";

  const item = document.createElement("li");
  item.className = "flex items-center justify-between gap-3 py-3";
  item.append(info, button);

  button.addEventListener("click", async () => {
    button.disabled = true;
    favoritesError.classList.add("hidden");
    try {
      const response = await fetch(`/api/favorites/complexes/${favorite.complex_id}`, { method: "DELETE" });
      if (!response.ok) {
        showMessage(favoritesError, await readErrorMessage(response));
        button.disabled = false;
        return;
      }
      item.remove();
      if (favoriteList.children.length === 0) showEmptyFavorites();
    } catch (error) {
      console.error(error);
      showMessage(favoritesError, "즐겨찾기를 제거하지 못했습니다. 잠시 후 다시 시도하세요.");
      button.disabled = false;
    }
  });
  return item;
}

async function loadFavorites() {
  favorites.classList.remove("hidden");
  try {
    const response = await fetch("/api/favorites/complexes");
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const items = await response.json();
    favoriteList.replaceChildren(...items.map(favoriteItem));
    if (items.length === 0) showEmptyFavorites();
  } catch (error) {
    console.error(error);
    showMessage(favoritesMessage, "즐겨찾기 목록을 불러오지 못했습니다. 잠시 후 다시 시도하세요.");
  }
}

async function loadProfile() {
  let user;
  try {
    user = await fetchMe();
  } catch (error) {
    console.error(error);
    showMessage(message, "내 정보를 불러오지 못했습니다. 잠시 후 다시 시도하세요.");
    return;
  }
  if (!user) {
    location.replace(`/login.html?${new URLSearchParams({ next: "/me.html" })}`);
    return;
  }
  document.getElementById("nickname").textContent = user.nickname;
  document.getElementById("email").textContent = user.email;
  document.getElementById("created-at").textContent = formatDate(KST_DATE.format(new Date(user.created_at)));
  profile.classList.remove("hidden");
  loadFavorites();
}

logoutButton.addEventListener("click", async () => {
  logoutButton.disabled = true;
  try {
    const response = await fetch("/api/users/logout", { method: "POST" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    location.href = "/";
  } catch (error) {
    console.error(error);
    showMessage(logoutMessage, "로그아웃하지 못했습니다. 잠시 후 다시 시도하세요.");
    logoutButton.disabled = false;
  }
});

loadProfile();
