import { fetchMe } from "/auth.js";
import { formatDate, showMessage } from "/common.js";

const message = document.getElementById("message");
const profile = document.getElementById("profile");
const logoutButton = document.getElementById("logout");
const logoutMessage = document.getElementById("logout-message");

// 가입 시각을 서비스 기준(KST) 날짜로 보여준다. sv-SE 형식이 YYYY-MM-DD라 formatDate에 그대로 넘긴다.
const KST_DATE = new Intl.DateTimeFormat("sv-SE", { timeZone: "Asia/Seoul" });

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
