import { fetchMe, readErrorMessage, safeNext } from "./auth.js";
import { showMessage } from "./common.js";

const form = document.getElementById("login-form");
const emailInput = document.getElementById("email");
const passwordInput = document.getElementById("password");
const submitButton = document.getElementById("submit");
const message = document.getElementById("message");

const next = safeNext(new URLSearchParams(location.search).get("next"));

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  message.classList.add("hidden");
  submitButton.disabled = true;
  try {
    const response = await fetch("/api/users/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: emailInput.value, password: passwordInput.value }),
    });
    if (response.ok) {
      // 로그인 페이지가 방문 기록에 남지 않게 해 뒤로 가기로 다시 돌아오지 않게 한다.
      location.replace(next);
      return;
    }
    if (response.status === 401) {
      passwordInput.value = "";
      passwordInput.focus();
    }
    showMessage(message, await readErrorMessage(response));
  } catch (error) {
    console.error(error);
    showMessage(message, "로그인하지 못했습니다. 잠시 후 다시 시도하세요.");
  } finally {
    submitButton.disabled = false;
  }
});

// 이미 로그인했으면 폼을 보여줄 필요가 없다. 확인에 실패하면 그대로 로그인 폼을 쓴다.
try {
  if (await fetchMe()) location.replace(next);
} catch (error) {
  console.error(error);
}
