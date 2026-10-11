import { fetchMe, readErrorMessage } from "./auth.js";
import { showMessage } from "./common.js";

// 422의 항목별 msg는 pydantic 영문이라 실패한 필드만 보고 안내 문구를 고른다.
const FIELD_ERROR_MESSAGES = {
  email: "올바른 이메일 형식이 아닙니다.",
  nickname: "닉네임은 2~30자로 입력하세요.",
  password: "비밀번호는 8~64자로 입력하세요.",
};

const form = document.getElementById("signup-form");
const submitButton = document.getElementById("submit");
const message = document.getElementById("message");

function fieldError(field) {
  return form.querySelector(`[data-error-for="${field}"]`);
}

function clearErrors() {
  message.classList.add("hidden");
  for (const element of form.querySelectorAll("[data-error-for]")) {
    element.classList.add("hidden");
  }
}

async function showErrors(response) {
  if (response.status === 409) {
    showMessage(fieldError("email"), await readErrorMessage(response));
    return;
  }
  if (response.status === 422) {
    const { errors } = await response.json();
    // loc는 ["body", 필드명] 형태다. 본문 자체가 잘못된 경우처럼 필드가 없으면 공통 문구를 쓴다.
    const fields = errors.map(({ loc }) => loc[1]).filter((field) => field in FIELD_ERROR_MESSAGES);
    for (const field of fields) {
      showMessage(fieldError(field), FIELD_ERROR_MESSAGES[field]);
    }
    if (fields.length) return;
  }
  showMessage(message, await readErrorMessage(response));
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  clearErrors();
  const { email, nickname, password, password_confirm: passwordConfirm } = Object.fromEntries(new FormData(form));
  if (password !== passwordConfirm) {
    showMessage(fieldError("password_confirm"), "비밀번호가 일치하지 않습니다.");
    return;
  }

  submitButton.disabled = true;
  try {
    const response = await fetch("/api/users/sign-up", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, nickname, password }),
    });
    if (response.ok) {
      // 가입은 세션을 만들지 않으므로 비로그인 상태로 홈에 간다.
      location.href = "/";
      return;
    }
    await showErrors(response);
  } catch (error) {
    console.error(error);
    showMessage(message, "가입하지 못했습니다. 잠시 후 다시 시도하세요.");
  } finally {
    submitButton.disabled = false;
  }
});

try {
  if (await fetchMe()) location.replace("/");
} catch (error) {
  console.error(error);
}
