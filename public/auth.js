// 로그인 상태 확인과 헤더 버튼. 불러오기만 하면 #auth-nav가 있는 페이지의 헤더를 채운다.

const LINK_CLASS = "inline-block rounded-lg px-3 py-1.5 text-sm";

let mePromise;

// 세션 쿠키가 HttpOnly라 JS로 읽을 수 없으므로 내 정보 API로 로그인 여부를 판별한다.
// 헤더와 페이지가 함께 부르므로 요청은 한 번만 보낸다. 401이면 null, 그 밖의 실패는 예외로 올린다.
export function fetchMe() {
  mePromise ??= fetch("/api/users/me").then((response) => {
    if (response.status === 401) return null;
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json();
  });
  return mePromise;
}

// 로그인 뒤 돌아갈 경로. 다른 사이트로 보내지 않도록 같은 사이트의 경로만 받는다.
export function safeNext(value) {
  if (!value?.startsWith("/") || value.startsWith("//") || value.startsWith("/\\")) return "/";
  return value;
}

export async function readErrorMessage(response) {
  try {
    const { message } = await response.json();
    if (message) return message;
  } catch {
    // 본문이 공통 오류 응답 형식이 아니면 아래 문구를 쓴다.
  }
  return "요청을 처리하지 못했습니다. 잠시 후 다시 시도하세요.";
}

function navLink(href, text, className) {
  const link = document.createElement("a");
  link.href = href;
  link.textContent = text;
  link.className = `${LINK_CLASS} ${className}`;
  return link;
}

async function renderAuthNav(nav) {
  let user = null;
  try {
    user = await fetchMe();
  } catch (error) {
    console.error(error);
  }
  if (user) {
    nav.replaceChildren(navLink("/me.html", "내 정보", "border border-slate-300 bg-white hover:bg-slate-100"));
    return;
  }
  const next = new URLSearchParams({ next: location.pathname + location.search });
  nav.replaceChildren(navLink(`/login.html?${next}`, "로그인", "bg-blue-600 font-semibold text-white hover:bg-blue-700"));
}

const authNav = document.getElementById("auth-nav");
if (authNav) renderAuthNav(authNav).catch(console.error);
