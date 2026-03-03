import os
from typing import Iterable, List, Optional

from playwright.sync_api import Page, Playwright, sync_playwright

PRIVATE_XPATHS_ENV_VAR = "STORE_ETA_PRIVATE_XPATHS"
PRIVATE_CONSENT_ENV_VAR = "STORE_ETA_PRIVATE_CONSENT_BUTTONS"
PRIVATE_LIST_SEPARATOR = "||"
FAST_WAIT_MAX_MS = 7000
ETA_ATTEMPT_MAX_MS = 4000
OVERLAY_CHECK_TIMEOUT_MS = 250
OVERLAY_CLICK_TIMEOUT_MS = 500
DEBUG_ENV_VAR = "STORE_ETA_DEBUG"


def _is_debug_enabled() -> bool:
    value = os.getenv(DEBUG_ENV_VAR, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _load_private_list_env(name: str, required: bool) -> List[str]:
    raw_value = os.getenv(name, "").strip()
    values = [item.strip() for item in raw_value.split(PRIVATE_LIST_SEPARATOR) if item.strip()]
    if required and not values:
        raise ValueError(
            f"Falta configuracion privada: {name}. "
            f"Define valores separados por {PRIVATE_LIST_SEPARATOR!r}."
        )
    return values


def _dump_debug_artifacts(page: Page) -> None:
    page.screenshot(path="debug_before_wait.png", full_page=True)
    with open("debug_before_wait.html", "w", encoding="utf-8") as handle:
        handle.write(page.content())
    print(page.url)
    print(page.title())


def _dismiss_overlays(page: Page, consent_labels: List[str]) -> None:
    for name in consent_labels:
        try:
            button = page.get_by_role("button", name=name).first
            if button.is_visible(timeout=OVERLAY_CHECK_TIMEOUT_MS):
                button.click(timeout=OVERLAY_CLICK_TIMEOUT_MS)
                return
        except Exception:
            continue


def _extract_visible_text(page: Page, xpath: str, timeout_ms: int) -> str:
    locator = page.locator(f"xpath={xpath}").first
    locator.wait_for(state="visible", timeout=timeout_ms)
    text = locator.inner_text().strip()
    if not text:
        raise ValueError("Se encontro el contenedor ETA, pero sin texto visible.")
    return text


def _extract_eta_text(page: Page, xpaths: List[str], timeout_ms: int) -> str:
    per_attempt_timeout_ms = max(1000, min(ETA_ATTEMPT_MAX_MS, timeout_ms))
    last_error: Optional[Exception] = None
    for xpath in xpaths:
        try:
            return _extract_visible_text(page, xpath=xpath, timeout_ms=per_attempt_timeout_ms)
        except Exception as exc:
            last_error = exc
    raise ValueError("No se pudo extraer ETA visible.") from last_error


def _scrape_eta_text(
    page: Page,
    url: str,
    timeout_ms: int,
    xpaths: List[str],
    consent_labels: List[str],
) -> str:
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    if _is_debug_enabled():
        _dump_debug_artifacts(page)
    try:
        page.wait_for_function(
            "() => document.body && document.body.innerText && document.body.innerText.trim().length > 0",
            timeout=min(timeout_ms, FAST_WAIT_MAX_MS),
        )
    except Exception:
        pass
    _dismiss_overlays(page, consent_labels=consent_labels)
    return _extract_eta_text(page, xpaths=xpaths, timeout_ms=timeout_ms)


def _create_browser_context(playwright: Playwright, headless: bool):
    browser = playwright.chromium.launch(headless=headless)
    context = browser.new_context(locale="es-ES")
    context.route(
        "**/*",
        lambda route: route.abort()
        if route.request.resource_type in {"image", "font", "media"}
        else route.continue_(),
    )
    return browser, context


def get_store_eta_texts(
    urls: Iterable[str],
    headless: bool = True,
    timeout_ms: int = 60000,
    continue_on_error: bool = False,
) -> List[str]:
    urls_list = list(urls)
    xpaths = _load_private_list_env(PRIVATE_XPATHS_ENV_VAR, required=True)
    consent_labels = _load_private_list_env(PRIVATE_CONSENT_ENV_VAR, required=False)

    with sync_playwright() as playwright:
        browser, context = _create_browser_context(playwright, headless=headless)
        try:
            page = context.new_page()
            try:
                results: List[str] = []
                for url in urls_list:
                    try:
                        results.append(
                            _scrape_eta_text(
                                page,
                                url=url,
                                timeout_ms=timeout_ms,
                                xpaths=xpaths,
                                consent_labels=consent_labels,
                            )
                        )
                    except Exception as exc:
                        if continue_on_error:
                            results.append(f"ERROR: {exc}")
                            continue
                        raise
                return results
            finally:
                page.close()
        finally:
            context.close()
            browser.close()
