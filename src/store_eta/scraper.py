import os
from typing import Iterable, List, Optional

from playwright.sync_api import Page, Playwright, sync_playwright

XPATH_ETA = (
    "//span[@data-testid='rich-text' and normalize-space()='Tiempo mínimo de llegada']"
    "/ancestor::p[1]/preceding-sibling::p[1]"
    "//span[@data-testid='rich-text']"
)
XPATH_ETA_BY_MAIN_LABELS = (
    "//span[@data-testid='rich-text' and ("
    "contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'hora de entrega')"
    " or contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'tiempo mínimo de llegada')"
    " or contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'tiempo minimo de llegada')"
    ")]/ancestor::p[1]/preceding-sibling::p[1]//span[@data-testid='rich-text']"
)
XPATH_ETA_BY_FLEX_LABELS = (
    "//span[@data-testid='rich-text' and ("
    "contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'entrega')"
    " or contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'llegada')"
    ")]/ancestor::p[1]/preceding-sibling::p[1]//span[@data-testid='rich-text']"
)
CONSENT_BUTTON_NAMES = ("Aceptar", "Aceptar todo", "Accept", "Accept all")
FAST_WAIT_MAX_MS = 7000
ETA_ATTEMPT_MAX_MS = 4000
OVERLAY_CHECK_TIMEOUT_MS = 250
OVERLAY_CLICK_TIMEOUT_MS = 500
DEBUG_ENV_VAR = "STORE_ETA_DEBUG"


def _is_debug_enabled() -> bool:
    value = os.getenv(DEBUG_ENV_VAR, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _dump_debug_artifacts(page: Page) -> None:
    page.screenshot(path="debug_before_wait.png", full_page=True)
    with open("debug_before_wait.html", "w", encoding="utf-8") as handle:
        handle.write(page.content())
    print(page.url)
    print(page.title())


def _dismiss_overlays(page: Page) -> None:
    for name in CONSENT_BUTTON_NAMES:
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
        raise ValueError("Se encontró el contenedor ETA, pero sin texto visible.")
    return text


def _extract_eta_text(page: Page, timeout_ms: int) -> str:
    per_attempt_timeout_ms = max(1000, min(ETA_ATTEMPT_MAX_MS, timeout_ms))
    xpaths = (XPATH_ETA, XPATH_ETA_BY_MAIN_LABELS, XPATH_ETA_BY_FLEX_LABELS)
    last_error: Optional[Exception] = None
    for xpath in xpaths:
        try:
            return _extract_visible_text(page, xpath=xpath, timeout_ms=per_attempt_timeout_ms)
        except Exception as exc:
            last_error = exc
    raise ValueError("No se pudo extraer ETA visible.") from last_error


def _scrape_eta_text(page: Page, url: str, timeout_ms: int) -> str:
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
    _dismiss_overlays(page)
    return _extract_eta_text(page, timeout_ms=timeout_ms)


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
    with sync_playwright() as playwright:
        browser, context = _create_browser_context(playwright, headless=headless)
        try:
            page = context.new_page()
            try:
                results: List[str] = []
                for url in urls_list:
                    try:
                        results.append(_scrape_eta_text(page, url=url, timeout_ms=timeout_ms))
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
