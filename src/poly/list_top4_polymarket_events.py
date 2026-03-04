import os
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from requests.exceptions import HTTPError

def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Falta variable de entorno requerida: {name}")
    return value


def _require_env_any(names: List[str]) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    joined = ", ".join(names)
    raise ValueError(f"Falta variable de entorno requerida. Define una de: {joined}")


def _require_env_int_any(names: List[str], min_value: int = 1) -> int:
    raw = _require_env_any(names)
    try:
        value = int(raw)
    except ValueError as exc:
        joined = ", ".join(names)
        raise ValueError(f"Valor entero invalido para {joined}: {raw!r}") from exc
    if value < min_value:
        joined = ", ".join(names)
        raise ValueError(f"El valor de {joined} debe ser >= {min_value}.")
    return value


def _supabase_headers(supabase_key: str, *, include_json_content_type: bool = False) -> Dict[str, str]:
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }
    if include_json_content_type:
        headers["Content-Type"] = "application/json"
    return headers


def fetch_top_active_categories(
    supabase_url: str,
    supabase_key: str,
    supabase_categories_table: str,
) -> List[Dict[str, Any]]:
    endpoint = (
        f"{supabase_url}/rest/v1/{supabase_categories_table}"
        "?select=id,label,slug&topActive=eq.true"
    )
    response = requests.get(endpoint, headers=_supabase_headers(supabase_key), timeout=30)
    try:
        response.raise_for_status()
    except HTTPError as exc:
        if response.status_code == 404:
            raise ValueError(
                f"No existe la tabla '{supabase_categories_table}' o la columna 'topActive' en Supabase."
            ) from exc
        raise

    raw_rows = response.json()
    if not isinstance(raw_rows, list):
        raise ValueError("Respuesta inesperada al leer categorias topActive de Supabase.")

    categories: List[Dict[str, Any]] = []
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        category_id = _as_int(row.get("id"))
        slug = str(row.get("slug") or "").strip()
        if category_id is None or not slug:
            continue
        categories.append(
            {
                "id": category_id,
                "slug": slug,
                "label": str(row.get("label") or "").strip(),
            }
        )
    return categories


def fetch_polymarket_events_by_tag_slug(
    polymarket_events_url: str,
    events_page_size: int,
    tag_slug: str,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    offset = 0

    while True:
        response = requests.get(
            polymarket_events_url,
            params={"limit": events_page_size, "offset": offset, "tag_slug": tag_slug},
            headers={"accept": "application/json"},
            timeout=45,
        )
        response.raise_for_status()
        page = response.json()

        if not isinstance(page, list):
            raise ValueError("Respuesta inesperada de Polymarket: se esperaba una lista.")
        if not page:
            break

        events.extend(item for item in page if isinstance(item, dict))
        if len(page) < events_page_size:
            break
        offset += events_page_size

        if offset > 10000:
            # Limite de seguridad para evitar loops infinitos ante cambios de API.
            break

    return events


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def select_top_events_for_categories(
    polymarket_events_url: str,
    events_page_size: int,
    top_events_count: int,
    top_active_categories: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    selected_events_by_id: Dict[int, Dict[str, Any]] = {}

    for category in top_active_categories:
        category_id = category["id"]
        tag_slug = category["slug"]
        category_events = fetch_polymarket_events_by_tag_slug(
            polymarket_events_url,
            events_page_size,
            tag_slug,
        )

        for event in category_events:
            event_id = _as_int(event.get("id"))
            if event_id is None:
                continue
            if bool(event.get("closed")):
                continue

            row = {
                "id": event_id,
                "title": event.get("title"),
                "slug": event.get("slug"),
                "category_id": category_id,
                "end_date": event.get("endDate"),
                "description": event.get("description"),
                "active": bool(event.get("active")),
                "closed": bool(event.get("closed")),
                "resolutionSource": event.get("resolutionSource"),
                "liquidity": _as_float(event.get("liquidity")),
                "volume": _as_float(event.get("volume")),
                "openInterest": _as_float(event.get("openInterest")),
                "volume24hr": _as_float(event.get("volume24hr")),
                "volume1wk": _as_float(event.get("volume1wk")),
                "volume1mo": _as_float(event.get("volume1mo")),
                "volume1yr": _as_float(event.get("volume1yr")),
            }

            existing = selected_events_by_id.get(event_id)
            if existing is None or row["volume24hr"] > existing["volume24hr"]:
                selected_events_by_id[event_id] = row

    selected_events = list(selected_events_by_id.values())
    selected_events.sort(key=lambda row: row["volume24hr"], reverse=True)
    return selected_events[:top_events_count]


def replace_top_events_in_supabase(
    supabase_url: str,
    supabase_key: str,
    supabase_output_table: str,
    top_events: List[Dict[str, Any]],
) -> None:
    delete_endpoint = f"{supabase_url}/rest/v1/{supabase_output_table}?id=not.is.null"
    delete_headers = _supabase_headers(supabase_key)
    delete_headers["Prefer"] = "return=minimal"

    delete_response = requests.delete(delete_endpoint, headers=delete_headers, timeout=30)
    try:
        delete_response.raise_for_status()
    except HTTPError as exc:
        if delete_response.status_code == 404:
            raise ValueError(
                f"No existe la tabla '{supabase_output_table}' en Supabase."
            ) from exc
        raise

    if not top_events:
        return

    insert_endpoint = f"{supabase_url}/rest/v1/{supabase_output_table}"
    insert_headers = _supabase_headers(supabase_key, include_json_content_type=True)
    insert_headers["Prefer"] = "return=minimal"

    insert_response = requests.post(
        insert_endpoint,
        headers=insert_headers,
        json=top_events,
        timeout=30,
    )
    insert_response.raise_for_status()


def main() -> None:
    load_dotenv()

    polymarket_events_url = _require_env_any(
        ["POLY_GAMMA_EVENTS_URL", "POLYMARKET_EVENTS_URL"]
    )
    supabase_url = _require_env("SUPABASE_URL").rstrip("/")
    supabase_key = _require_env("SUPABASE_KEY")
    supabase_categories_table = _require_env_any(
        ["POLY_SUPABASE_CATEGORIES_TABLE", "SUPABASE_POLY_CATEGORIES_TABLE"]
    )
    supabase_output_table = _require_env_any(
        ["POLY_SUPABASE_TOP_EVENTS_TABLE", "SUPABASE_POLY_TOP_EVENTS_TABLE"]
    )
    events_page_size = _require_env_int_any(["POLY_EVENTS_PAGE_SIZE"], min_value=1)
    top_events_count = _require_env_int_any(["POLY_TOP_EVENTS_COUNT"], min_value=1)

    top_active_categories = fetch_top_active_categories(
        supabase_url,
        supabase_key,
        supabase_categories_table,
    )
    top_events = select_top_events_for_categories(
        polymarket_events_url,
        events_page_size,
        top_events_count,
        top_active_categories,
    )
    replace_top_events_in_supabase(
        supabase_url,
        supabase_key,
        supabase_output_table,
        top_events,
    )

    print(f"Categorias topActive encontradas: {len(top_active_categories)}")
    print(f"Eventos top guardados en Supabase: {len(top_events)}")


if __name__ == "__main__":
    main()
