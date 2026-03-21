import json
import os
import re
import time
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Falta variable de entorno requerida: {name}")
    return value


def _require_store_eta_supabase_env(kind: str) -> str:
    if kind == "url":
        return os.getenv("SUPABASE_URL", "").strip() or _require_env("SUPABASE_URL_DI")
    if kind == "key":
        return os.getenv("SUPABASE_KEY", "").strip() or _require_env("SUPABASE_KEY_DI")
    raise ValueError(f"Tipo de credencial Supabase no soportado: {kind}")


def _is_retryable_supabase_error(status_code: Optional[int], detail: str) -> bool:
    detail_lower = (detail or "").lower()

    if status_code in {500, 502, 503, 504}:
        return True

    retryable_patterns = [
        "57014",  # statement timeout en postgres
        "statement timeout",
        "canceling statement due to statement timeout",
        "temporarily unavailable",
        "timeout",
        "connection reset",
    ]
    return any(pattern in detail_lower for pattern in retryable_patterns)


def _supabase_request(
    method: str,
    endpoint: str,
    supabase_key: str,
    body: Optional[Dict[str, object]] = None,
    timeout_seconds: int = 60,
    max_retries: int = 3,
) -> str:
    payload = None
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }

    if body is not None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
        headers["Prefer"] = "return=minimal"

    backoff_seconds = [2, 5, 10]

    for attempt in range(1, max_retries + 1):
        request = Request(endpoint, headers=headers, data=payload, method=method)

        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return response.read().decode("utf-8")

        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            retryable = _is_retryable_supabase_error(exc.code, detail)

            print(
                f"[supabase] HTTPError intento {attempt}/{max_retries} "
                f"status={exc.code} endpoint={endpoint} detail={detail}"
            )

            if retryable and attempt < max_retries:
                sleep_for = backoff_seconds[min(attempt - 1, len(backoff_seconds) - 1)]
                print(f"[supabase] Reintentando en {sleep_for}s...")
                time.sleep(sleep_for)
                continue

            raise ValueError(f"Error HTTP de Supabase ({exc.code}): {detail}") from exc

        except URLError as exc:
            reason = str(exc.reason)
            print(
                f"[supabase] URLError intento {attempt}/{max_retries} "
                f"endpoint={endpoint} reason={reason}"
            )

            if attempt < max_retries:
                sleep_for = backoff_seconds[min(attempt - 1, len(backoff_seconds) - 1)]
                print(f"[supabase] Reintentando en {sleep_for}s...")
                time.sleep(sleep_for)
                continue

            raise ValueError(f"No se pudo conectar a Supabase: {reason}") from exc

    raise ValueError("No se pudo completar la petición a Supabase tras varios intentos.")


def load_name_url_rows_from_supabase(
    table: str,
    name_column: str = "name",
    url_column: str = "url",
) -> List[Tuple[str, str]]:
    if not table.strip():
        raise ValueError("Debes indicar el nombre de tabla de Supabase.")

    supabase_url = _require_store_eta_supabase_env("url").rstrip("/")
    supabase_key = _require_store_eta_supabase_env("key")
    select_value = f"{quote(name_column, safe='')},{quote(url_column, safe='')}"
    endpoint = f"{supabase_url}/rest/v1/{table}?select={select_value}"

    payload = _supabase_request(method="GET", endpoint=endpoint, supabase_key=supabase_key)
    try:
        rows = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError("Respuesta inválida de Supabase (no es JSON).") from exc
    if not isinstance(rows, list):
        raise ValueError("Respuesta inesperada de Supabase: se esperaba una lista.")

    parsed_rows: List[Tuple[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get(name_column) or "").strip()
        url = str(row.get(url_column) or "").strip()
        if name and url:
            parsed_rows.append((name, url))
    return parsed_rows


def normalize_restaurant_column_name(name: str) -> str:
    cleaned_name = re.sub(r"([A-Za-z])s['’]s\b", r"\1s", name)
    cleaned_name = cleaned_name.replace("'", "").replace("’", "")
    normalized = unicodedata.normalize("NFKD", cleaned_name).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower().strip()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    if not normalized:
        raise ValueError(f"Nombre de restaurante inválido para columna: {name!r}")
    if normalized[0].isdigit():
        normalized = f"r_{normalized}"
    return normalized


def build_eta_snapshot_payload(
    rows_eta: List[Tuple[str, str]],
    timestamp_column: str = "captured_at",
    captured_at: Optional[datetime] = None,
) -> Dict[str, str]:
    if not timestamp_column.strip():
        raise ValueError("Debes indicar la columna de timestamp.")

    payload: Dict[str, str] = {
        timestamp_column: (captured_at or datetime.now().astimezone()).isoformat()
    }
    seen_columns = {timestamp_column}
    for restaurant_name, eta_text in rows_eta:
        column_name = normalize_restaurant_column_name(restaurant_name)
        if column_name in seen_columns:
            raise ValueError(
                "Hay nombres que se convierten a la misma columna: "
                f"{restaurant_name!r} -> {column_name!r}"
            )
        seen_columns.add(column_name)
        payload[column_name] = eta_text
    return payload


def build_eta_snapshot_payload_by_order(
    eta_values: List[str],
    output_columns: List[str],
    timestamp_column: str = "captured_at",
    captured_at: Optional[datetime] = None,
) -> Dict[str, str]:
    if not timestamp_column.strip():
        raise ValueError("Debes indicar la columna de timestamp.")
    if len(eta_values) != len(output_columns):
        raise ValueError(
            "La cantidad de ETAs no coincide con columnas destino: "
            f"{len(eta_values)} != {len(output_columns)}"
        )

    payload: Dict[str, str] = {
        timestamp_column: (captured_at or datetime.now().astimezone()).isoformat()
    }
    seen_columns = {timestamp_column}
    for column_name, eta_text in zip(output_columns, eta_values):
        cleaned = column_name.strip()
        if not cleaned:
            raise ValueError("Hay una columna de salida vacía.")
        if cleaned in seen_columns:
            raise ValueError(f"Columna repetida en salida: {cleaned!r}")
        seen_columns.add(cleaned)
        payload[cleaned] = eta_text
    return payload


def insert_eta_snapshot_in_supabase(table: str, payload: Dict[str, str]) -> None:
    if not table.strip():
        raise ValueError("Debes indicar el nombre de tabla destino.")

    supabase_url = _require_store_eta_supabase_env("url").rstrip("/")
    supabase_key = _require_store_eta_supabase_env("key")
    endpoint = f"{supabase_url}/rest/v1/{table}"

    print(
        "[supabase] Insertando snapshot "
        f"table={table} "
        f"captured_at={payload.get('captured_at')} "
        f"columns={sorted(payload.keys())}"
    )

    _supabase_request(
        method="POST",
        endpoint=endpoint,
        supabase_key=supabase_key,
        body=payload,
        timeout_seconds=60,
        max_retries=3,
    )
