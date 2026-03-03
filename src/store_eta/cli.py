import os
from time import perf_counter
from typing import List, Tuple

from dotenv import load_dotenv

from .scraper import get_store_eta_texts
from .supabase import (
    build_eta_snapshot_payload,
    build_eta_snapshot_payload_by_order,
    insert_eta_snapshot_in_supabase,
    load_name_url_rows_from_supabase,
)

Row = Tuple[str, str]


def _get_env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default.strip()
    cleaned = value.strip()
    return cleaned if cleaned else default.strip()


def _parse_bool_env(name: str, default: bool) -> bool:
    value = _get_env(name)
    if not value:
        return default
    return value.lower() in {"1", "true", "yes", "on", "si", "s"}


def _parse_int_env(name: str, default: int) -> int:
    value = _get_env(name)
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"Valor inválido para {name}: {value!r}. Debe ser entero.") from exc
    if parsed <= 0:
        raise ValueError(f"Valor inválido para {name}: {parsed}. Debe ser > 0.")
    return parsed


def _build_payload(
    rows: List[Row],
    eta_values: List[str],
    timestamp_column: str,
    output_columns: str,
):
    output_columns_list = [c.strip() for c in output_columns.split(",") if c.strip()]
    if output_columns_list:
        return build_eta_snapshot_payload_by_order(
            eta_values=eta_values,
            output_columns=output_columns_list,
            timestamp_column=timestamp_column,
        )
    rows_eta = list(zip((name for name, _ in rows), eta_values))
    return build_eta_snapshot_payload(rows_eta=rows_eta, timestamp_column=timestamp_column)


def main() -> None:
    load_dotenv()
    source_table = _get_env("SUPABASE_SOURCE_TABLE", "stores")
    source_name_column = _get_env("SUPABASE_SOURCE_NAME_COLUMN", "name")
    source_url_column = _get_env("SUPABASE_SOURCE_URL_COLUMN", "url")
    output_table = _get_env("SUPABASE_OUTPUT_TABLE", "eta_snapshots")
    output_timestamp_column = _get_env("SUPABASE_OUTPUT_TIMESTAMP_COLUMN", "captured_at")
    output_columns = _get_env("SUPABASE_OUTPUT_COLUMNS", "")
    headless = not _parse_bool_env("SCRAPER_HEADFUL", default=False)
    timeout_ms = _parse_int_env("SCRAPER_TIMEOUT_MS", default=60000)

    rows: List[Row] = load_name_url_rows_from_supabase(
        table=source_table,
        name_column=source_name_column,
        url_column=source_url_column,
    )
    if not rows:
        raise ValueError("No se encontraron filas válidas para ejecutar.")

    started_at = perf_counter()
    eta_values = get_store_eta_texts(
        urls=[url for _, url in rows],
        headless=headless,
        timeout_ms=timeout_ms,
        continue_on_error=True,
    )

    for (name, _), eta_text in zip(rows, eta_values):
        print(f"{name},{eta_text}")

    payload = _build_payload(
        rows=rows,
        eta_values=eta_values,
        timestamp_column=output_timestamp_column,
        output_columns=output_columns,
    )
    insert_eta_snapshot_in_supabase(table=output_table, payload=payload)
    print(f"snapshot_inserted,{output_table}")
    print(f"elapsed_seconds,{perf_counter() - started_at:.2f}")


if __name__ == "__main__":
    raise SystemExit("Ejecuta __main__.py.")
