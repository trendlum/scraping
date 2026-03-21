from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from store_eta.supabase import _require_store_eta_supabase_env


def test_require_store_eta_supabase_env_prefers_scraping_names(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_URL", "https://scraping.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "scraping-secret")
    monkeypatch.setenv("SUPABASE_URL_DI", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY_DI", "secret")

    assert _require_store_eta_supabase_env("url") == "https://scraping.supabase.co"
    assert _require_store_eta_supabase_env("key") == "scraping-secret"


def test_require_store_eta_supabase_env_falls_back_to_di_names(monkeypatch) -> None:
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_KEY", raising=False)
    monkeypatch.setenv("SUPABASE_URL_DI", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY_DI", "secret")

    assert _require_store_eta_supabase_env("url") == "https://example.supabase.co"
    assert _require_store_eta_supabase_env("key") == "secret"
