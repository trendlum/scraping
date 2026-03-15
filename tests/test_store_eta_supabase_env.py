from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from store_eta.supabase import _require_store_eta_supabase_env


def test_require_store_eta_supabase_env_uses_di_names(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_URL_DI", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY_DI", "secret")

    assert _require_store_eta_supabase_env("url") == "https://example.supabase.co"
    assert _require_store_eta_supabase_env("key") == "secret"
