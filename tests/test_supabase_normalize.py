from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from store_eta.supabase import normalize_restaurant_column_name


def test_normalize_apostrophe_brand() -> None:
    assert normalize_restaurant_column_name("Wendy's") == "wendys"


def test_normalize_double_possessive_brand() -> None:
    assert normalize_restaurant_column_name("McDonalds's") == "mcdonalds"

