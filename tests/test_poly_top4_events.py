from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from poly import list_top4_polymarket_events as module


def test_select_top_events_includes_icon_and_buy_yes(monkeypatch) -> None:
    monkeypatch.setattr(
        module,
        "fetch_polymarket_events_by_tag_slug",
        lambda polymarket_events_url, events_page_size, tag_slug: [
            {
                "id": "30829",
                "title": "Democratic Presidential Nominee 2028",
                "slug": "democratic-presidential-nominee-2028",
                "endDate": "2028-11-07T00:00:00Z",
                "description": "desc",
                "active": True,
                "closed": False,
                "resolutionSource": "",
                "icon": "https://example.com/icon.png",
                "liquidity": 100,
                "volume": 200,
                "openInterest": 300,
                "volume24hr": 400,
                "volume1wk": 500,
                "volume1mo": 600,
                "volume1yr": 700,
                "markets": [
                    {
                        "outcomePrices": "[\"0.0115\", \"0.9885\"]",
                    }
                ],
            }
        ],
    )

    rows = module.select_top_events_for_categories(
        polymarket_events_url="https://example.com/events",
        events_page_size=50,
        top_events_count=4,
        top_active_categories=[{"id": 12, "slug": "politics"}],
    )

    assert len(rows) == 1
    assert rows[0]["icon"] == "https://example.com/icon.png"
    assert rows[0]["buy_yes"] == 0.0115
    assert rows[0]["category_id"] == 12
