from app.web.routers.properties import _market_photo_urls


def test_market_photo_urls_skips_placeholder_slots_and_keeps_local_alignment() -> None:
    photos, photos_with_fallback = _market_photo_urls(
        "26-CA-000001",
        local_paths=[
            "Foreclosure/26-CA-000001/photos/000_logo.jpg",
            "Foreclosure/26-CA-000001/photos/001_house.jpg",
        ],
        cdn_urls=[
            "https://ssl.cdn-redfin.com/logos/redfin-logo-square-red-1200.png",
            "https://photos.example.com/house.jpg",
        ],
    )

    assert photos == ["/property/26-CA-000001/photos/001_house.jpg"]
    assert photos_with_fallback == [
        {
            "url": "/property/26-CA-000001/photos/001_house.jpg",
            "cdn_fallback": "https://photos.example.com/house.jpg",
        }
    ]
