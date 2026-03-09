# ruff: noqa: SLF001
from __future__ import annotations

import asyncio
from typing import Any

from src.scrapers.redfin_scraper import RedfinScraper


class _FakeKeyboard:
    def __init__(self, page: _FakePage, url_after_enter: str | None = None) -> None:
        self._page = page
        self._url_after_enter = url_after_enter

    async def press(self, key: str) -> None:
        if key == "Enter" and self._url_after_enter is not None:
            self._page.url = self._url_after_enter


class _FakeElement:
    def __init__(self, href: str | None = None) -> None:
        self._href = href

    async def click(self) -> None:
        return None

    async def get_attribute(self, name: str) -> str | None:
        if name == "href":
            return self._href
        return None


class _FakePage:
    def __init__(
        self,
        *,
        url: str,
        url_after_enter: str | None = None,
        first_result_href: str | None = None,
    ) -> None:
        self.url = url
        self.keyboard = _FakeKeyboard(self, url_after_enter=url_after_enter)
        self._search_box = _FakeElement()
        self._first_result = (
            _FakeElement(first_result_href) if first_result_href is not None else None
        )

    async def query_selector(self, selector: str) -> _FakeElement | None:
        if selector == "button[aria-label=\"Clear context\"]":
            return None
        if selector == "input#search-box-input, input[type=\"search\"], input[placeholder*=\"Address\"]":
            return self._search_box
        if selector == "a[href*=\"/home/\"]":
            return self._first_result
        return None

    async def wait_for_selector(self, _selector: str, _timeout: int = 0) -> None:
        return None

    async def wait_for_timeout(self, _timeout_ms: int) -> None:
        return None


async def _noop(*_args: Any, **_kwargs: Any) -> None:
    return None


def test_search_property_rejects_stale_previous_listing(monkeypatch: Any) -> None:
    scraper = object.__new__(RedfinScraper)
    scraper._page = _FakePage(
        url="https://www.redfin.com/FL/Tampa/2131-W-Cypress-St-33606/home/11111111"
    )
    scraper._force_remove_dialogs = _noop  # type: ignore[method-assign]
    scraper.delay = _noop  # type: ignore[method-assign]

    async def _fake_human_type(_cdp: Any, _text: str) -> None:
        return None

    monkeypatch.setattr(RedfinScraper, "human_type", staticmethod(_fake_human_type))

    status, listing = asyncio.run(
        scraper.search_property(object(), "2535 MIDDLETON GROVE DR")
    )

    assert status == "not_found"
    assert listing is None


def test_search_property_rejects_mismatched_detail_url(monkeypatch: Any) -> None:
    scraper = object.__new__(RedfinScraper)
    scraper._page = _FakePage(
        url="https://www.redfin.com/",
        url_after_enter="https://www.redfin.com/FL/Brandon/604-Julie-Ln-33511/home/47212003",
    )
    scraper._force_remove_dialogs = _noop  # type: ignore[method-assign]
    scraper.delay = _noop  # type: ignore[method-assign]

    async def _fake_human_type(_cdp: Any, _text: str) -> None:
        return None

    monkeypatch.setattr(RedfinScraper, "human_type", staticmethod(_fake_human_type))

    status, listing = asyncio.run(
        scraper.search_property(object(), "2535 MIDDLETON GROVE DR")
    )

    assert status == "not_found"
    assert listing is None


def test_search_property_rejects_mismatched_first_search_result(monkeypatch: Any) -> None:
    scraper = object.__new__(RedfinScraper)
    scraper._page = _FakePage(
        url="https://www.redfin.com/",
        url_after_enter="https://www.redfin.com/city/18203/FL/Brandon/filter/property-type=house",
        first_result_href="/FL/Brandon/604-Julie-Ln-33511/home/47212003",
    )
    scraper._force_remove_dialogs = _noop  # type: ignore[method-assign]
    scraper.delay = _noop  # type: ignore[method-assign]

    async def _fake_human_type(_cdp: Any, _text: str) -> None:
        return None

    monkeypatch.setattr(RedfinScraper, "human_type", staticmethod(_fake_human_type))

    status, listing = asyncio.run(
        scraper.search_property(object(), "2535 MIDDLETON GROVE DR")
    )

    assert status == "not_found"
    assert listing is None
