from __future__ import annotations

import time
from typing import Any

from src.services.vision_service import VisionService
from src.services.vision_service import _extract_response_text


def test_extract_response_text_handles_content_filtered_response() -> None:
    result = {
        "choices": [
            {
                "finish_reason": "content_filter: RECITATION",
                "message": {"role": "assistant"},
            }
        ]
    }

    assert _extract_response_text(result, context="mortgage-page-1") is None


def test_extract_response_text_joins_text_blocks() -> None:
    result = {
        "choices": [
            {
                "message": {
                    "content": [
                        {"type": "text", "text": "alpha"},
                        {"type": "text", "text": "beta"},
                    ]
                }
            }
        ]
    }

    assert _extract_response_text(result, context="multi-page-doc") == "alpha\nbeta"


def test_try_all_endpoints_preserves_response_format_for_local_endpoint(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    class _FakeResponse:
        ok = True
        status_code = 200
        text = ""

    class _FakeSession:
        def post(
            self,
            url: str,
            *,
            json: dict[str, Any],
            headers: dict[str, Any],
            timeout: Any,
        ) -> _FakeResponse:
            captured["url"] = url
            captured["json"] = json
            captured["headers"] = headers
            captured["timeout"] = timeout
            return _FakeResponse()

    monkeypatch.setattr(
        VisionService,
        "get_available_endpoints",
        classmethod(
            lambda cls: [  # noqa: ARG005
                {
                    "url": "http://127.0.0.1:1234/v1/chat/completions",
                    "model": "qwen/qwen3-vl-8b",
                }
            ]
        ),
    )

    service = VisionService()
    service.session = _FakeSession()

    payload = {
        "messages": [{"role": "user", "content": "hi"}],
        "response_format": {"type": "json_schema", "json_schema": {"name": "x"}},
    }

    response = service._try_all_endpoints(payload, timeout=5)  # noqa: SLF001

    assert response is not None
    assert captured["json"]["response_format"] == payload["response_format"]
    assert captured["headers"] == {}


def test_analyze_text_uses_text_only_chat_payload(monkeypatch: Any) -> None:
    class _FakeResponse:
        def json(self) -> dict[str, Any]:
            return {
                "choices": [
                    {
                        "message": {
                            "content": '{"ok": true}',
                        }
                    }
                ]
            }

    service = VisionService()
    captured: dict[str, Any] = {}

    def _fake_try_all_endpoints(payload: dict[str, Any], timeout: int = 120) -> _FakeResponse:
        captured["payload"] = payload
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr(service, "_try_all_endpoints", _fake_try_all_endpoints)

    result = service.analyze_text(
        "Extract this",
        max_tokens=321,
        response_format={"type": "json_schema"},
    )

    assert result == '{"ok": true}'
    assert captured["payload"]["messages"] == [{"role": "user", "content": "Extract this"}]
    assert captured["payload"]["response_format"] == {"type": "json_schema"}
    assert captured["payload"]["max_tokens"] == 321


def test_ensure_endpoints_built_loads_dotenv_for_gemini(tmp_path: Any, monkeypatch: Any) -> None:
    (tmp_path / ".env").write_text('GEMINI_API_KEY="test-gemini-key"\n', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("VISION_CLOUD_ONLY", raising=False)

    original_api_endpoints = VisionService.API_ENDPOINTS
    original_api_urls = VisionService.API_URLS
    original_built = VisionService._endpoints_built  # noqa: SLF001
    try:
        VisionService.reset_health_check()
        VisionService._ensure_endpoints_built()  # noqa: SLF001

        first = VisionService.API_ENDPOINTS[0]
        assert first["url"] == "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
        assert first["model"] == "gemini-2.5-flash-lite"
        assert first.get("api_key") == "test-gemini-key"
    finally:
        VisionService.API_ENDPOINTS = original_api_endpoints
        VisionService.API_URLS = original_api_urls
        VisionService._endpoints_built = original_built  # noqa: SLF001
        VisionService.reset_health_check()


def test_get_available_endpoints_bootstraps_health_check(monkeypatch: Any) -> None:
    fake_healthy = [
        {
            "url": "https://example.invalid/v1/chat/completions",
            "model": "gemini-2.5-flash-lite",
            "api_key": "test-key",
        }
    ]
    calls: list[int] = []

    def _fake_health_check(cls, timeout: int = 15) -> list[dict[str, Any]]:
        calls.append(timeout)
        cls._healthy_endpoints = fake_healthy
        cls._health_check_done = True
        return fake_healthy

    original_health_done = VisionService._health_check_done  # noqa: SLF001
    original_healthy = VisionService._healthy_endpoints  # noqa: SLF001
    try:
        VisionService._health_check_done = False  # noqa: SLF001
        VisionService._healthy_endpoints = None  # noqa: SLF001
        monkeypatch.setattr(VisionService, "health_check_endpoints", classmethod(_fake_health_check))

        assert VisionService.get_available_endpoints() == fake_healthy
        assert calls == [3]
    finally:
        VisionService._health_check_done = original_health_done  # noqa: SLF001
        VisionService._healthy_endpoints = original_healthy  # noqa: SLF001


def test_try_all_endpoints_probes_suspended_endpoint_when_all_candidates_are_suspended(
    monkeypatch: Any,
) -> None:
    endpoints = [
        {
            "url": "https://example.invalid/v1/chat/completions",
            "model": "gemini-2.5-flash-lite",
            "api_key": "test-key",
        },
        {
            "url": "http://127.0.0.1:1234/v1/chat/completions",
            "model": "qwen/qwen3-vl-8b",
        },
    ]

    class _FakeResponse:
        def __init__(self, *, ok: bool, status_code: int, text: str = "") -> None:
            self.ok = ok
            self.status_code = status_code
            self.text = text

    class _FakeSession:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def post(
            self,
            url: str,
            *,
            json: dict[str, Any],
            headers: dict[str, Any],
            timeout: Any,
        ) -> _FakeResponse:
            self.calls.append(url)
            if len(self.calls) <= 2:
                return _FakeResponse(ok=False, status_code=503, text="temporary overload")
            return _FakeResponse(ok=True, status_code=200)

    original_health_done = VisionService._health_check_done  # noqa: SLF001
    original_healthy = VisionService._healthy_endpoints  # noqa: SLF001
    original_suspended = VisionService._suspended_endpoints.copy()  # noqa: SLF001
    original_api_endpoints = VisionService.API_ENDPOINTS
    original_api_urls = VisionService.API_URLS
    original_built = VisionService._endpoints_built  # noqa: SLF001
    try:
        VisionService._health_check_done = True  # noqa: SLF001
        VisionService._healthy_endpoints = endpoints  # noqa: SLF001
        VisionService._suspended_endpoints = {}  # noqa: SLF001
        VisionService.API_ENDPOINTS = endpoints
        VisionService.API_URLS = [endpoint["url"] for endpoint in endpoints]
        VisionService._endpoints_built = True  # noqa: SLF001
        monkeypatch.setattr(time, "sleep", lambda _: None)

        service = VisionService()
        service.session = _FakeSession()

        payload = {"messages": [{"role": "user", "content": "hi"}]}

        assert service._try_all_endpoints(payload, timeout=5) is None  # noqa: SLF001
        assert set(VisionService._suspended_endpoints) == {  # noqa: SLF001
            endpoints[0]["url"],
            endpoints[1]["url"],
        }

        response = service._try_all_endpoints(payload, timeout=5)  # noqa: SLF001

        assert response is not None
        assert service.session.calls == [
            endpoints[0]["url"],
            endpoints[1]["url"],
            endpoints[0]["url"],
        ]
    finally:
        VisionService._health_check_done = original_health_done  # noqa: SLF001
        VisionService._healthy_endpoints = original_healthy  # noqa: SLF001
        VisionService._suspended_endpoints = original_suspended  # noqa: SLF001
        VisionService.API_ENDPOINTS = original_api_endpoints
        VisionService.API_URLS = original_api_urls
        VisionService._endpoints_built = original_built  # noqa: SLF001
