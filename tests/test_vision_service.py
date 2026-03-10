from __future__ import annotations

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
