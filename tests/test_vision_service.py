from __future__ import annotations

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
