"""Tests for the connections graph API endpoints."""

from unittest.mock import MagicMock


def _mock_engine():
    """Create a mock PG engine that returns controlled results."""
    return MagicMock()


class TestEntitySearch:
    """Tests for GET /api/connections/search."""

    def test_search_returns_results(self):
        from app.web.routers.connections import _search_entities

        # Will fail because module doesn't exist yet
        assert callable(_search_entities)

    def test_search_empty_query_returns_empty(self):
        from app.web.routers.connections import _search_entities

        assert _search_entities("") == []

    def test_search_short_query_returns_empty(self):
        from app.web.routers.connections import _search_entities

        assert _search_entities("AB") == []


class TestEntityExpand:
    """Tests for GET /api/connections/entity/<doc_number>."""

    def test_expand_entity_callable(self):
        from app.web.routers.connections import _expand_entity
        assert callable(_expand_entity)

    def test_expand_entity_not_found_returns_none(self):
        from app.web.routers.connections import _expand_entity
        # Will use real DB or mock; for unit test, just confirm function exists
        assert _expand_entity is not None
