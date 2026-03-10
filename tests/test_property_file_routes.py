from app.web.routers.properties import router


def test_local_file_routes_allow_head_requests() -> None:
    methods_by_path = {
        route.path: route.methods
        for route in router.routes
        if hasattr(route, "methods")
    }

    assert methods_by_path["/{folio}/doc/{doc_id}"] == {"GET", "HEAD"}
    assert methods_by_path["/{folio}/documents/{filename:path}"] == {
        "GET",
        "HEAD",
    }
    assert methods_by_path["/{folio}/photos/{filename}"] == {"GET", "HEAD"}
