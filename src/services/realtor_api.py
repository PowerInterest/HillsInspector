"""
Realtor data client via RapidAPI.

Requires environment variable:
    RAPIDAPI_KEY: your RapidAPI key for realtor-data1.p.rapidapi.com

Usage:
    from src.services.realtor_api import RealtorRapidAPI
    client = RealtorRapidAPI()
    data = client.search(
        status=["for_sale"],
        postal_code="10022",
        limit=42,
        offset=0,
        sort_field="list_date",
        sort_direction="desc",
    )
"""
import os
import json
from typing import Any, Dict, List, Optional
import requests
from loguru import logger


class RealtorRapidAPI:
    BASE_URL = "https://realtor-data1.p.rapidapi.com/property_list/"
    HOST = "realtor-data1.p.rapidapi.com"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("RAPIDAPI_KEY")
        if not self.api_key:
            logger.warning("RAPIDAPI_KEY not set; RealtorRapidAPI calls will fail without a key.")

    def search(
        self,
        status: List[str],
        postal_code: str,
        limit: int = 42,
        offset: int = 0,
        sort_field: str = "list_date",
        sort_direction: str = "desc",
        extra_query: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Perform a property search.

        Args:
            status: list of status strings (e.g., ["for_sale"])
            postal_code: ZIP code
            limit: max results
            offset: pagination offset
            sort_field: field to sort by
            sort_direction: "asc" or "desc"
            extra_query: optional additional query params to merge

        Returns:
            Parsed JSON dict or None on failure.
        """
        if not self.api_key:
            logger.error("RAPIDAPI_KEY not configured.")
            return None

        query = {
            "status": status,
            "postal_code": postal_code,
        }
        if extra_query:
            query.update(extra_query)

        payload = {
            "query": query,
            "limit": limit,
            "offset": offset,
            "sort": {"direction": sort_direction, "field": sort_field},
        }

        headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.HOST,
            "Content-Type": "application/json",
        }

        try:
            resp = requests.post(self.BASE_URL, headers=headers, data=json.dumps(payload), timeout=20)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Realtor RapidAPI request failed: {e}")
            return None
