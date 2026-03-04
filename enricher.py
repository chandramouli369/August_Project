"""
Data enrichment service for firmographic and contact data.
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional

import httpx


class Enricher:
    """Handles data enrichment for firms."""

    def __init__(self, base_url: str, timeout: float = 30.0, max_retries: int = 3):
        """
        Initialize enricher with API configuration.

        Args:
            base_url: Base URL for enrichment API
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts per request
        """
        self._client = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout)
        self._max_retries = max_retries

    def _get_with_retries(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Perform a GET request with retry and backoff, handling 429 and 5xx errors.

        Args:
            path: Relative request path (e.g. "/firms/{id}/firmographic")

        Returns:
            Parsed JSON response as a dict, or None if the request ultimately fails.
        """
        backoff = 1.0

        for attempt in range(self._max_retries):
            try:
                response = self._client.get(path)

                # Handle server-enforced rate limiting explicitly
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    try:
                        sleep_seconds = float(retry_after) if retry_after is not None else 1.0
                    except ValueError:
                        sleep_seconds = 1.0
                    time.sleep(max(sleep_seconds, 0.1))
                    continue

                response.raise_for_status()
                data = response.json()
                if isinstance(data, dict):
                    return data
                return None

            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code

                if 500 <= status < 600 and attempt < self._max_retries - 1:
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                return None

            except httpx.RequestError:
                if attempt < self._max_retries - 1:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return None

        return None

    def fetch_firmographic(self, firm_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch firmographic data for a firm.

        Args:
            firm_id: Unique identifier for the firm

        Returns:
            Firmographic data or None if unavailable
        """
        data = self._get_with_retries(f"/firms/{firm_id}/firmographic")
        if data is None:
            return None

        if "num_lawyers" not in data and "lawyer_count" in data:
            data["num_lawyers"] = data.pop("lawyer_count")

        return data

    def fetch_contact(self, firm_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch contact information for a firm.

        Args:
            firm_id: Unique identifier for the firm

        Returns:
            Contact data or None if unavailable
        """
        data = self._get_with_retries(f"/firms/{firm_id}/contact")
        if data is None:
            return None

        return data

    def close(self) -> None:
        """Release underlying HTTP resources."""
        self._client.close()
