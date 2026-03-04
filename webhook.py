"""
Webhook client for firing events to downstream systems.
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional

import httpx


class WebhookClient:
    """Handles webhook delivery to external systems."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize webhook client with configuration.

        Args:
            config: Webhook configuration (typically `apis.webhooks` section)
        """
        self._crm_endpoint = config.get("crm_endpoint")
        self._email_endpoint = config.get("email_endpoint")
        timeout = float(config.get("timeout", 10))
        self._max_retries = int(config.get("max_retries", 2))

        self._client = httpx.Client(timeout=timeout)

    def _post_with_retries(self, url: str, payload: Dict[str, Any]) -> bool:
        """
        Post payload to a single URL with retry and backoff, handling 429 and 5xx.
        """
        backoff = 1.0

        for attempt in range(self._max_retries):
            try:
                response = self._client.post(url, json=payload)

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    try:
                        sleep_seconds = float(retry_after) if retry_after is not None else 1.0
                    except ValueError:
                        sleep_seconds = 1.0
                    time.sleep(max(sleep_seconds, 0.1))
                    continue

                response.raise_for_status()
                return True

            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if 500 <= status < 600 and attempt < self._max_retries - 1:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return False

            except httpx.RequestError:
                if attempt < self._max_retries - 1:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return False

        return False

    def fire(self, payload: Dict[str, Any]) -> bool:
        """
        Fire webhook with payload to configured endpoints.

        Args:
            payload: Data to send in webhook. Expected structure:
                     {"crm": {...}, "email": {...}} but keys are optional.

        Returns:
            True if all requested deliveries were successful, False otherwise.
        """
        all_ok = True

        crm_payload: Optional[Dict[str, Any]] = payload.get("crm")  # type: ignore[assignment]
        email_payload: Optional[Dict[str, Any]] = payload.get("email")  # type: ignore[assignment]

        if crm_payload is not None and self._crm_endpoint:
            ok = self._post_with_retries(self._crm_endpoint, crm_payload)
            all_ok = all_ok and ok

        if email_payload is not None and self._email_endpoint:
            ok = self._post_with_retries(self._email_endpoint, email_payload)
            all_ok = all_ok and ok

        return all_ok

    def close(self) -> None:
        """Release underlying HTTP resources."""
        self._client.close()
