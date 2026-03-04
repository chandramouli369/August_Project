"""
Main pipeline orchestrator for GTM data processing.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Tuple

import httpx
import yaml

from enricher import Enricher
from experiment import ExperimentAssigner
from router import LeadRouter
from scorer import ICPScorer
from webhook import WebhookClient


def _get_firms_page(
    client: httpx.Client, page: int, per_page: int, max_retries: int = 3
) -> Dict[str, Any]:
    """
    Fetch a single page of firms with retry and basic backoff logic.
    """
    backoff = 1.0

    for attempt in range(max_retries):
        try:
            response = client.get(
                "/firms",
                params={"page": page, "per_page": per_page},
            )

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
            return {}

        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if 500 <= status < 600 and attempt < max_retries - 1:
                time.sleep(backoff)
                backoff *= 2
                continue
            return {}

        except httpx.RequestError:
            if attempt < max_retries - 1:
                time.sleep(backoff)
                backoff *= 2
                continue
            return {}

    return {}


def _deduplicate_firms(firms: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """
    Deduplicate firms based primarily on domain.

    Returns:
        (unique_firms, num_duplicates)
    """
    seen_domains: Dict[str, Dict[str, Any]] = {}
    unique: List[Dict[str, Any]] = []
    duplicates = 0

    for firm in firms:
        domain = firm.get("domain")
        key = domain.lower() if isinstance(domain, str) else None

        if key is None:
            unique.append(firm)
            continue

        if key in seen_domains:
            duplicates += 1
            continue

        seen_domains[key] = firm
        unique.append(firm)

    return unique, duplicates


def run_pipeline(config_path: str) -> Any:
    """
    Run the complete GTM data pipeline.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Summary of pipeline execution.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    apis_cfg = config.get("apis", {})
    enrichment_cfg = apis_cfg.get("enrichment", {})
    webhooks_cfg = apis_cfg.get("webhooks", {})
    pipeline_cfg = config.get("pipeline", {})

    base_url = enrichment_cfg.get("base_url", "http://localhost:8000")
    timeout = float(enrichment_cfg.get("timeout", 30))
    max_retries = int(enrichment_cfg.get("max_retries", 3))

    per_page = min(int(pipeline_cfg.get("batch_size", 50)), 50)

    enricher = Enricher(
        base_url=base_url,
        timeout=timeout,
        max_retries=max_retries,
    )
    scorer = ICPScorer(config)
    router = LeadRouter(config)
    experimenter = ExperimentAssigner(config)
    webhook_client = WebhookClient(webhooks_cfg)

    http_client = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout)

    all_firms: List[Dict[str, Any]] = []

    try:
        first_page = _get_firms_page(http_client, page=1, per_page=per_page, max_retries=max_retries)
        items = first_page.get("items", [])
        total_pages = int(first_page.get("total_pages", 1))
        all_firms.extend(items)

        for page in range(2, total_pages + 1):
            page_data = _get_firms_page(http_client, page=page, per_page=per_page, max_retries=max_retries)
            all_firms.extend(page_data.get("items", []))

        unique_firms, duplicate_count = _deduplicate_firms(all_firms)

        results: Dict[str, Any] = {
            "total_firms": len(all_firms),
            "unique_firms": len(unique_firms),
            "duplicates_skipped": duplicate_count,
            "routed_counts": {"high_priority": 0, "nurture": 0, "disqualified": 0},
            "webhook_success": 0,
            "webhook_failed": 0,
        }

        for firm in unique_firms:
            firm_id = firm.get("id")
            if not firm_id:
                continue

            firmographic = enricher.fetch_firmographic(firm_id)
            if firmographic is None:
                continue

            # Merge the basic firm stub info with enriched firmographic data.
            merged_firm: Dict[str, Any] = {**firmographic, **firm}

            contact = enricher.fetch_contact(firm_id)

            score = scorer.score(merged_firm)
            route_category = router.route(merged_firm, score)
            results["routed_counts"][route_category] += 1

            if route_category == "disqualified":
                continue

            variant = experimenter.assign_variant(str(firm_id))

            crm_payload = {
                "firm": merged_firm,
                "contact": contact,
                "score": score,
                "route": route_category,
                "experiment_variant": variant,
            }

            email_payload = {
                "firm_id": firm_id,
                "variant": variant,
                "contact": contact,
                "score": score,
                "route": route_category,
            }

            ok = webhook_client.fire({"crm": crm_payload, "email": email_payload})
            if ok:
                results["webhook_success"] += 1
            else:
                results["webhook_failed"] += 1

        return results

    finally:
        http_client.close()
        webhook_client.close()
        enricher.close()

