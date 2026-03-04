"""
ICP scoring system for evaluating firm fit.
"""
from __future__ import annotations

from typing import Any, Dict


class ICPScorer:
    """Scores firms against ideal customer profile criteria."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize scorer with ICP configuration.

        Args:
            config: ICP scoring configuration (typically loaded from config.yaml)
        """
        self._criteria = config.get("icp_criteria", {})

        firm_size_cfg = self._criteria.get("firm_size", {})
        practice_cfg = self._criteria.get("practice_areas", {})
        geo_cfg = self._criteria.get("geography", {})

        self._min_lawyers = firm_size_cfg.get("min_lawyers", 0)
        self._max_lawyers = firm_size_cfg.get("max_lawyers", 0)

        self._preferred_practice_areas = set(practice_cfg.get("preferred", []))
        self._preferred_regions = set(geo_cfg.get("preferred_regions", []))

        self._w_size = float(firm_size_cfg.get("weight", 0.4))
        self._w_practice = float(practice_cfg.get("weight", 0.3))
        self._w_geo = float(geo_cfg.get("weight", 0.3))

        self._weight_sum = max(self._w_size + self._w_practice + self._w_geo, 1e-6)

    def _score_firm_size(self, num_lawyers: int | None) -> float:
        if not num_lawyers or self._min_lawyers <= 0 or self._max_lawyers <= 0:
            return 0.0

        if self._min_lawyers <= num_lawyers <= self._max_lawyers:
            return 1.0

        return 0.0

    def _score_practice_areas(self, practice_areas: list[str] | None) -> float:
        if not practice_areas or not self._preferred_practice_areas:
            return 0.0

        overlap = self._preferred_practice_areas.intersection(practice_areas)
        if not overlap:
            return 0.0

        return len(overlap) / len(self._preferred_practice_areas)

    def _score_geography(self, country: str | None, region: str | None) -> float:
        if not self._preferred_regions:
            return 0.0

        if country in self._preferred_regions or region in self._preferred_regions:
            return 1.0

        return 0.0

    def score(self, firm: Dict[str, Any]) -> float:
        """
        Calculate ICP score for a firm.

        Args:
            firm: Firm data with enriched information

        Returns:
            ICP score between 0.0 and 1.0
        """
        num_lawyers = firm.get("num_lawyers")
        practice_areas = firm.get("practice_areas") or []
        country = firm.get("country")
        region = firm.get("region")

        size_score = self._score_firm_size(num_lawyers)
        practice_score = self._score_practice_areas(practice_areas)
        geo_score = self._score_geography(country, region)

        weighted = (
            size_score * self._w_size
            + practice_score * self._w_practice
            + geo_score * self._w_geo
        )
        return max(0.0, min(1.0, weighted / self._weight_sum))
