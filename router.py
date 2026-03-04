"""
Lead routing system for qualified prospects.
"""
from __future__ import annotations

from typing import Any, Dict


class LeadRouter:
    """Routes qualified leads to appropriate sales representatives."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize router with routing configuration.

        Args:
            config: Full pipeline configuration (uses `routing` section)
        """
        routing_cfg = config.get("routing", {})

        self._high_priority_min_score = float(
            routing_cfg.get("high_priority_min_score", 0.7)
        )
        self._nurture_min_score = float(routing_cfg.get("nurture_min_score", 0.4))

        self._disqualify_below_min_lawyers = int(
            routing_cfg.get("disqualify_below_min_lawyers", 10)
        )
        self._disqualify_above_max_lawyers = int(
            routing_cfg.get("disqualify_above_max_lawyers", 2000)
        )

    def route(self, firm: Dict[str, Any], score: float) -> str:
        """
        Route a qualified lead based on score and firm data.

        Args:
            firm: Firm data (including num_lawyers if available)
            score: ICP score

        Returns:
            Route category: "high_priority", "nurture", or "disqualified"
        """
        num_lawyers = firm.get("num_lawyers")
        if isinstance(num_lawyers, int):
            if (
                num_lawyers < self._disqualify_below_min_lawyers
                or num_lawyers > self._disqualify_above_max_lawyers
            ):
                return "disqualified"

        if score >= self._high_priority_min_score:
            return "high_priority"
        if score >= self._nurture_min_score:
            return "nurture"
        return "disqualified"

