"""
Experiment assignment system for A/B testing.
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List


class ExperimentAssigner:
    """Assigns leads to experiment variants."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize experiment assigner with configuration.

        Args:
            config: Experiment configuration (expects an `experiments` section)
        """
        experiments_cfg = config.get("experiments", {})
        email_cfg = experiments_cfg.get("email_variants", {}) or {}

        # Maintain a stable ordering of variants for deterministic hashing.
        self._variants: List[str] = sorted(email_cfg.keys())
        if not self._variants:
            # Fallback to a single default variant if none configured.
            self._variants = ["variant_a"]

    def assign_variant(self, lead_id: str) -> str:
        """
        Assign a lead to an experiment variant.

        Args:
            lead_id: Unique lead identifier

        Returns:
            Experiment variant identifier (e.g. "variant_a" or "variant_b")
        """
        # Deterministic hashing so the same lead_id always maps to the same variant.
        digest = hashlib.sha256(lead_id.encode("utf-8")).hexdigest()
        bucket = int(digest, 16) % len(self._variants)
        return self._variants[bucket]

