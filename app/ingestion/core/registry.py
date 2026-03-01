"""
Adapter registry: map dataset_id -> adapter class (not instance).

Adapters are stateless; instantiate per run via adapter_cls().
No DB, no lifecycle. Only registration and lookup.
"""

from typing import Dict, Optional, Type

from app.ingestion.core.base_adapter import BaseAdapter


class AdapterRegistry:
    """Maps dataset_id to adapter class. Caller instantiates per run."""

    def __init__(self) -> None:
        self._adapters: Dict[str, Type[BaseAdapter]] = {}

    def register(self, dataset_id: str, adapter_cls: Type[BaseAdapter]) -> None:
        """Register an adapter class for a dataset_id."""
        self._adapters[dataset_id] = adapter_cls

    def get(self, dataset_id: str) -> Optional[Type[BaseAdapter]]:
        """Return adapter class for dataset_id. Caller does adapter = adapter_cls()."""
        return self._adapters.get(dataset_id)

    def list_datasets(self) -> list[str]:
        """Return all registered dataset_ids."""
        return list(self._adapters.keys())


# Singleton for use once adapters are wired (not used by old logic yet).
registry = AdapterRegistry()
