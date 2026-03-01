# ingestion core — base_adapter, orchestrator, registry, etc.

from app.ingestion.core.base_adapter import BaseAdapter
from app.ingestion.core.registry import AdapterRegistry, registry
from app.ingestion.core.orchestrator import Orchestrator

__all__ = ["BaseAdapter", "AdapterRegistry", "registry", "Orchestrator"]
