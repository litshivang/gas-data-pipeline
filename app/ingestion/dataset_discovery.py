def discover_national_gas_datasets() -> list[str]:
    """
    Returns dataset_ids like PUBOB637, PUBOBJ486, ...
    """
    # Phase-1: static list or config
    # Phase-2: scrape / discover via NG metadata endpoint


def register_datasets(dataset_ids: list[str]):
    """
    Inserts into dataset_registry if missing.
    """
