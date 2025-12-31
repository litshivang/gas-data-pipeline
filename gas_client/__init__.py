from gas_client.client import GasClient

_client = GasClient()

def get_history(*args, **kwargs):
    return _client.get_history(*args, **kwargs)
