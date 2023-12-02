
__version__ = "0.0.0a"

def get_provider_info():
    return {
        "package-name": "airflow-provider-tea-pot",  # Required
        "name": "airflow-provider-tea-pot",  # Required
        "description": "A provider for something short and stout.",  # Required
        "versions": [__version__],  # Required
    }
