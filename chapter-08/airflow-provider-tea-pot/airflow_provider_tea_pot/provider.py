import typing
import airflow_provider_tea_pot


def get_provider_info() -> typing.Dict[str,typing.Any]:
    return {
        "package-name": "airflow-provider-tea-pot",  # Required
        "name": "airflow-provider-tea-pot",  # Required
        "description": "Short and stout.",  # Required
        "versions": airflow_provider_tea_pot.__version__,  # Required
        "connection-types" : [
             {
                "connection-type": "teapot",
                "hook-class-name": "airflow_provider_tea_pot.hooks.TeaPotHook"
            }
        ]
    }
