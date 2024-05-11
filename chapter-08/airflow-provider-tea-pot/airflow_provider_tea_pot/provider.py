import typing
import airflow_provider_tea_pot


def get_provider_info() -> typing.Dict[str,typing.Any]:
    return {
        "package-name": "airflow-provider-tea-pot",  # Required
        "name": "Teapot Provider",  # Required
        "description": "`A short and stout provider for Pakt Publication <https://github.com/PacktPublishing/Apache-Airflow-Best-Practices>`__",  # Required
        "versions": airflow_provider_tea_pot.__version__,  # Required
        "connection-types" : [
             {
                "connection-type": "teapot",
                "hook-class-name": "airflow_provider_tea_pot.hooks.TeaPotHook"
            }
        ]
    }
