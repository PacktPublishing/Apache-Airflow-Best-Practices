import logging
import os
from contextlib import contextmanager

from airflow.hooks.base import BaseHook
from airflow.models import Connection, DagBag, Variable

ENV_VARS_NONE = (
    "PYTEST_THEME"
)

def pytest_itemcollected(item):
    """
    use test doc strings as messages for the testing suite
    """
    if item._obj.__doc__:
        item._nodeid = f"{item.obj.__doc__.strip().ljust(50,' ')[:50]}"


@contextmanager
def suppress_logging(namespace):
    """
    Suppress logging within a specific namespace to keep tests "clean" during build
    """
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value

def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        # we prepend "(None,None)" to ensure that a test object is always created even if its a no op.
        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


# =========== MONKEYPATCH OS.GETENV() ===========
def os_getenv_monkeypatch(key: str, *args, **kwargs):

    default=None
    if args:
        default = args[0]
    if kwargs:
        default = kwargs.get('default',None)

    env_value = os.environ.get(key,None)

    if env_value:
        return env_value
    if (
        key in ENV_VARS_NONE and default is None
    ):
        return None
    if default:
        return default
    return f"MOCKED_{key.upper()}_VALUE"


os.getenv = os_getenv_monkeypatch
# # =========== /MONKEYPATCH OS.GETENV() ===========


# =========== MONKEYPATCH BaseHook.get_connection() ===========
def basehook_get_connection_monkeypatch(key: str, *args, **kwargs):
    print(
        f"Attempted to fetch connection during parse returning an empty Connection object for {key}"
    )
    return Connection(key)
BaseHook.get_connection = basehook_get_connection_monkeypatch
# # =========== /MONKEYPATCH BASEHOOK.GET_CONNECTION() ===========



# =========== MONKEYPATCH VARIABLE.GET() ===========


class magic_dict(dict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __getitem__(self, key):
        return {}.get(key, "MOCKED_KEY_VALUE")


def variable_get_monkeypatch(key: str, default_var=None, deserialize_json=False):
    print(
        f"Attempted to get Variable value during parse, returning a mocked value for {key}"
    )

    if default_var:
        return default_var
    if deserialize_json:
        return magic_dict()
    return "NON_DEFAULT_MOCKED_VARIABLE_VALUE"


Variable.get = variable_get_monkeypatch
# # =========== /MONKEYPATCH VARIABLE.GET() ===========
