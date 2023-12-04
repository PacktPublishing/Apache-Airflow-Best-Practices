from airflow_provider_tea_pot.hooks import TeaPotHook


from airflow.models import Connection
from airflow.utils import db
import json

# init airflow database
db.initdb()

db.merge_conn(
    Connection(
        conn_id="tea_pot_default",
        conn_type="teapot",
        host='tea-pot',
        port='8083',
        extra=dict(pot_designator=0,additions="sugar")
    )
)

def test_this_provider_hook_init():
    """this test will fail until you implement the hook"""

    hook = TeaPotHook()
    conn = hook.get_connection(hook.tea_pot_conn_id)
    assert isinstance(hook,TeaPotHook)
