import typing
from airflow.models import BaseOperator

from airflow_provider_tea_pot.triggers import WaterLevelTrigger




class WaterLevelSensor(BaseOperator):

    template_fields = ()

    def __init__(self, tea_pot_conn_id, minimum_level, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tea_pot_conn_id = tea_pot_conn_id
        self.minimum_level = minimum_level


    def execute(self, context) -> typing.Any:

        self.defer(
            trigger=WaterLevelTrigger(tea_pot_conn_id=self.tea_pot_conn_id,
                                      minimum_level=self.minimum_level),
            method_name="execute_complete"
        )

    def execute_complete(self, context, event=None):
        return event
