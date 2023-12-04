import logging
import typing

from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async

from airflow_provider_tea_pot.hooks import TeaPotHook

import json

logger = logging.getLogger("airflow")




class WaterLevelTrigger(BaseTrigger):

    def __init__(self, tea_pot_conn_id, minimum_level) -> None:
        self.tea_pot_conn_id = tea_pot_conn_id
        self.minimum_level = minimum_level
        pass


    def serialize(self) -> typing.Tuple[str,typing.Dict[str,typing.Any]]:

        return "airflow_provider_tea_pot.triggers.TeaPotTrigger",{
                    "minimum_level" : self.minimum_level,
                    "tea_pot_conn_id" : self.tea_pot_conn_id
                    }


    async def run(self):

        hook = TeaPotHook(tea_pot_conn_id=self.tea_pot_conn_id)
        async_get_water_level = sync_to_async(hook.get_water_level)

        while True:
            rv = await async_get_water_level()
            if json.loads(rv).get('level') > self.minimum_level :
                yield TriggerEvent(rv)
