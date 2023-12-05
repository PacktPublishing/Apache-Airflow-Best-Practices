from airflow_provider_tea_pot.triggers import WaterLevelTrigger
from airflow_provider_tea_pot.hooks import TeaPotHook
import pytest
import asyncio
import json

def test_water_level_trigger():

    trigger = WaterLevelTrigger(
        tea_pot_conn_id = "tea_pot_other",
        minimum_level = 0.25
    )

    assert isinstance(trigger,WaterLevelTrigger)

    classpath, kwargs = trigger.serialize()

    assert classpath == "airflow_provider_tea_pot.triggers.WaterLevelTrigger"
    assert kwargs == dict(tea_pot_conn_id = "tea_pot_other",
        minimum_level = 0.25)

@pytest.mark.asyncio
async def test_trigger_run_good(mocker):

    trigger = WaterLevelTrigger(
        tea_pot_conn_id = "tea_pot_other",
        minimum_level = 0.25
    )

    rv = json.dumps(dict(level=0.7))
    mocker.patch.object(TeaPotHook,"get_water_level",return_value=rv)

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(1.0)
    assert task.done() is True
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
async def test_trigger_run_bad(mocker):

    trigger2 = WaterLevelTrigger(
        tea_pot_conn_id = "tea_pot_other",
        minimum_level = 0.25
    )

    rv = json.dumps(dict(level=0.1))
    mocker.patch.object(TeaPotHook,"get_water_level",return_value=rv)

    task2 = asyncio.create_task(trigger2.run().__anext__())
    await asyncio.sleep(1.0)
    assert task2.done() is False
    asyncio.get_event_loop().stop()
