from airflow_provider_tea_pot.sensors import WaterLevelSensor
from airflow.exceptions import TaskDeferred
import pytest


def test_water_level_defers():
    """ Test our sensor immediately enteres a deferred state on execute"""
    sensor = WaterLevelSensor(
        tea_pot_conn_id = "tea_pot_other",
        minimum_level = 0.7,
        task_id = "test"
    )

    with pytest.raises(TaskDeferred):
        sensor.execute(context={})



def test_water_level_execute_complete():
    """ Test execute_complete method works as intended """
    sensor = WaterLevelSensor(
        tea_pot_conn_id = "tea_pot_other",
        minimum_level = 0.7,
        task_id = "test"
    )

    assert "test" == sensor.execute_complete(context={}, event="test")
