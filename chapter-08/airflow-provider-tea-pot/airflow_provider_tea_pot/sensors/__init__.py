import typing
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue


def method_for_checking_state():
    """A method for checking on something when we poke"""
    pass

class TeaPotSensor(BaseSensorOperator):

    template_fields = ()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        raise NotImplementedError("You need to implement an __init__ method for this class")

    def poke(self, context) -> typing.Union[PokeReturnValue, bool]:

        raise NotImplementedError("You need to implement a poke method for this class")
        return_value = method_for_checking_state()
        return PokeReturnValue(bool(return_value))
