import logging
import typing


from airflow.models import BaseOperator

from airflow_provider_tea_pot.hooks import *
from airflow_provider_tea_pot.triggers import *



logger = logging.getLogger("airflow")




class TeaPotOperator(BaseOperator):

    template_fields = ()

    def __init__(self, **kwargs) -> None:

        super().__init__(**kwargs)

        # This is where you set all the attributes you need to for the operator to function
        raise NotImplementedError("You need to implement an __init__ method for this class")


    def execute(self,context) -> typing.Any:

        # This is where you write the python code that gets executed on a schedule
        raise NotImplementedError("You need to implement an execute method for this class")
