import logging
import typing


from airflow.models import BaseOperator

from airflow_provider_tea_pot.hooks import TeaPotHook



logger = logging.getLogger("airflow")




class MakeTeaOperator(BaseOperator):

    template_fields = ()

    def __init__(self, tea_pot_conn_id, additions = None, pot_designator = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tea_pot_conn_id = tea_pot_conn_id
        self.pot_designator = pot_designator
        self.additions = additions



    def execute(self,context) -> typing.Any:
        self.hook = TeaPotHook(tea_pot_conn_id=self.tea_pot_conn_id)

        if self.pot_designator:
            self.hook.pot_designator = self.pot_designator
        if self.additions :
            self.hook.additions = self.additions
        return self.hook.make_tea()


class BrewCoffeeOperator(BaseOperator):

    template_fields = ()

    def __init__(self, tea_pot_conn_id, additions = None, pot_designator = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tea_pot_conn_id = tea_pot_conn_id
        self.pot_designator = pot_designator
        self.additions = additions



    def execute(self,context) -> typing.Any:
        self.hook = TeaPotHook(tea_pot_conn_id=self.tea_pot_conn_id)

        if self.pot_designator:
            self.hook.pot_designator = self.pot_designator
        if self.additions :
            self.hook.additions = self.additions
        return self.hook.brew_coffee()
