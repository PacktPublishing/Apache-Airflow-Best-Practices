

from __future__ import annotations
import logging
import typing

from airflow.hooks.base import BaseHook
from airflow.compat.functools import cached_property

logger = logging.getLogger("airflow")

class TeaPotHook(BaseHook):
    """
    A simple hook that allows us to interact with our smart Tea Pot.

    :param sample_conn_id: connection that has the base url for the tea pot
    :type sample_conn_id: str
    """

    conn_name_attr = "tea_pot_conn_id"
    default_conn_name = "tea_pot_default"
    conn_type = "teapot"
    hook_name = "TeaPot"


    def __init__(
        self,
        tea_pot_conn_id: str = default_conn_name,
    ) -> None:
        super().__init__()
        self.tea_pot_conn_id = tea_pot_conn_id
        self.get_conn

    @cached_property
    def get_conn(self) -> typing.Any:
        """get our actual connection object from the database"""
        config = self.get_connection(self.tea_pot_conn_id)
        return config


    @staticmethod
    def get_connection_form_widgets() -> dict[str, typing.Any]:
        """Returns connection widgets to add to connection form"""
        return {
            }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behaviour"""
        return {
            }


    def test_connection(self) -> typing.Tuple[bool, str]:
        """Test a connection"""
        self.get_conn()
        alive = True
        if alive:
            return (True, "Alive")
        return (False, "Not Alive")


    def is_ready(self) -> bool :
        self.get_conn()
        return True

    def make_tea(self) -> str:
        self.get_conn()
        return ''

    def brew_coffee(self) -> str:
        self.get_conn()
        return ''

    def get_water_level(self) -> float:
        self.get_conn()
        return 0.0
