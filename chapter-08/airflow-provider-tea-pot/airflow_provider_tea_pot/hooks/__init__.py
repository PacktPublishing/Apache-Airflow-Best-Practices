






from __future__ import annotations
import logging
import typing

from airflow.hooks.base import BaseHook

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

    def get_conn(self) -> typing.Any:
        """
        A method for getting a connection to an external service given your connection information.
        """
        conn = self.get_connection(self.sample_conn_id)
        return conn

    # @staticmethod
    # def get_connection_form_widgets() -> dict[str, typing.Any]:
    #     """Returns connection widgets to add to connection form"""
    #     return {
    #         }

    # @staticmethod
    # def get_ui_field_behaviour() -> dict:
    #     """Returns custom field behaviour"""
    #     return {
    #         }


    def test_connection(self) -> typing.Tuple[bool, str]:
        """Test a connection"""
        x = self.get_conn()
        alive = True 
        if alive:
            return [True, "Alive"]
        return [False, "Not Alive"]
