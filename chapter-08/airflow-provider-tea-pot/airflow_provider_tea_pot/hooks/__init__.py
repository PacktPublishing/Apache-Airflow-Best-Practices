

from __future__ import annotations
import logging
import typing
import requests

from airflow.hooks.base import BaseHook
from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException


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


    @staticmethod
    def get_connection_form_widgets() -> dict[str, typing.Any]:
        """Adding two fields to our connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        # We create two new fields for our smart tea pot connection
        # pot_designator - for if we have a smart tea pot with multiple caraffes
        # additions - for descibing anything we'd like to add to our tea
        # these fields will be stored in the extras dictionary of the connection object
        return {
            "pot_designator": StringField(lazy_gettext("Pot Designator"), widget=BS3TextFieldWidget()),
            "additions": StringField(lazy_gettext("Additions"), widget=BS3TextFieldWidget()),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Updates our connection field  form behaviors"""
        return {
            "hidden_fields": ["password", "login", "schema", "extra"], # we hide these fields
            "placeholders": { # we provide examples for these fields
                "pot_designator": "1",
                "additions": "sugar",
                "host": "tea-pot",
                "port": "8083",
            },
            "relabeling" : {} # and we don't relabel anything (but we do need to explicitly declare that)
        }

    @cached_property
    def get_conn(self) -> typing.Any:
        """build our url and set any additional information for our connection"""
        conn = self.get_connection(self.tea_pot_conn_id)
        self.url = f"{conn.host}:{conn.port}"
        self.pot_designator = conn.extra_dejson.get("pot_designator",None)
        self.additions = conn.extra_dejson.get("additions",None)
        return

    def test_connection(self) -> typing.Tuple[bool, str]:
        """Test a connection"""
        if self.is_ready():
            return (True, "Alive")
        return (False, "Not Alive")


    def is_ready(self) -> bool :
        self.get_conn
        response = requests.get(f"http://{self.url}/ready")
        if response.status_code == 200:
            return True
        return False

    def make_tea(self) -> str:
        self.get_conn
        response = requests.get(f"http://{self.url}/ready")
        if response.status_code == 200: 
            return response.text
        raise AirflowException(f"{response.status_code} : {response.reason}")

    def brew_coffee(self) -> typing.Tuple(int,str):
        self.get_conn
        response = requests.get(f"http://{self.url}/ready")
        if response.status_code == 200: 
            return response.text
        raise AirflowException(f"{response.status_code} : {response.reason}")

    def get_water_level(self) -> typing.Tuple(int,str):
        self.get_conn
        response = requests.get(f"http://{self.url}/ready")
        if response.status_code == 200: 
            return response.text
        raise AirflowException(f"{response.status_code} : {response.reason}")
