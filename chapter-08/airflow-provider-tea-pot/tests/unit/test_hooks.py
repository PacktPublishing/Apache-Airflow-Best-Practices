from airflow_provider_tea_pot.hooks import TeaPotHook

from mock import patch
import pytest
from airflow.exceptions import AirflowException


def test_this_provider_hook_init():
    """ test initialization """
    hook = TeaPotHook()
    assert isinstance(hook,TeaPotHook)

    hook = TeaPotHook(tea_pot_conn_id="tea_pot_other")
    assert isinstance(hook,TeaPotHook)

def test_this_provider_hook_get_conn():
    """ test get_conn method"""
    hook = TeaPotHook()
    hook.get_conn
    assert hook.additions == "sugar"


    hook = TeaPotHook(tea_pot_conn_id="tea_pot_other")
    hook.get_conn
    assert hook.additions == "milk"

def test_is_ready():
    """ test is ready method """

    hook = TeaPotHook(tea_pot_conn_id="tea_pot_other")

    with patch("requests.get") as mocked_requests_get:
        mocked_requests_get.return_value.status_code = 500
        assert hook.is_ready() is False

        mocked_requests_get.return_value.status_code = 200
        assert hook.is_ready() is True


def test_make_tea():
    """ test make_tea method """

    hook = TeaPotHook(tea_pot_conn_id="tea_pot_other")

    with patch("requests.get") as mocked_requests_get:
        mocked_requests_get.return_value.status_code = 200
        mocked_requests_get.return_value.text = '{ "message": "success" }'
        assert hook.make_tea() == "successful"

        with pytest.raises(AirflowException):
            mocked_requests_get.return_value.status_code = 400
            mocked_requests_get.return_value.reason = "undefined"
            hook.make_tea()

def test_brew_coffee():
    """ test brew_coffee method """

    hook = TeaPotHook(tea_pot_conn_id="tea_pot_other")

    with patch("requests.get") as mocked_requests_get:
        mocked_requests_get.return_value.status_code = 200
        mocked_requests_get.return_value.text = '{ "message": "I am a teapot." }'
        assert hook.brew_coffee() == "successful"

        with pytest.raises(AirflowException):
            mocked_requests_get.return_value.status_code = 400
            mocked_requests_get.return_value.reason = "undefined"
            hook.brew_coffee()

def test_get_water_level():
    """ test get_water_level method """

    hook = TeaPotHook(tea_pot_conn_id="tea_pot_other")

    with patch("requests.get") as mocked_requests_get:
        mocked_requests_get.return_value.status_code = 200
        mocked_requests_get.return_value.text = '{ "level": 0.31 }'
        assert hook.get_water_level() == "successful"

        with pytest.raises(AirflowException):
            mocked_requests_get.return_value.status_code = 400
            mocked_requests_get.return_value.reason = "undefined"
            hook.get_water_level()
