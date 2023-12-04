from airflow_provider_tea_pot.operators import MakeTeaOperator, BrewCoffeeOperator
from mock import patch


def test_make_tea_operator():
    """ test MakeTeaOperator """
    with patch("requests.get") as mocked_requests_get:
        mocked_requests_get.return_value.status_code = 200
        mocked_requests_get.return_value.text = "successful"
        operator = MakeTeaOperator(tea_pot_conn_id="tea_pot_other", task_id="test",additions="lemon",pot_designator="3")
        x = operator.execute(context={})

        assert operator.additions == "lemon"
        assert operator.pot_designator == "3"
        assert x == "successful"


def test_brew_coffee_operator():
    """ test BrewCoffeeOperator """
    with patch("requests.get") as mocked_requests_get:
        mocked_requests_get.return_value.status_code = 200
        mocked_requests_get.return_value.text = "successful"
        operator = BrewCoffeeOperator(tea_pot_conn_id="tea_pot_other", task_id="test",additions="lemon",pot_designator="3")
        x = operator.execute(context={})

        assert operator.additions == "lemon"
        assert operator.pot_designator == "3"
        assert x == "successful"
