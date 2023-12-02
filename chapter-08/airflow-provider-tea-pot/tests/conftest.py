import pytest



def pytest_itemcollected(item):
    """
    use test doc strings as messages for the testing suite
    """
    if item._obj.__doc__:
        item._nodeid = f"{item.obj.__doc__.strip().ljust(50,' ')[:50]}{str(item._nodeid).ljust(100,' ')[:50]}"

@pytest.fixture
def fixture_name():
    pass
