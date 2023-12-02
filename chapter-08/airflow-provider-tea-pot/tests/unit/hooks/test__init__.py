from airflow_provider_tea_pot.hooks import TeaPotHook



def test_this_provider_hook_init():
    """this test will fail until you implement the hook"""

    hook = TeaPotHook()
    assert isinstance(hook,TeaPotHook)
