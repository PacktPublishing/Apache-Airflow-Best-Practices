from dags.example_dag import _choosing_best_model, _train_model


def test__train_model():
    """test _training_model returns"""
    assert isinstance(_train_model('this') , int)

def test__choosing_best_model():
    """test _choosing_best_model returns"""

    # we could do some fancy stuff with mocking libraries here but for now it's sufficient to just write a class that mimics
    # the method of airflow.models.taskinstance.TaskInstance that the function is using (i.e. xcom_pull)

    class MockTi:
        def __init__(self,call_rv):
            self.rv = call_rv

        def xcom_pull(self,*args,**kwargs):
            return self.rv
    assert _choosing_best_model(MockTi([0,0,0])) == "inaccurate"
    assert _choosing_best_model(MockTi([0,0,9])) == "accurate"
    assert _choosing_best_model(MockTi([0,9,9])) == "accurate"
