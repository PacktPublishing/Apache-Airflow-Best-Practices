
from datetime import datetime


from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from recsys_dag import _data_is_new, _fetch_dataset, _generate_data_frames, _create_knn_vector_table, _swap_knn_vector_table, _swap_knn_vector_table, _upload_model_artifact



from airflow import DAG


with DAG(
    "movie_rec_sys", start_date=datetime(2023, 1, 1), schedule_interval="@weekly", catchup=False
) as dag:


    data_is_new = BranchPythonOperator(
        task_id = "data_is_new",
        python_callable=_data_is_new
    )

    do_nothing = EmptyOperator(
        task_id = "do_nothing"
    )

    fetch_dataset = PythonOperator(
        task_id = "fetch_dataset",
        python_callable = _fetch_dataset
    )


    generate_data_frames = PythonOperator(
        task_id = "generate_data_frames",
        python_callable = _generate_data_frames
    )

    create_knn_vector_table = PythonOperator(

    )

    train_deep_learning_model = PythonOperator(

    )

    join_no_op = EmptyOperator(
        task_id="join_no_op"
    )

    swap_knn_vector_table = PythonOperator(

    )

    upload_model_artifact = PythonOperator{

    }

data_is_new >> do_nothing
data_is_new >> fetch_dataset >> generate_data_sets

generate_data_sets >> create_knn_vector_table >> join_no_op
generate_data_sets >> train_deep_learning_model >> join_no_op

join_no_op >> swap_knn_vector_table
join_no_op >> upload_model_artifact

