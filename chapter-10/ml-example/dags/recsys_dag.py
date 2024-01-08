
from datetime import datetime


from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from recsys_dag import  _data_is_new, _fetch_dataset, _generate_data_frames, _load_movie_vectors, _update_internal_hash



from airflow import DAG


PG_VECTOR_BACKEND = 'recsys_pg_vector_backend'
S3_HOOK_CONNECTION = 'minio_connection'


# Preload our connections in from the dag

with open('/usr/local/airflow/include/.kube/config', 'w') as f:
   f.write("""apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lJR25wSlN6bklXZEF3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TkRBeE1EZ3hNek15TkROYUZ3MHpOREF4TURVeE16TTNORE5hTUJVeApFekFSQmdOVkJBTVRDbXQxWW1WeWJtVjBaWE13Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLCkFvSUJBUUREQk4rQkJZNXVmUm5scjI3c0tUYVlSa0tNaHVmdFBqNXhOYzdab3NYb1ZlR25QSnd6bEdjY3VPd2EKbXhFK2ljUld2Z1Zad1hWM1F3QUF2L1k4VFhBUWVRdSt5eG15blNnZHVlTUtxQnpCSS9xeFFVQWlINkhEdVIzQgp3SUdNeWpKZjRXb21MNWNGazZlMFJRN0NBZVBud1NWdjBOalU0VGFoK2k3TldFMUVFYXZ4VDZQbS83VUpIZlQ4CmR0ZmUvUTV0bHRmcHM4LzBvRTVDRmlwOXI5OWVjdWNHNmFjOTF6OGcvejhxY09EVE1lMkNlcVFYaVVDSmVVcWgKS24yVXIrVnFDcDh2K01IUnQvbWhROTdXdUlPS2ZYVjNCOGJPTWptcGl0QVFRM3BkeW5CNGx1akhFSzdCQSt4UwpFSGVTdkJQOFEzWU8yTkhzNm00VjlFZEJXMHZ6QWdNQkFBR2pXVEJYTUE0R0ExVWREd0VCL3dRRUF3SUNwREFQCkJnTlZIUk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJTc3JnbW9KUFIzd1p6TzBzQVBEMWZiSEhndzlqQVYKQmdOVkhSRUVEakFNZ2dwcmRXSmxjbTVsZEdWek1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQm1ydStjVTkxTgpVcXZIMnRKWWx3V0J5ME52VzFPbWlTNFhFaVJLYTNPUGVlb2xvRE9RZVFkZnZ6bmpiV1duUVNjYlBSMmNjQjhOCkxJWHdIOVRRVTQ2V1NLQjlTREZZSC9XMDcyS3BuYzEwR3pNcjZPclNNNHB6NW50OGtqRTllYUNwRU9kTXY2ckwKYTAzNW1nbFpQYXhRWndLRXlkYWsya3R2dzZnMXBhL05HN0RTNXVwV3dVMUllMlkrZms5ZDBFemY3U2s4YXBmVQpNY1lpTXN3MXEwRmFHa3VuNUZGc1pjMC9jUG1MTzkvN3hhenhMT1F4anZYYlJ2dGoxN1BtTDdzMDFZd2piSlJjCnh1WFRUUWJJNFNIeHc2QlhnSXlvVTRoTHVNR2hmRmxOTERUZmZHcDNzMVFEaTBkYnZBV2hWWkJ2YzM4QytmWEEKOGNyOTNZRkNXSWJ0Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    server: https://kubernetes.docker.internal:6443
  name: docker-desktop
contexts:
- context:
    cluster: docker-desktop
    user: docker-desktop
  name: docker-desktop
current-context: docker-desktop
kind: Config
preferences: {}
users:
- name: docker-desktop
  user:
    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURRakNDQWlxZ0F3SUJBZ0lJV014aTI5LzkrUVF3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TkRBeE1EZ3hNek15TkROYUZ3MHlOVEF4TURjeE16TTNORFZhTURZeApGekFWQmdOVkJBb1REbk41YzNSbGJUcHRZWE4wWlhKek1Sc3dHUVlEVlFRREV4SmtiMk5yWlhJdFptOXlMV1JsCmMydDBiM0F3Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLQW9JQkFRQzF5TVV2WDl6NmcxNVkKaElPZW1rK3IrUHhZb252SkpUQjdkN3hLMHRMVFFqY3ZjQkNDNnFzVjZmM2NjWWd6ZFIzVGQ0UC9oNDhqeWZCZApZR0ZUNk9GdGE1RW1uK0FBQ2lzeTl2MzIrdjN6VWNlb2dYSkw5OFdDYXJ0cXBpUzJxQXRMcTVHY0hIbExsbWEzCkd0MVJneXE4NDFJcTVOQndBRmtJenZiSkdwbEpOcmtlQ0RodGVDWHh4Q1RpSGlxOHNzK014YkRsVGIxYXhUaEYKVk8wZFMwMldka0xUYUJHZWN1SUlKRVhma0VRMmZyUm5xV3BIdVdVbUlwaTM4ZmFWUjJlTHlQT2lVaWZ0ZG5SSgozc1lkeWlZaGhUeHJHUWZyQXgrdFU4V1d3RDdrcHhvY1Z2dUxhcFBLZGtDdEJ1dllXcmFwbWZFOEVqVWs4NVZTCkpvODZkUG5wQWdNQkFBR2pkVEJ6TUE0R0ExVWREd0VCL3dRRUF3SUZvREFUQmdOVkhTVUVEREFLQmdnckJnRUYKQlFjREFqQU1CZ05WSFJNQkFmOEVBakFBTUI4R0ExVWRJd1FZTUJhQUZLeXVDYWdrOUhmQm5NN1N3QThQVjlzYwplREQyTUIwR0ExVWRFUVFXTUJTQ0VtUnZZMnRsY2kxbWIzSXRaR1Z6YTNSdmNEQU5CZ2txaGtpRzl3MEJBUXNGCkFBT0NBUUVBb0ZDVWlJSXRoQWpYTm5DdzBOYVVzcGNGWjEyMUxGK1FTVWlHNFk3VWJFendCa1dVUnJDaTYxRjEKSU1KTDFLZUZIT29OU0pFbmNZamNUTXV3dGcxZWJTS0RaeXIyeGdTN2Z0cXBTc1NQcHFOQUUzeGZVTmcxNFl4TQprQUEvOCt1TzBGTUhlc3JDbW4zTWtiZzFhaGFabFRSZWhKbk52aG1kUmRiRGdaZWlkckJjMUVHaXJnV0hwcmR2Cm1XNUhBaDNMSHdMdk45L3dZZ1d1dmtxQTRhL0YwTzQvMGxBdjM5ZWhRN2tUbCtvWW9LelJCUjlZWGZpZGpMMlYKQ09QYVM2UWpPVEhCRG5ta29YZE9zbTV1aS90VTVJU1FCclkvZkFYMmRCUS9rMW1yMFUxZmQrdllFQVBrdkw3TApOQUh0TCtBVjIvQ1lPczkzVUh0ODZ1UDFGQ2RvQ2c9PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==
    client-key-data: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFb3dJQkFBS0NBUUVBdGNqRkwxL2Mrb05lV0lTRG5wcFBxL2o4V0tKN3lTVXdlM2U4U3RMUzAwSTNMM0FRCmd1cXJGZW45M0hHSU0zVWQwM2VELzRlUEk4bndYV0JoVStqaGJXdVJKcC9nQUFvck12Yjk5dnI5ODFISHFJRnkKUy9mRmdtcTdhcVlrdHFnTFM2dVJuQng1UzVabXR4cmRVWU1xdk9OU0t1VFFjQUJaQ003MnlScVpTVGE1SGdnNApiWGdsOGNRazRoNHF2TExQak1XdzVVMjlXc1U0UlZUdEhVdE5sblpDMDJnUm5uTGlDQ1JGMzVCRU5uNjBaNmxxClI3bGxKaUtZdC9IMmxVZG5pOGp6b2xJbjdYWjBTZDdHSGNvbUlZVThheGtINndNZnJWUEZsc0ErNUtjYUhGYjcKaTJxVHluWkFyUWJyMkZxMnFabnhQQkkxSlBPVlVpYVBPblQ1NlFJREFRQUJBb0lCQUJCd3F0eWc5OVEwSTBrdwpHdE15TTNoakdUZE80cEVMYXZvUGx4alJQajNhNURESEdzY3NQL0xJYWF3UkhIZEM1VUtsc1d0ZnNDTlZkUWFNCjIrQ0IxRGxZSEJWNUtnSDI2WGMvZlRKaitxbVJ3TlBOZVMvRHBib291dkJuUERURXQ5ak5HR3NjN0I5WTU4c3gKZzhpR2Ewd1A1dk40SkIycyttQmdqMU1hczdnNk1xdWd5N3duZXBya3RnQ1hFcm8xeUJEMmJJVFI0Y0xSMjNsdwphUDR1anVzYStqQm0xQmJVMWpvZzYxYnV3TVIybzdTWWFsdVlVYVZyOURZQ3lVb3JtckhoSnAyaGovQ0pOQzJlCjRjYnVwL2lYMnNjaUVJbmpsTzRJNVEwNkx5bk16VlVESGd2Uy9pSGE4akJOaGY1K3RkZ0ppekdlOUxpWlpZTWkKTmVwN1BWMENnWUVBNHdrdWxvbDZianZvdmNRNWhneWtueEJaR0hhbUJOREN0Nzd0TTJzTkxGcUVaOHZQYlprSgp5TVdneCtSd1R5Z3E5OEdiZDc1QnJuUFcxVkU5TlIvZW1pT252alQvWjJCTVBXT0Vsd0FLTWtCMC9hZ04xanlMClNleXdnM0s3TTRIQ0lwaUxMUmZ3OHA3Z1JGNnJYd3p2UzdySDM1NEhyUVNLQTliaXZWNy83Z01DZ1lFQXpQbTAKanMxRFJIVWtPclBFUjFaU3hFSHJ1MzVuS3Fwcm5uelNUaFE0MEJjeFZkb1NLWXIyeERkSEFMcms0aURoMVBTbwp0UHpaRHZNRDNUdE1UZFhHbEJIaHNkK0VBb0Jxd1lMVnZWc21mcnN4QXhJaXFWNTRrN3BzOWtvOERlOXhkeHpPCmUxb1RrWCtCUUYyL0dGa0VMSHpuOVE5cUE4NFZRSnBWQWhLeGVxTUNnWUJRdSs0bU9BTWllZ2xjbWcrK25IWloKM2cxZ3hxaG80L2VxTWFuVjlBWitORDRMVHcyWE1xbXBES1lORHkzazZDckhhY0NvUnk3Q0k0MU0xQlBJOVdsSwpOTTJzdE5ueDMrdWNsT3dNYWtZMThuZm56OEFENUFuQUl2dnQ4bm1oYWs0bjBVa0VveFBhb3lhckNXTTFiTHNYCm1mY0RqUVc3di9aNFFRem1QSWNVRXdLQmdEZGY2a2g5NllOUUJqYVpwQmdGbVJ5T0Zvb3pqdGwyNnRZSk1LT0oKVVFQZWtDUjZZbGU0eEF1a1IxbEtKRlYydnF2U1lOVnNUWk45VVhqdDhTSkI4NEREQis4T3pGSUVzVktQN3dCVgo1S051SE1LUW5xNlU2QXc0M0FENWN4bnNxd0diMWFoN1lEZjVjMGlaU3V2ZitJR3dTTlhxa1NCd0IwdkpwZGVHCmgwM3pBb0dCQUxqbVU2K25SSVNXNHFhZmFkMENsZEUrQ3ppOURhd1llZ203OXcvUXQyLytjWXFNczlhNGtLYzgKWERmVjBYWEcyM1M5WnU4TzMwZWdMemFueitDbmhhenViRE9uT1VjSHRNMVpEZ1lDZThoSG1qYzhLcm4xbGVzawpkQ1VNL21tZ3pRU21xbXk5V3FORnNWMTVJQUJmYnJGTlAvYkZ5c0RYTWlQSVVVZWszazlOCi0tLS0tRU5EIFJTQSBQUklWQVRFIEtFWS0tLS0tCg==
""")


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

    enable_vector_extension = PostgresOperator(
        task_id="enable_vector_extension",
        postgres_conn_id=PG_VECTOR_BACKEND,
        sql="CREATE EXTENSION IF NOT EXISTS vector;",
    )

    load_movie_vectors = PythonOperator(
        task_id="load_movie_vectors",
        python_callable = _load_movie_vectors,
        op_kwargs={'pg_connection_id': PG_VECTOR_BACKEND},
    )


    create_temp_table = PostgresOperator(
        task_id='create_temp_table',
        postgres_conn_id = PG_VECTOR_BACKEND,
        sql= 'DROP TABLE IF EXISTS "temp"; CREATE TABLE "temp" AS TABLE "' + "{{ ti.xcom_pull(key='hash_id', task_ids='data_is_new') }}" + '";'
    )

    join_no_op = EmptyOperator(
        task_id="join_no_op"
    )

    swap_prod_table = PostgresOperator(
        task_id='swap_temp_to_prod',
        postgres_conn_id = PG_VECTOR_BACKEND,
        sql= 'DROP TABLE IF EXISTS "movie_vectors"; ALTER TABLE "temp" RENAME TO "movie_vectors";'
    )


    # train_DL_model = KubernetesPodOperator(
    #     namespace="default",
    #     image="model_trainer",
    #     name="airflow-recsys-model-trainer",
    #     task_id="task-one",
    #     in_cluster=False,
    #     env_vars = {
    #         'RUN_HASH' : "{{ ti.xcom_pull(key='hash_id',task_ids='data_is_new')}}",
    #         'RECSYS_DATA_SET_KEY' : "{{ ti.xcom_pull(key='ratings.csv',task_ids='fetch_dataset')}}"
    #     },
    #     cluster_context="docker-desktop",  # is ignored when in_cluster is set to True
    #     config_file='/usr/local/airflow/include/.kube/config',
    # )

    train_DL_model = DockerOperator(
        task_id='train_dl_model',
        image='model_trainer',
        container_name="{{ ti.xcom_pull(key='hash_id',task_ids='data_is_new')}}"+"_model_trainer",
        api_version='auto',
        auto_remove=True,
        environment = {
            'RUN_HASH' : "{{ ti.xcom_pull(key='hash_id',task_ids='data_is_new')}}",
            'RECSYS_DATA_SET_KEY' : "{{ ti.xcom_pull(key='ratings.csv',task_ids='fetch_dataset')}}"
        },
        docker_url="tcp://docker-socket-proxy:2375",
        network_mode="airflow_recsys_default"
        )

    update_internal_hash = PythonOperator(
        task_id = 'update_internal_hash',
        python_callable = _update_internal_hash
    )



data_is_new >> do_nothing
data_is_new >> fetch_dataset >> generate_data_frames

generate_data_frames >> enable_vector_extension >>  load_movie_vectors >> create_temp_table >> join_no_op
generate_data_frames >> train_DL_model >> join_no_op

join_no_op >> swap_prod_table
# join_no_op >> upload_model_artifact

swap_prod_table >> update_internal_hash
