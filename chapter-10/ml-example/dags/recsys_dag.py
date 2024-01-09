
from datetime import datetime


from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from recsys_dag import  _data_is_new, _fetch_dataset, _generate_data_frames, _load_movie_vectors, _update_internal_hash, __get_recsys_bucket



from airflow import DAG


PG_VECTOR_BACKEND = 'recsys_pg_vector_backend'
S3_HOOK_CONNECTION = 'minio_connection'



# If you use the KubernetesPodOperator in this example, you can get the config file in through the following steps:
# 1. Install Docker Desktop, and under settings enable kubernetes
# 2. Get your kubernetes config file via `kubectl config view --minify --raw `
# 3. Update the server section from `https://127.0.0.1:6443` to `https://kubernetes.docker.internal:6443`
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
    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURRakNDQWlxZ0F3SUJBZ0lJYU11WXh4MTh6QTh3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TkRBeE1EZ3hNek15TkROYUZ3MHlOVEF4TURjeE9EUTNORGRhTURZeApGekFWQmdOVkJBb1REbk41YzNSbGJUcHRZWE4wWlhKek1Sc3dHUVlEVlFRREV4SmtiMk5yWlhJdFptOXlMV1JsCmMydDBiM0F3Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLQW9JQkFRQy9xMkhHbFBPblBiLzUKMFZTT0wzOW41NGlMWjRoemErQlBXNXRVQjZlQy9VeFNnR2hVVGFOYzNabWRoY3NCMWtUVmZKb2M1V1V5a0ZiSAozdkZUNzZmS3VTVEJzRFl1dnhvYk9RMTMzblFmVnhRZ3ZmOTRPTVNmUW9NNGw3Rnpnay9tSGpWQWllMVVScUdBCm9DVFBzeDlmT3FFMG4yaXpKUTJISWVjWHRIVEFBcVFpSXkwUVhXS01tVDdVc1pxekdaeExtRHlTQ0twRmloUVUKRE82T09DWjl3ZTQyajlHSks3RjdHRzNodVdYZkRJSHVqczUwaHhFZGtXODRpR0phRGlhTW1DcUxlM3ZTRUxBQgpHTld6NUwrSlhPcDF3d1BwZnpIQ1FTWGwwS2d3UVgxN2l5dHJqa3ZZZGIzWXh2bUpnS01QcHc0NklrMlJwaTJFCkZPOHBlbmoxQWdNQkFBR2pkVEJ6TUE0R0ExVWREd0VCL3dRRUF3SUZvREFUQmdOVkhTVUVEREFLQmdnckJnRUYKQlFjREFqQU1CZ05WSFJNQkFmOEVBakFBTUI4R0ExVWRJd1FZTUJhQUZLeXVDYWdrOUhmQm5NN1N3QThQVjlzYwplREQyTUIwR0ExVWRFUVFXTUJTQ0VtUnZZMnRsY2kxbWIzSXRaR1Z6YTNSdmNEQU5CZ2txaGtpRzl3MEJBUXNGCkFBT0NBUUVBT1lCRjVJYlNCNGxRcEY3azVITzhPc3pUUHZuNitxZHFlcFVaNjh6SGhYOEk0aHJxUXp4Q29UbWkKQ0RpSUliK1B2d3VXcDg2Mi9qMHh3dWpmNUZrZHR5UzNHUktBclA2aExHaUFXRlJ2L1Z3WVQyZitZN3FkQlk1egowOExuYU5pSENNRGI5SlJnUitpQndjYUxRa1NWSUdHcWN1bldSeXhMcTE2YnJYWVBSNkEzUHBwSVh6clRoTVZuCk5mdU0zOGt3RXRJdWlWRWM2L0pGaUVvR09ZeDRJUDJoYXZlYnR6YzJnWVBzRUNUQ2JnWlU3WVp4Sy9vWEFKTnkKdWZyT1VtRlN5Zm9qaS94TERHNExjQUdRSTV4WlJ1MEZ5ZGtOTTJrNnJPdTVCV2tYcUZTT3MvVjB4Z04zZTNpTApVRlRja0Y2RkM0eGgzOFZHYkFVTzFWS29CbjdTbUE9PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==
    client-key-data: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFcEFJQkFBS0NBUUVBdjZ0aHhwVHpwejIvK2RGVWppOS9aK2VJaTJlSWMydmdUMXViVkFlbmd2MU1Vb0JvClZFMmpYTjJabllYTEFkWkUxWHlhSE9WbE1wQld4OTd4VSsrbnlya2t3YkEyTHI4YUd6a05kOTUwSDFjVUlMMy8KZURqRW4wS0RPSmV4YzRKUDVoNDFRSW50VkVhaGdLQWt6N01mWHpxaE5KOW9zeVVOaHlIbkY3UjB3QUtrSWlNdApFRjFpakprKzFMR2FzeG1jUzVnOGtnaXFSWW9VRkF6dWpqZ21mY0h1Tm8vUmlTdXhleGh0NGJsbDN3eUI3bzdPCmRJY1JIWkZ2T0loaVdnNG1qSmdxaTN0NzBoQ3dBUmpWcytTL2lWenFkY01ENlg4eHdrRWw1ZENvTUVGOWU0c3IKYTQ1TDJIVzkyTWI1aVlDakQ2Y09PaUpOa2FZdGhCVHZLWHA0OVFJREFRQUJBb0lCQUJaQmxsYnkwT0Fvb1hRbQp4SVRHS3lzcmpZVnlmanU0MlJFZCtBbG40aUdwdkFVUWVBemgvd2czaXpEcGFrdEtVQy9pQ2hzMDMrMWhLVktJCkpzbXlXRytIMUNiUmlWa2dRNjNKa0RETkdmclJwN3F0bFU2bWVONXRZTU5maWZGNXZrRmdYSnFqNlhVUnhvc2oKRFdNNHozbkZXeGE3TS9NcGFFcTZ1eFRibFhyK0lBb3U0UHZpNTY1SEVleTNtL1JmWlI0Q3ZnWVlNQzRGVnNVbApZTldKYjNDN3ZDMnlib3doV1Z5UVBENTJDSUI0OVkxSndVcVRqM2tkRmNNelpyNXhQai9odWFkTUx3ZTlGY2dHClRsNWJkY1BaODZrOEREOVkwS1ltRENSZnBwRE1oZEM1MjR6QmtkeDJ1U3I4bEptVVpUR2VrQmZKeSt1SGtFNXIKWjUxdHRua0NnWUVBNWZsR1FXeXhQN3pOQnpPS1ZncmV0SFVBYUMxRTFIazFDSCttNjVDaElmMU9Kc1JhVldCNQpENEFZeEp6MUVtcHRxUjB1blpNcEgyMk4zWnZjK2VBZ2tWcXZvMFdrQWxjZTJSN3Bkem93N0ZLL1F3UUFScXNWCkV0ZXh2L2tpaGxrZyt0VzFvUk5RNzV2UzNMZmx3S1F6SHdkci9XTmo3OTJoZWhnQitNM1FaSjhDZ1lFQTFWeGUKVStXK3VFU1NBMjF1U3d5aU1vbjNUSFhIbC9YK29aajM4VVBqYWlmd2pYaFBsTlR3RTQxSFdyckVPZkNBTzloMgpwSmN5SkttODluUjIzVWVKTFl2TGkrRjNMNXdLNGpkcysvU1BzUTd1SXlIWGk5QTkyY21SejNqM085elBKZlljCkxNNFRqd2cwbnJ1aHlvMGg5YXJhdXd1UDRxdkNvelZSWlp4cEJlc0NnWUVBb3duTVV3bTV4NVFVVHV5dFFEN0kKa0dyNmRPRFRFVWMwMytlUDhTVkI5eFRiMlFRUGZzTzhUODdpd0ZEK05hWWFSSENaNGNDNHdMMHNmRlhKbmFUVgpzZmIyMmV1L1VLRnZEMEtwQll2TnIxMGlsMkk0eCtEMk9idU5HcXFIWFhQR3U4M1N6SFVqUmh2VXJBSDc4a3oyCnFTTGIzbkllSWFtZ2x5eDZDV2Z3TzhVQ2dZRUFscXNMMGFORDgwdzQ4RUt1eUVDN3FZVFVKaUptT0dGMjF4YjIKd0dGNGp0WjFnUEdkQVRUOTlGQ29PdUg1QUJGZC9PVDNvM05CN2JJUHh0cW50Y3QyaTd0VW1nczE1MkVDTG8yRgpZTWRyVVZXQnhUMTR3VHJrOG83dGNOMnplWXFNbmZvV0cyM0xVZzR4V29hVjBqdW41NXducWo2WDlUSGwyT3NsCjkrTmtKdEVDZ1lCdUoxNXBDRU52UmgrL2VJUHpTdzQvVTlVeWxwa2Y5emhPQ00zbEp4emphSjhRUDJWUU5RajYKcnhKSTFlOEFFTTNtbFFRRy9vSitiNnkrTzVyOVNuQWxxWW1nV0dXMXR5QzVxaVlFVFU4Wnd5Q1l4eXQ3QTIzWAprcjRjWEMvaExLTVlWMHBTazJlcjN2NHVBa05CVFlYd25NaEpvcDV0UWcyQ0prcWNPSjFzZnc9PQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=

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


    train_DL_model = KubernetesPodOperator(
        namespace="default",
        image="model_trainer",
        name="airflow-recsys-model-trainer",
        task_id="train_dl_model",
        do_xcom_push = True,
        in_cluster=False,
        image_pull_policy='Never',        # This is a hack for Docker Desktop local images
        env_vars = {
            'RUN_HASH' : "{{ ti.xcom_pull(key='hash_id',task_ids='data_is_new')}}",
            'RECSYS_DATA_SET_KEY' : "{{ ti.xcom_pull(key='ratings.csv',task_ids='fetch_dataset')}}"
        },
        cluster_context="docker-desktop",  # is ignored when in_cluster is set to True
        config_file='/usr/local/airflow/include/.kube/config',
    )

    # train_DL_model = DockerOperator(
    #     task_id='train_dl_model',
    #     image='model_trainer',
    #     container_name="{{ ti.xcom_pull(key='hash_id',task_ids='data_is_new')}}"+"_model_trainer",
    #     api_version='auto',
    #     auto_remove=True,
    #     environment = {
    #         'RUN_HASH' : "{{ ti.xcom_pull(key='hash_id',task_ids='data_is_new')}}",
    #         'RECSYS_DATA_SET_KEY' : "{{ ti.xcom_pull(key='ratings.csv',task_ids='fetch_dataset')}}"
    #         },
    #     docker_url="tcp://docker-socket-proxy:2375",
    #     network_mode="airflow_recsys_default"
    #     )
    	

    promote_model = S3CopyObjectOperator(
        task_id = "promote_dl_model",
        aws_conn_id = 'recsys_s3' ,
        source_bucket_key = "{{ ti.xcom_pull(key='return_value',task_ids='train_dl_model')}}" ,
        dest_bucket_key = "latest_model",
        source_bucket_name = __get_recsys_bucket(),
        dest_bucket_name = __get_recsys_bucket(),
    )

    update_internal_hash = PythonOperator(
        task_id = 'update_internal_hash',
        python_callable = _update_internal_hash
    )



data_is_new >> do_nothing
data_is_new >> fetch_dataset >> generate_data_frames

generate_data_frames >> enable_vector_extension >>  load_movie_vectors >> create_temp_table >> join_no_op 
generate_data_frames >> train_DL_model >> join_no_op

join_no_op >> swap_prod_table >> update_internal_hash
join_no_op >> promote_model >> update_internal_hash