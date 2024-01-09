# Machine Learning System Example Code
--- 

This repo is meant to be a companion to Apache Airflow Best Practices chapter. 


### Quickstart

1. `angreal demo start`
2. naviate to localhost:8080


### Repo Layout

```
.
├── dags           # Where DAGs are stored
│   └── recsys_dag # The recsys_dag module and supporting assets
├── data           # Notebook / Object storage location
├── dev            # localized docker image and docker compose stack
├── notebooks      # Exploratory notebooks for the "data science" work
└── tests          # Unit and functional tests
2  [error opening dir]
```


### Notes about this repo

The described dag has two ways of running the model training task: 
    - DockerOperator
    - KubernetesPodOperator

If you're using the KPO you need to provide a config file for the cluster you'll
be running against. For demo purposes the following instructions work with Docker Desktop locally.

1. Enable Kubernetes in Docker Desktop
2. Get your k8s config via `kubectl config view --minify --raw`
3. Update server section to `https://kubernetes.docker.internal:6443`
4. Add to the dag definition file where indicated. 

> THIS IS NOT A SECURE METHOD OF CONFIGURING OR GETTING A KUBECONFIG FILE INTO YOUR AIRFLOW INSTANCE, PLEASE USE ALTERNATIVE METHODS OF MOUNTING THIS INFORMATION IN PRODUCTION SCENARIOS.


We utilize minio as a substitute for S3 object storage to hopefully make everyones life a little easier. Because this is running in the docker-compose stack, when we're running a KPO within the local cluster - we have to do some additional work to resolve locally.

First, we bind port 9000 in our docker-compose file to the localhost. Next within the model training script,  we utilize the special DNS 
`http://host.docker.internal:9000` to resolve our minio instance bound to local host.