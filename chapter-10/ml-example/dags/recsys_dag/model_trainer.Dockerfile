FROM python:3.11

WORKDIR /usr/src/app

RUN pip install --no-cache-dir numpy polars keras tensorflow scikit-learn boto3 botocore

COPY dags/recsys_dag/model_trainer.py .

CMD [ "python", "./model_trainer.py" ]
