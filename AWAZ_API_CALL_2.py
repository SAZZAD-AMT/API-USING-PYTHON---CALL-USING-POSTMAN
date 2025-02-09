import datetime
import json
import os
import sys
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.oracle.hooks.oracle  import OracleHook
from kubernetes.client import models as k8s
from airflow.settings import AIRFLOW_HOME
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from airflow.utils.dates import days_ago
from smart_open import open


# Define default arguments
default_args = {
    'owner': 'sazzad',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='AWAZ_API_CALL',
    schedule_interval='0 7 * * *',  
    start_date=datetime(2024, 1, 2),  
    catchup=False,
    )

# Step 3 - Declare dummy start and stop tasks
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)


spark_conf = (SparkConf()
       .set("spark.driver.bindAddress", "0.0.0.0")\
       .set("spark.submit.deployMode", "client")\
       .set("spark.kubernetes.driver.pod.name", os.getenv("MY_POD_NAME"))\
       .set("spark.kubernetes.authenticate.subdmission.caCertFile", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")\
       .set("spark.kubernetes.authenticate.submission.oauthTokenFile", "/var/run/secrets/kubernetes.io/serviceaccount/token")\
       .set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-driver")\
       .set("spark.kubernetes.namespace", os.getenv("MY_POD_NAMESPACE"))\
       .set("spark.executor.instances", "1")\
       .set("spark.dynamicAllocation.maxExecutors", "2")\
       .set("spark.dynamicAllocation.enabled", "true")\
       .set("spark.dynamicAllocation.shuffleTracking.enabled", "true")\
       .set("spark.driver.host", "airflow-spark-driver.analytics")\
       .set("spark.driver.port", "20020")\
       .set("spark.driver.memory", "10G")\
       .set("spark.port.maxRetries", "0")\
       .set("spark.kubernetes.executor.request.cores", "1")\
       .set("spark.kubernetes.executor.limit.cores", "2")\
       .set("spark.executor.memory", "18G")\
       .set("spark.kubernetes.container.image", "quay.io/klovercloud/apache.spark:3.2.1")\
       .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
       .set("spark.kubernetes.container.image.pullPolicy", "IfNotPresent")\
       .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
       .set("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")\
       .set("spark.sql.catalog.spark_catalog.type","hive")\
       .set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")\
       .set("spark.hadoop.hive.metastore.uris","thrift://hive-metastore-prod-2.analytics:9083")\
       .set("spark.hadoop.hive.metastore.warehouse.dir","s3a://bigdata-prod-2-uavjvefd/warehouse/")\
       .set("spark.hadoop.fs.s3a.experimental.input.fadvise","sequential")\
       .set("spark.hadoop.fs.s3a.connection.ssl.enabled","true")\
       .set("spark.hadoop.fs.s3a.path.style.access","true")\
       .set("spark.hadoop.fs.s3a.endpoint","https://minio.waltonbd.com")\
       .set("spark.hadoop.fs.s3a.access.key","46ewexkQoHtQrtOVbYOi")\
       .set("spark.hadoop.fs.s3a.secret.key","JvEimtHqsK22z1dFKLij5zBKjG4MdtqTv0bXuwog")\
       .set("spark.hadoop.fs.s3a.attempts.maximum","1")\
       .set("spark.hadoop.fs.s3a.connection.establish.timeout","500")\
       .set("spark.sql.parquet.int96RebaseModeInRead","CORRECTED")\
       .set("spark.sql.parquet.int96RebaseModeInWrite","CORRECTED")\
       .set("spark.sql.parquet.datetimeRebaseModeInRead","CORRECTED")\
       .set("spark.hadoop.datanucleus.autoCreateSchema","true")\
       .set("spark.sql.parquet.enableVectorizedReader","true")\
       .set("spark.hadoop.fs.s3a.experimental.input.fadvise","sequential")\
       .set("spark.sql.broadcastTimeout", "3000")\
       .set("spark.hadoop.datanucleus.fixedDatastore","false"));

# Function to login and get the session token
def login_to_api():
    login_url = "https://walton-amar-awaz-prod.com/api/admin/login"
    username = "Ramisa-SBD"  # Replace with your username
    password = "Ramisa-SBD"  # Replace with your password

    try:
        # Sending login request
        response = requests.post(login_url, json={"username": username, "password": password}, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes

        # Assuming the response contains a token
        data = response.json()
        token = data.get("token")  # Adjust this depending on the API response structure
        if not token:
            raise ValueError("Login failed, no token received.")
        
        print("Login successful, token:", token)
        return token
    except requests.exceptions.RequestException as e:
        print("Login request failed:", e)
        raise

# Function to call the first API with the session token
def fetch_removed_products(token, startDate, endDate):
    
    api_url = "https://walton-amar-awaz-prod.com/api/admin/automaticRemovedProducts"
    headers = {
        "Authorization": f"Bearer: {token}"  # Assuming the API uses Bearer token for authentication
    }
    params = {
        "startDate": startDate,
        "endDate": endDate
    }

    try:
        response = requests.post(api_url, headers=headers, params=params, timeout=10)
        result = response.json()
        print(f"Automatic Remove Product{result}")
        print("Request URL:", response.url)  
        print("Request Headers:", headers)  
        print("Request Params:", params)    
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()
        # Log or process the data
        print("API Response (Removed Without Warrenty Card Products):", data)
    except requests.exceptions.RequestException as e:
        print(f"Error in fetch_removed_products: {e}")
        return None

# Function to call the second API with the session token
def fetch_removed_low_size_image_products(token, startDate, endDate):

    api_url = "https://walton-amar-awaz-prod.com/api/admin/automaticRemovedLowSizeImageProducts"
    headers = {
        "Authorization": f"Bearer: {token}"  # Assuming the API uses Bearer token for authentication
    }
    params = {
        "startDate": startDate,
        "endDate": endDate
    }
    try:
        response = requests.post(api_url, headers=headers, params=params, timeout=10)
        result = response.json()
        print(f"Automatic Remove Product {result}")
        response.raise_for_status()  
        data = response.json()
        # Log or process the data
        print("API Response (Removed Low Size Image Products):", data)
    except requests.exceptions.RequestException as e:
        print(f"Error in fetch_removed_low_size_image_products: {e}")
        return None



token = login_to_api()

fetch_removed_products = PythonOperator(
        task_id="fetch_removed_products",
        provide_context=True,
        python_callable=fetch_removed_products, 
        op_kwargs={
            "start_date": "2024-12-20",
            "end_date": "2025-01-02",
        },
        dag=dag,
        executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(labels={"app": "airflow-spark-driver"}),
                spec=k8s.V1PodSpec(
                    service_account_name="spark-driver",
                    containers=[
                        k8s.V1Container(
                            name="base",
                            ports=[
                                k8s.V1ContainerPort(
                                    container_port=20020,
                                    name="spark-driver",
                                ),
                            ],
                            resources = k8s.V1ResourceRequirements(
                                requests= {
                                    "cpu": "1",
                                    "memory": "4Gi"
                                },
                                limits = {
                                    "cpu": "6",
                                    "memory": "18Gi"
                                }
                            ),
                        ),
                    ],
                )
            ),
        },
    )

fetch_removed_low_size_image_products = PythonOperator(
        task_id="fetch_removed_low_size_image_products",
        provide_context=True,
        python_callable=fetch_removed_low_size_image_products, 
        op_kwargs={
            "start_date": "2024-12-20",
            "end_date": "2025-01-02",
        },
        dag=dag,
        executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(labels={"app": "airflow-spark-driver"}),
                spec=k8s.V1PodSpec(
                    service_account_name="spark-driver",
                    containers=[
                        k8s.V1Container(
                            name="base",
                            ports=[
                                k8s.V1ContainerPort(
                                    container_port=20020,
                                    name="spark-driver",
                                ),
                            ],
                            resources = k8s.V1ResourceRequirements(
                                requests= {
                                    "cpu": "1",
                                    "memory": "4Gi"
                                },
                                limits = {
                                    "cpu": "6",
                                    "memory": "18Gi"
                                }
                            ),
                        ),
                    ],
                )
            ),
        },
    )

start_task >> fetch_removed_products >> fetch_removed_low_size_image_products >> end_task