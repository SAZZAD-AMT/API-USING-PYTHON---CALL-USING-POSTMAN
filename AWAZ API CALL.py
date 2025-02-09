from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

# Define default arguments
default_args = {
    'owner': 'sazzad',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

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
def fetch_removed_products(token, start_date, end_date):
    api_url = "https://walton-amar-awaz-prod.com/api/admin/automaticRemovedProducts"
    headers = {
        "Authorization": f"Bearer {token}"  # Assuming the API uses Bearer token for authentication
    }
    params = {
        "startDate": start_date,
        "endDate": end_date
    }

    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()
        # Log or process the data
        print("API Response (Removed Products):", data)
    except requests.exceptions.RequestException as e:
        print("API request failed (Removed Products):", e)
        raise

# Function to call the second API with the session token
def fetch_removed_low_size_image_products(token, start_date, end_date):
    api_url = "https://walton-amar-awaz-prod.com/api/admin/automaticRemovedLowSizeImageProducts"
    headers = {
        "Authorization": f"Bearer {token}"  # Assuming the API uses Bearer token for authentication
    }
    params = {
        "startDate": start_date,
        "endDate": end_date
    }

    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()
        # Log or process the data
        print("API Response (Removed Low Size Image Products):", data)
    except requests.exceptions.RequestException as e:
        print("API request failed (Removed Low Size Image Products):", e)
        raise

# Define the DAG
with DAG(
    dag_id='AWAZ_API_CALL',
    default_args=default_args,
    description='DAG to login and fetch automatically removed products and low size image products from the API',
    schedule_interval='0 7 * * *',  
    start_date=datetime(2024, 1, 2),  
    catchup=False,
) as dag:
    # Define the PythonOperator for login
    login_task = PythonOperator(
        task_id='login_task',
        python_callable=login_to_api,
    )

    # Define the PythonOperator for fetching removed products data
    fetch_removed_products_task = PythonOperator(
        task_id='fetch_removed_products_task',
        python_callable=fetch_removed_products,
        op_args=[
            '{{ task_instance.xcom_pull(task_ids="login_task") }}',
            '2024-11-27',
            '2024-12-31'
        ],  # Pass the token and date range
    )

    # Define the PythonOperator for fetching removed low size image products data
    fetch_removed_low_size_image_products_task = PythonOperator(
        task_id='fetch_removed_low_size_image_products_task',
        python_callable=fetch_removed_low_size_image_products,
        op_args=[
            '{{ task_instance.xcom_pull(task_ids="login_task") }}',
            '2024-11-27',
            '2024-12-31'
        ],  # Pass the token and date range
    )

    # Set task dependencies
    login_task >> [fetch_removed_products_task, fetch_removed_low_size_image_products_task]
