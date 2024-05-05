import psycopg2
import requests
import re
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def extract_data():
    response = requests.get('https://raw.githubusercontent.com/Hipo/university-domains-list/master/world_universities_and_domains.json')
    return response.json()

def search_type(name):
    if re.search(r'\b(College|Colleges|Collège|Colegio)\b', name, flags=re.IGNORECASE):
        return 'College'
    elif re.search(r'\b(Institute|Institut|Institutions|Instituts|Instituto|Institución|Institucion)\b', name, flags=re.IGNORECASE):
        return 'Institute'
    elif re.search(r'\b(Universiteit|Universitat|Univesidade|Univerzitet|Universiti|Univerisity|Université|Universität|Universitas|Università|Universita|Universidade|Universidad|Üniversitesi|University|Uinversity|Universitatea)\b', name, flags=re.IGNORECASE):
        return 'University'
    else: return None

def process_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    conn = psycopg2.connect(
        database="postgres", user='postgres', password='postgres', host='localhost', port='5432'
    )
    cursor = conn.cursor()
    new_institutions = []
    for item in data:
        name = item.get('name')
        cursor.execute("SELECT * FROM institutions WHERE name = %s", (name,))
        existing_university = cursor.fetchone()
        if existing_university:
            continue
        country = item.get('country')
        alpha_two_code = item.get('alpha_two_code')
        state_province = item.get('state-province')
        type_institution = search_type(name)
        cursor.execute("INSERT INTO institutions (name, country, alpha_two_code, state_province, type_institution) VALUES (%s, %s, %s, %s, %s)", (name, country, alpha_two_code, state_province, type_institution))
        conn.commit()
        new_institutions.append((name, country, alpha_two_code, state_province, type_institution))
    
    cursor.close()
    conn.close()

dag = DAG(
    'institutions_processing',
    description='Process new institutions data and store in database',
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 1),
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag
)

extract_task >> process_task