from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

import requests
import logging

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    
    return conn.cursor()

def _create_table(cur, schema, table, drop_first):
    """테이블 생성 함수, 임시 테이블과 저장 테이블을 생성"""
    # Full Refresh : 테이블이 존재한다면 지우고 적재
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    
    # 국가 이름은 공식 명칭으로 -> 문자열 길이를 길게 설정
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            name varchar(128),
            population bigint,
            area float
        );
    """
    cur.execute(create_table_sql)

def extract_country_data(**context):
    """api로부터 각 국가 별 이름, 인구수, 크기 정보를 json으로 가져옴"""
    
    logging.info('extract started')
    # 나라 정보를 가져올 api 엔드포인트
    url = context['params']['url']
        
    # get으로 응답을 받아온 후, json 데이터 저장
    res = requests.get(url)
    json_res = res.json()
    
    logging.info('extract done')
    
    return json_res

def transform_country_data(**context):
    """url에서 받아온 국가 json 데이터에서 이름, 인구, 크기를 리스트에 저장 후 반환"""
    
    logging.info('transform started')
    
    # extract 단계에서 받아 온 json 데이터    
    json_res = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")   
    
    # 결과를 저장할 레코드 배열    
    records = []

    # json 데이터를 돌면서 나라 명칭, 인구, 크기를 records에 저장
    for idx in range(len(json_res)):
        country_data = json_res[idx]
        
        official_name = country_data['name']['official']
        
        # 작은 따옴표가 있는 경우는 insert 시 오류 방지를 위해 다른 문자열로 교체
        # 예시) People's Republic of China
        if '\'' in official_name:
            official_name = official_name.replace('\'', '’')
        
        population = country_data['population']
        area = country_data['area']
        
        logging.info((official_name, population, area))
        records.append((official_name, population, area))
    
    
    logging.info('transform done')
    
    return records

def load_country_data(**context):
    """records 리스트에 저장된 각 국가의 정보를 Redshift 테이블에 적재"""
    
    logging.info('load started')
    
    cur = get_Redshift_connection()
    
    # 테이블 생성에 필요한 두 정보
    schema = context['params']['schema']
    table = context['params']['table']
    
    # get_country_all 함수에서 리턴한 records 데이터
    task_instance = context["task_instance"]
    records = task_instance.xcom_pull(key="return_value", task_ids="transform")    
    
    try:
        cur.execute('BEGIN;')
        
        # 최초 테이블 생성
        _create_table(cur, schema, table, drop_first=False)
        
        # 임시 테이블 생성하여 원본 복사
        cur.execute(f"CREATE TEMP TABLE temp AS SELECT * FROM {schema}.{table};")
        for r in records:
            insert_sql = f"INSERT INTO temp VALUES (\'{r[0]}\', {r[1]}, {r[2]});"
            logging.info(insert_sql)
            cur.execute(insert_sql)
        
        # 원본 테이블 생성
        _create_table(cur, schema, table, drop_first=True)
        
        # 임시 -> 원본 테이블로 데이터 이동
        cur.execute(f"INSERT INTO {schema}.{table} SELECT DISTINCT * FROM temp;")
        cur.execute("COMMIT;")
            
    except Exception as e:
        logging.info(e)
        cur.execute("ROLLBACK;")
        raise
    
    # 결과 출력
    cur.execute(f'SELECT * FROM {schema}.{table}')
    result = cur.fetchall()
    
    logging.info(result)
    
    logging.info('load done')
    
dag = DAG(
    dag_id = 'update_country',
    start_date = datetime(2023,6,5), 
    schedule = '30 6 * * 6',  # 매 주 토요일 오전 6시 30분에 실행
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_country_data,
    params={
        'url' : Variable.get('country_api_url') # 국가 정보를 가져올 api 엔드포인트 
    },
    provide_context=True,
    dag=dag
)

transform = PythonOperator(
    task_id ='transform',
    python_callable=transform_country_data,
    params={},
    provide_context=True,
    dag=dag
)

load = PythonOperator(
    task_id='load',
    python_callable=load_country_data,
    params={
        'schema': 'chanwoo0628',
        'table': 'country_info'
    },
    dag=dag
)

extract >> transform >> load
