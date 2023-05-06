# airflow DAG 모듈 import
from airflow import DAG
# 날짜와 시간 출력하기 위한 import
from datetime import datetime, timedelta

# airflow 실행을 위한 PythonOperator import
from airflow.operators.python_operator import PythonOperator

# dict pass to airflow objects containing meta datas
default_args = {
    # owner name of the DAG
    'owner': 'airflow',
    # whether to rely on previous task status
    'depends_on_past': False,
    # start date of task instance
    'start_date': datetime(2021, 3, 31),
    # email address to get notifications
    'email': ['abc@abc.com'],
    # retry the task once, if it fails
    'retries': 1,
    # after waiting for 10 min
    'retry_delay': timedelta(minutes=10),
}

# configure schedule & set DAG settings
dag = DAG(
    dag_id='my_dag',
    # prior tasks not executed
    catchup = False,
    default_args=default_args,
    # how log DagRun should be up before timing out(failing)
    # dagrun_timeout = timedelta(minutes=30),
    # continue to run DAG once per hour
    schedule_interval = timedelta(hours=1),
    # schedule_interval = '*/1 * * * *', => 'M H D/M M D/W'
    # '@daily' = '0 0 * * *'
)

def print_time():
    now = datetime.now()

    print('=' * 20)
    print('현재 시간은 {}입니다.'.format(now))
    print('=' * 20)

def print_day():
    days = ['월', '화', '수', '목', '금', '토', '일']
    days_idx = datetime.now().date().weekday()

    print('=' * 20)
    print('오늘은 {}요일 입니다.'.format(days[days_idx]))
    print('=' * 20)


t1 = PythonOperator(
    task_id = 'print_time',
    python_callable = print_time,
    dag = dag,
)

t2 = PythonOperator(
    task_id = 'print_day',
    python_callable = print_day,
    dag = dag,
)

t1.set_downstream(t2) # or t1 >> t2