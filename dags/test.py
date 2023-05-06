from datetime import timedelta

# DAG 인스턴스화에 사용하는 라이브러리
from airflow import DAG
from kubernetes import client, config
import ssl
# Operator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

def k8s_test(
    config = client.Configuration()
    config.api_key['authorization'] = open('/var/run/secrets/kubernetes.io/serviceaccount/token').read()
    config.api_key_prefix['authorization'] = 'Bearer'
    config.host = 'https://kubernetes.default'
    config.ssl_ca_cert = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
    config.verify_ssl=True

# api_client는 "2. 연결 정보 설정하기" 항목을 참고한다
api_client = client.CoreV1Api(client.ApiClient(config))

# 첫 번째 argument에 당신이 사용하는 namespace를 입력한다
ret = api_client.list_namespaced_pod("namespace 입력", watch=False)

print("Listing pods with their IPs:")

for i in ret.items:
    print(f"{i.status.pod_ip}\t{i.metadata.name}")
)


# DAG 인스턴스화
dag = DAG(
    'k8s test',
    default_args=default_args,
    description='echo "k8s test"',
    schedule_interval=timedelta(days=1),

)

t1 = BashOperator(
    task_id='echo_k8s',
    bash_command=k8s_test,
    dag=dag,
)
