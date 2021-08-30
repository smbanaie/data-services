from airflow.models import Variable
from datetime import timedelta
import ast
from textwrap import dedent
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
import json
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
# import elasticapm
from confluent_kafka import avro



# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator 
# from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

##----------------------------- Functions -----------------------------------


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def produce_cities(**kwargs):
    # clientAPM = elasticapm.Client(service_name='ETL', service_version="1.0",
                                #   server_url=Variable.get("Schema-Subject-Cities"))
    print("*-"*25)
    key_schema_str = '''
    {"namespace": "saba.references.city",
    "name": "key",
    "type": "record",
    "fields" : [
        {
        "name" : "cityCode",
        "type" : "string"
        }
    ]}'''
     
    print (f"Key Schema Config : {key_schema_str} ")
    key_schema = avro.loads(key_schema_str)
    print("*&"*25)
    TOPIC= Variable.get("Topic-Cities")
    print("*$"*25)
    print(f"Topic : {TOPIC}")
    SUBJECT= Variable.get("Schema-Subject-Cities")
    print("*$"*25)
    print(f"Schema of Cities: {SUBJECT}")
    BOOTSTRAP_SERVERS=Variable.get("Bootstrap-Servers")
    print("*$"*25)
    print(f"Kafka Brokers : {BOOTSTRAP_SERVERS}")
    SCHEMA_REGISTRY_URL=Variable.get("Schema-Registry")
    print("*$"*25)
    print(f"Schema Registry URL : {SCHEMA_REGISTRY_URL}")
    try : 
        ti = kwargs['ti']
        city_list = json.loads(ti.xcom_pull( task_ids="etl_rayan_cities_get_cities"))
        print(f"Len of Input data  : {len(city_list)}")
        print(f"First Entry : {city_list[1]}")
        # clientAPM.capture_message(f"{city_list[0]}")
        print("*-"*25)
        # client= CachedSchemaRegistryClient(SchemaRegistryConfig)    
        client= CachedSchemaRegistryClient({"url" : f"{SCHEMA_REGISTRY_URL}" })    
        schema_id, schema, version = client.get_latest_schema(f"{SUBJECT}")
        print(f"Schema ID: {schema_id}, Schema : {schema}, Version : {version}")
        print("*-"*25)
        i =1
        avroProducer = AvroProducer({
                'bootstrap.servers': f'{BOOTSTRAP_SERVERS}',
                'on_delivery': delivery_report,
                'schema.registry.url': f'{SCHEMA_REGISTRY_URL}'
                },
                default_value_schema=schema, default_key_schema=key_schema)
        for city in city_list : 
            print("*$"*25)
            print(city)
            try :
                avroProducer.produce(topic=f'{TOPIC}', value=city, key = {'cityCode' : city['cityCode']})
                print(i)
                i+=1
                if (i%10==0) :
                    avroProducer.flush()
            except Exception as es : 
                # clientAPM.capture_exception()
                raise es
        avroProducer.flush()
        return True
    except Exception as e:
        print("es exception: " + str(e))
        # clientAPM.capture_exception()
        raise e

##----------------------------- Functions Ends Here---------------------------


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
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
with DAG(
    'Rayan-Cities',
    default_args=default_args,
    description='A simple HttpRequest DAG',
    schedule_interval=timedelta(days=30),
    start_date=days_ago(5),
    tags=['rayan', 'rest-api'],
) as dag:


    task_get_token = SimpleHttpOperator(
        task_id='etl_rayan_cities_get_token',
        method='POST',
        http_conn_id='http_service',
        data= Variable.get("Rayan-Authentication"),
        endpoint='/authenticate',
        dag=dag,
        headers = {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache',
        },
        response_filter=lambda response: response.json()['clientToken'],
    )

    
    task_get_cities = SimpleHttpOperator(
        task_id='etl_rayan_cities_get_cities',
        method='GET',
        http_conn_id='http_service',
        endpoint='/cities?dsCode=120',
        dag=dag,
        headers = {
        'X-CLIENT-TOKEN':'{{ ti.xcom_pull(task_ids="etl_rayan_cities_get_token") }}'
        }
        # response_filter=lambda response: response.json()['clientToken'],
    )

    task_save_results = PythonOperator(
        task_id='etl_rayan_cities_save_result',
        python_callable=produce_cities,
        dag=dag,
)


    task_get_token >> task_get_cities >> task_save_results