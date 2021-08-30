from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from datetime import timedelta
import ast, sys
from textwrap import dedent
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
import json
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
import elasticapm
from confluent_kafka import avro
import requests
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from utility import TopicEmptyError
from airflow.exceptions import AirflowTaskTimeout, AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator 
# from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from requests.api import get, head
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

##----------------------------- Functions -----------------------------------
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
        
    else:
        print('Message delivered To {} [{}]'.format(msg.topic(), msg.partition()))



def consume_companies():

# To consume latest messages and auto-commit offsets
    clientAPM = elasticapm.Client(service_name='ETL-Basics', service_version="1.0",
                                  server_url=Variable.get("APM-Server"))
    company_list=[]
    try :                               
        print("*-"*25)
        key_schema_str = '''
        {"namespace": "saba.references.basics.company",
        "name": "key",
        "type": "record",
        "fields" : [
            {
            "name" : "id",
            "type" : "string"
            }
        ]}'''
        
        print (f"Key Schema Config : {key_schema_str} ")
        key_schema = avro.loads(key_schema_str)
        print("*&"*25)
        TOPIC= Variable.get("Topic-Companies")
        print(f"Topic : {TOPIC}")
        SUBJECT= Variable.get("Schema-Subject-Companies")
        print("*$"*25)
        print(f"Schema of Companies Data: {SUBJECT}")
        BOOTSTRAP_SERVERS=Variable.get("Bootstrap-Servers")
        print("*$"*25)
        print(f"Kafka Brokers : {BOOTSTRAP_SERVERS}")
        SCHEMA_REGISTRY_URL=Variable.get("Schema-Registry")
        print("*$"*25)
        print(f"Schema Registry URL : {SCHEMA_REGISTRY_URL}")
        import random 
        Group_ID=f"ETL-Comapnies-Kafka-2-PG"


        consumer = AvroConsumer({
            'default.topic.config': {'auto.offset.reset': 'earliest'},
            'enable.auto.commit': True,
            'session.timeout.ms': 10000,
            'heartbeat.interval.ms': 1000,
            'api.version.request': True,
            'bootstrap.servers': f'{BOOTSTRAP_SERVERS}',
            'group.id': f'{Group_ID}',
            'schema.registry.url': f'{SCHEMA_REGISTRY_URL}'})
        r=consumer.subscribe([TOPIC])

        msg_count=0
        while True:
            try:
                msg = consumer.poll(10)

            except SerializerError as e:
                print("Message deserialization failed for {}: {}".format(msg, e))
                clientAPM.capture_exception()
                break

            if msg is None:

                print("-()-"*20)
                print(f"# Processed msg : {msg_count}")
                if msg_count==0 :
                    error = TopicEmptyError("Topic Has No Messages",f"{TOPIC}")
                    try :
                        raise error
                    except Exception  as err:
                        c= clientAPM.capture_exception(sys.exc_info()) 
                        consumer.close()
                        raise AirflowTaskTimeout()
                else :
                    print("--"*25)        
                    print(f"Message Queue Is Empty - Finishing The Poll Process of Topic:{TOPIC}")
                    break
            
                
                
            elif msg.error():
                print("AvroConsumer error: {}".format(msg.error()))
                clientAPM.capture_message("AvroConsumerError", msg.error())
                raise AirflowException()
            print("--"*25)
            msg_count+=1
            mvalue=msg.value()
            company_list.append()
            print(f"{msg_count:>5} : {mvalue}")

        consumer.close()
        return company_list
    except Exception as err : 
        print(f"Some Error : {err}")
        clientAPM.capture_exception()
        raise AirflowException()
 
def insert_into_pg(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_lookups')
    company_info_insert = """INSERT INTO lookups.company_info
(id, cp_name, short_name, english_title, description, fiscalyear, english_trade_symbol, trade_symbol, english_short_name, english_name, state_id, exchange_id)
VALUES(%d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %d);"""

    company_info_update = """UPDATE lookups.company_info
SET cp_name=%s, short_name=%s, english_title=%s, description=%s, fiscalyear=%s, english_trade_symbol=%s, trade_symbol=%s, english_short_name=%s, english_name=%s, state_id=%d, exchange_id=%d
WHERE id=%d;"""

    ti = kwargs['ti']
    company_list = ti.xcom_pull(task_ids="etl_mabna_companies_consumer_from_kafka")
    for company in company_list:
        # If converting the rate to a float fails for whatever reason then
        # just move on.
       
        try : 
            pg_hook.run(company_info_update, parameters=(company.get("name"),
                                              company.get("short_name"),
                                              company.get("english_name",None),
                                              company.get("description",""),
                                              company.get("fiscalyear",None),
                                              company.get("english_trade_symbol",None),
                                              company.get("trade_symbol",None),
                                              company.get("english_short_name",None),
                                              company.get("state").get("id"),
                                              company.get("exchange").get("id"),
                                              company.get("id")
                                              ))
        except Exception as ex : 
            print("-(^)-"*20)
            print("Inserting ... ")
            pg_hook.run(company_info_insert, parameters=(company.get("id"),
                                              company.get("name"),
                                              company.get("short_name"),
                                              company.get("english_name",None),
                                              company.get("description",""),
                                              company.get("fiscalyear",None),
                                              company.get("english_trade_symbol",None),
                                              company.get("trade_symbol",None),
                                              company.get("english_short_name",None),
                                              company.get("state").get("id"),
                                              company.get("exchange").get("id")
                                              ))



##----------------------------- Functions Ends Here---------------------------


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
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
    'ETL-Mabna-Companies-2-PG',
    default_args=default_args,
    description='A simple HttpRequest DAG',
    schedule_interval=None,
    start_date=days_ago(0),
    tags=['mabna', 'rest-api','ref-tables'],
) as dag:

    # task_get_request = PythonOperator(
    #     task_id='etl_mabna_get_companies_req',
    #     python_callable=get_companies,
    #     dag=dag,
    # )


    # task_save_results = PythonOperator(
    #     task_id='etl_mabna_companies_to_kafka',
    #     python_callable=produce_companies,
    #     dag=dag,
    # )


    task_consume_companies = PythonOperator(
        task_id='etl_mabna_companies_consumer_from_kafka',
        python_callable=consume_companies,
        dag=dag,
    )
