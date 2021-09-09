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
# from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from requests.api import get, head
from utility import NumberOfAffectedRowsMismatch

from airflow.utils.log.logging_mixin import LoggingMixin


##----------------------------- Functions -----------------------------------
def delivery_report(err, msg):
    logger = LoggingMixin().log

    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logger.error('Message delivery failed: {}'.format(err))
        
    else:
        logger.info('Message delivered To {} [{}]'.format(msg.topic(), msg.partition()))

def consume_companies():
    logger = LoggingMixin().log

# To consume latest messages and auto-commit offsets
    clientAPM = elasticapm.Client(service_name='ETL-Basics', service_version="1.0",
                                  server_url=Variable.get("APM-Server"))
    company_list=[]
    try :                               
        logger.info("*-"*25)
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
        
        logger.info (f"Key Schema Config : {key_schema_str} ")
        key_schema = avro.loads(key_schema_str)
        logger.info("*&"*25)
        TOPIC= Variable.get("Topic-Companies")
        logger.info(f"Topic : {TOPIC}")
        SUBJECT= Variable.get("Schema-Subject-Companies")
        logger.info("*$"*25)
        logger.info(f"Schema of Companies Data: {SUBJECT}")
        BOOTSTRAP_SERVERS=Variable.get("Bootstrap-Servers")
        logger.info("*$"*25)
        logger.info(f"Kafka Brokers : {BOOTSTRAP_SERVERS}")
        SCHEMA_REGISTRY_URL=Variable.get("Schema-Registry")
        logger.info("*$"*25)
        logger.info(f"Schema Registry URL : {SCHEMA_REGISTRY_URL}")
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
                logger.error("Message deserialization failed for {}: {}".format(msg, e))
                clientAPM.capture_exception()
                break

            if msg is None:

                # logger.info("-()-"*20)
                logger.info(f"# Processed msg : {msg_count}")
                if msg_count==0 :
                    error = TopicEmptyError("Topic Has No Messages",f"{TOPIC}")
                    try :
                        raise error
                    except Exception  as err:
                        c= clientAPM.capture_exception() 
                        consumer.close()
                        raise AirflowTaskTimeout()
                else :
                    logger.info("--"*25)        
                    logger.info(f"Message Queue Is Empty - Finishing The Poll Process of Topic:{TOPIC}")
                    consumer.close()
                    break
            
                
                
            elif msg.error():
                logger.info("AvroConsumer error: {}".format(msg.error()))
                clientAPM.capture_message("AvroConsumerError", msg.error())
                consumer.close()
                raise AirflowException()
            # logger.info("--"*25)
            msg_count+=1
            mvalue=msg.value()
            company_list.append(mvalue)
            logger.info(f"{msg_count:>5} : {mvalue.get('short_name')}")

        consumer.close()
        return company_list
    except Exception as err : 
        logger.error(f"Some Error : {err}")
        clientAPM.capture_exception()
        raise AirflowException()
 
def insert_into_pg(**kwargs):
    logger = LoggingMixin().log

    clientAPM = elasticapm.Client(service_name='ETL-Basics', service_version="1.0",
                                  server_url=Variable.get("APM-Server"))
    pg_hook = PostgresHook(postgres_conn_id='postgres_lookups')

    company_info_insert = """INSERT INTO lookups.company_info
(id, cp_name, short_name, english_name, description, fiscalyear, english_trade_symbol, trade_symbol, english_short_name, state_id, exchange_id)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    company_info_update = """UPDATE lookups.company_info
SET cp_name=%s, short_name=%s, english_name=%s, description=%s, fiscalyear=%s, english_trade_symbol=%s, trade_symbol=%s, english_short_name=%s, state_id=%s, exchange_id=%s
WHERE id=%s;"""

    ti = kwargs['ti']
    company_list = ti.xcom_pull(task_ids="etl_mabna_companies_consumer_from_kafka")
    companies_len=len(company_list)
    rows_count =0
    for company in company_list:
        # If converting the rate to a float fails for whatever reason then
        # just move on.
        rows_count+=1
        logger.info(f"Row # {rows_count} is processing ... ")
        try :
            ckeck_row_existed = f"select cp_name from lookups.company_info where id = {company.get('id')} "
            connection = pg_hook.get_conn()
            nt_cur = connection.cursor()
            nt_cur.execute(ckeck_row_existed)
            result = nt_cur.fetchone()
            # logger.info(f"Query Issued  : {ckeck_row_existed}")
            if result != None :
                logger.info("Updating ... ")
                pg_hook.run(company_info_update, parameters=(company.get("name"),
                                                company.get("short_name"),
                                                company.get("english_name"),
                                                company.get("description",""),
                                                company.get("fiscalyear"),
                                                company.get("english_trade_symbol"),
                                                company.get("trade_symbol"),
                                                company.get("english_short_name"),
                                                company.get("state").get("id") if company.get("state") else None,
                                                company.get("exchange").get("id") if company.get("exchange") else None ,
                                                company.get("id")
                                                ))
            else : 

                pg_hook.run(company_info_insert, parameters=(company.get("id"),
                                                company.get("name"),
                                                company.get("short_name"),
                                                company.get("english_name"),
                                                company.get("description",""),
                                                company.get("fiscalyear"),
                                                company.get("english_trade_symbol"),
                                                company.get("trade_symbol"),
                                                company.get("english_short_name"),
                                                company.get("state").get("id") if company.get("state") else None,
                                                company.get("exchange").get("id") if company.get("exchange") else None
                                                ))
                
        except Exception as err : 
            clientAPM.capture_exception()
            logger.error(f"Exeption : {err}")

            raise err
    logger.info(f"Company List Len : {companies_len}")
    select_rows_affected = """select count(*) as cnt from lookups.company_info where updated_at > now() - interval '1 hour';"""
    rows_affected = pg_hook.get_first(select_rows_affected)[0]

    logger.info(f"Rows Inserted/Updated : {rows_affected}")
    if rows_affected != companies_len :
        try : 
            raise NumberOfAffectedRowsMismatch(f"Company Lenght:{companies_len}/Rows Inserted{rows_affected}","ETL-Mabna-Companeis") 
        except Exception as ex:    
            clientAPM.capture_exception()
            return False
    return True 
        
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

    task_save_companies = PythonOperator(
        task_id='etl_mabna_companies_save_to_pg',
        python_callable=insert_into_pg,
        dag=dag,
    )

    task_consume_companies >> task_save_companies