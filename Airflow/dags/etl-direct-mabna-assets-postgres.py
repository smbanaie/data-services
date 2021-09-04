from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from datetime import timedelta
import pprint
from textwrap import dedent
# from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
import elasticapm
from confluent_kafka import avro
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utility import KafkaSchemaNotFound

from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator 
# from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from requests.api import head
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

from airflow.utils.log.logging_mixin import LoggingMixin

##----------------------------- Functions -----------------------------------

def get_inst_groups(**kwargs): 
    logger = LoggingMixin().log
    clientAPM = elasticapm.Client(service_name='ETL-Basics', service_version="1.0",
                                  server_url=Variable.get("APM-Server"))
    try : 
        conn = Variable.get("URL-API-Mabna")
        logger.info("^-"*25)
        logger.info(f"Connection Base URL : {conn}")
        endpoint=f'/{Variable.get("Mabna-Assets-Endpoint")}'
        logger.info(f"Endpoint : {endpoint}")
        headers = {
            'Authorization': f'Basic {Variable.get("Mabna-Basic-Token")}',
            }
        count = 100
        increment_size=100
        skip=0
        flag=True
        data= []
        API_URL= f"{conn}{endpoint}"
        logger.info(f"Mabna URL in Request :{API_URL}")
        while flag :
            payload = {'_count': f'{count}', '_skip': f'{skip}'}
            r = requests.get(API_URL, params=payload, headers=headers)
            if r.status_code == 200:
                logger.info("-=-"*20)
                logger.info(f'Successfully read {count} data from {skip} to {skip+increment_size}')
                skip += increment_size
            return_data = r.json()["data"]
            if len(return_data) == 0 :
                flag = False
            data.extend(return_data)
        return data
    except Exception as ex : 
        clientAPM.capture_exception()
        logger.error(ex)
        raise ex


def delivery_report(err, msg):
    logger = LoggingMixin().log

    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logger.error('Message delivery failed: {}'.format(err))
        
    else:
        logger.info('Message delivered To {} [{}]'.format(msg.topic(), msg.partition()))



def save_2_pg(**kwargs):
    logger = LoggingMixin().log

    clientAPM = elasticapm.Client(service_name='ETL-Basics', service_version="1.0",
                                  server_url=Variable.get("APM-Server"))
    logger.info("*-"*25)
    errors=0
    try : 
        ti = kwargs['ti']
        asset_list = ti.xcom_pull( task_ids="etl_mabna_get_assets_req")
        logger.info(f"Len of Input data (Assets) : {len(asset_list)}")
        logger.info("^-"*25)
        logger.info(f"First Entry : {asset_list[0]}")
        logger.info(f"Last Entry : {asset_list[-1]}")
        logger.info("^-"*25)

        rows_count = 1
        for asset in asset_list : 
            try :
                logger.info(f"# {rows_count:5} : {asset.get('name','No Name Provided!')}")
                # logger.info(i)

                pg_hook = PostgresHook(postgres_conn_id='postgres_lookups')
                asset_insert = """INSERT INTO lookups.assets
(id, asset_type, trade_symbol, english_trade_symbol, asset_name, english_name, short_name, english_short_name, exchange_id, state_id, entity_id, entity_type, fingilish_name, fingilish_short_name, fingilish_trade_symbol, catgeory)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::int[]);
                """

                asset_update = """UPDATE lookups.assets
SET asset_type=%s, trade_symbol=%s, english_trade_symbol=%s, asset_name=%s, english_name=%s, short_name=%s, 
english_short_name=%s, exchange_id=%s, state_id=%s, entity_id=%s, entity_type=%s, fingilish_name=%s, fingilish_short_name=%s,
fingilish_trade_symbol=%s, catgeory=%s::int[] WHERE id=%s;"""

                logger.info(f"Row # {rows_count} is processing ... ")
                logger.info("--"*30)
                logger.info(pprint.pprint(asset))
                logger.info("--"*30)
                rows_count+=1
                if asset.get("meta").get("state") == "deleted" :
                    logger.info(f"Deleted Record Encountered! - ID : {asset.get('id')} ")
                else :     
                    ckeck_row_existed = f"select * from lookups.assets where id = {asset.get('id')} "
                    connection = pg_hook.get_conn()
                    nt_cur = connection.cursor()
                    nt_cur.execute(ckeck_row_existed)
                    result = nt_cur.fetchone()
                    # logger.info(f"Query Issued  : {ckeck_row_existed}")
                    categories = None
                    if asset.get("categories") :
                        categories = [x.get("id") for x in asset.get("categories") ]                        
                    if result != None :
                        
                        logger.info("Updating ... ")
                        pg_hook.run(asset_update, parameters=(
                                                    asset.get("type").get("id"),
                                                    asset.get("trade_symbol"),
                                                    asset.get("english_trade_symbol"),
                                                    asset.get("name"),
                                                    asset.get("english_name"),
                                                    asset.get("short_name"),
                                                    asset.get("english_short_name"),
                                                    asset.get("exchange").get("id") if asset.get("exchange") else None,
                                                    asset.get("state").get("id"),
                                                    asset.get("entity").get("id") if asset.get("entity") else None,
                                                    asset.get("entity").get("meta").get("type") if asset.get("entity") else None,
                                                    asset.get("fingilish_name"),
                                                    asset.get("fingilish_short_name"),
                                                    asset.get("fingilish_trade_symbol"),
                                                    categories,
                                                    asset.get("id")
                                                    ))
                    else : 
                        pg_hook.run(asset_insert, parameters=(asset.get("id"),
                                                    asset.get("type").get("id"),
                                                    asset.get("trade_symbol"),
                                                    asset.get("english_trade_symbol"),
                                                    asset.get("name"),
                                                    asset.get("english_name"),
                                                    asset.get("short_name"),
                                                    asset.get("english_short_name"),
                                                    asset.get("exchange").get("id") if asset.get("exchange") else None,
                                                    asset.get("state").get("id"),
                                                    asset.get("entity").get("id") if asset.get("entity") else None,
                                                    asset.get("entity").get("meta").get("type") if asset.get("entity") else None,
                                                    asset.get("fingilish_name"),
                                                    asset.get("fingilish_short_name"),
                                                    asset.get("fingilish_trade_symbol"),
                                                    categories
                                                    ))
                    
            except Exception as err : 
                clientAPM.capture_exception()
                logger.error(f"Exeption : {err}")
                raise err

        logger.info(f"Asset List - Len : {len(asset_list)}")
        select_rows_affected = """select count(*) as cnt from lookups.assets where updated_at > now() - interval '1 hour';"""
        rows_affected = pg_hook.get_first(select_rows_affected)[0]
        logger.info(f"Rows Inserted/Updated : {rows_affected}")
    except Exception as ex : 
        clientAPM.capture_exception()
        logger.error(f"Exeption : {ex}")
        raise ex

##----------------------------- Functions Ends Here---------------------------


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
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
    'ETL-Direct-Mabna-Assets-2-PG',
    default_args=default_args,
    description='A simple HttpRequest DAG',
    schedule_interval=timedelta(days=30),
    start_date=days_ago(0),
    tags=['mabna', 'rest-api','ref-tables'],
) as dag:

    task_get_request = PythonOperator(
        task_id='etl_mabna_get_assets_req',
        python_callable=get_inst_groups,
        dag=dag,
    )


    task_save_results = PythonOperator(
        task_id='etl_mabna_categories_2_pg',
        python_callable=save_2_pg,
        dag=dag,
    )

    task_get_request >> task_save_results 
