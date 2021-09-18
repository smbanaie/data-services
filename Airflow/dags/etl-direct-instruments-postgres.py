import airflow
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
        endpoint=f'/{Variable.get("Mabna-Instruments-Endpoint")}'
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
                                  server_url=Variable.get("APM-Server"), config={'transport_class':'elasticapm.transport.http.AsyncTransport'})
    logger.info("*-"*25)
    errors=0
    try : 
        ti = kwargs['ti']
        instrument_list = ti.xcom_pull( task_ids="etl_mabna_get_instruments_req")
        logger.info(f"Len of Input data (Assets) : {len(instrument_list)}")
        logger.info("^-"*25)
        logger.info(f"First Entry : {instrument_list[0]}")
        logger.info(f"Last Entry : {instrument_list[-1]}")
        logger.info("^-"*25)

        rows_count = 1
        for instrument in instrument_list : 
            try :

                logger.info(f"# {rows_count:5} : {instrument.get('name','No Name Provided!')}")
                # logger.info(i)

                pg_hook = PostgresHook(postgres_conn_id='postgres_lookups')
                asset_insert = """
INSERT INTO lookups.instruments
(id, code, isin, "name", english_name, short_name, english_short_name, "type",
 exchange_id, market_id, board_id, group_id, entity_id, value_type, base_volume,
 nominal_price, price_tick, trade_tick, payment_delay,
 minimum_volume_permit, maximum_volume_permit, exchange_state_id, asset_id, listing_date)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s);
                """
                asset_update = """
                UPDATE lookups.instruments
SET code=%s, isin=%s, "name"=%s, english_name=%s, short_name=%s, english_short_name=%s, "type"=%s, 
exchange_id=%s, market_id=%s, board_id=%s, group_id=%s, entity_id=%s, value_type=%s, base_volume=%s, 
nominal_price=%s, price_tick=%s, trade_tick=%s, payment_delay=%s, minimum_volume_permit=%s, 
maximum_volume_permit=%s, exchange_state_id=%s, asset_id=%s, listing_date=%s
WHERE id=%s; """

                logger.info(f"Row # {rows_count} is processing ... ")
                logger.info("--"*30)
                logger.info(pprint.pprint(instrument))
                logger.info("--"*30)
                rows_count+=1
                if instrument.get("meta").get("state") == "deleted" :
                    logger.info(f"Deleted Record Encountered! - ID : {instrument.get('id')} ")
                else :     
                    ckeck_row_existed = f"select * from lookups.instruments where id = {instrument.get('id')} "
                    connection = pg_hook.get_conn()
                    nt_cur = connection.cursor()
                    nt_cur.execute(ckeck_row_existed)
                    result = nt_cur.fetchone()
                    # logger.info(f"Query Issued  : {ckeck_row_existed}")
                    entity_id = 0
                    if instrument.get("stock") :
                        entity_id = instrument.get("stock").get("company").get("id")
                    elif instrument.get("option") :
                        entity_id = instrument.get("option").get("contract").get("id")    
                    elif instrument.get("index") :
                        entity_id = instrument.get("index").get("id")  
                    elif instrument.get("bond") :
                        entity_id = instrument.get("bond").get("bond").get("id")    
                    elif instrument.get("mortgage_loan") :
                        entity_id = instrument.get("mortgage_loan").get("mortgage_loan").get("id")    
                    else :
                        # raise ValueError("Entity Type in Instrument List is Unknown!", instrument)
                        entity_id=None
                    if result != None :
                        logger.info("Updating ... ")
                        pg_hook.run(asset_update, parameters=(
                                                    instrument.get("code"),
                                                    instrument.get("isin"),
                                                    instrument.get("name"),
                                                    instrument.get("english_name"),
                                                    instrument.get("short_name"),
                                                    instrument.get("english_short_name"),
                                                    instrument.get("type"),
                                                    instrument.get("exchange").get("id"),
                                                    instrument.get("market").get("id"),
                                                    instrument.get("board").get("id") if instrument.get("board") else None,
                                                    instrument.get("group").get("id"),
                                                    entity_id,
                                                    instrument.get("value_type"),
                                                    instrument.get("base_volume"),
                                                    instrument.get("nominal_price"),
                                                    instrument.get("price_tick"),
                                                    instrument.get("trade_tick"),
                                                    instrument.get("payment_delay"),
                                                    instrument.get("minimum_volume_permit"),
                                                    instrument.get("maximum_volume_permit"),
                                                    instrument.get("exchange_state").get("id"),
                                                    instrument.get("asset").get("id") if instrument.get("asset") else None,
                                                    instrument.get("listing_date"),
                                                    instrument.get("id")
                                                    ))
                    else : 
                        pg_hook.run(asset_insert, parameters=(instrument.get("id"),
                                                    instrument.get("code"),
                                                    instrument.get("isin"),
                                                    instrument.get("name"),
                                                    instrument.get("english_name"),
                                                    instrument.get("short_name"),
                                                    instrument.get("english_short_name"),
                                                    instrument.get("type"),
                                                    instrument.get("exchange").get("id"),
                                                    instrument.get("market").get("id"),
                                                    instrument.get("board").get("id"),
                                                    instrument.get("group").get("id"),
                                                    entity_id,
                                                    instrument.get("value_type"),
                                                    instrument.get("base_volume"),
                                                    instrument.get("nominal_price"),
                                                    instrument.get("price_tick"),
                                                    instrument.get("trade_tick"),
                                                    instrument.get("payment_delay"),
                                                    instrument.get("minimum_volume_permit"),
                                                    instrument.get("maximum_volume_permit"),
                                                    instrument.get("exchange_state").get("id"),
                                                    instrument.get("asset").get("id") if instrument.get("asset") else None,
                                                    instrument.get("listing_date")                                                    
                                                    ))
                    
            except ValueError as verr : 
                clientAPM.capture_exception()
                logger.error(f"Exeption : {verr}")
                continue
            except Exception as err : 
                clientAPM.capture_exception()
                logger.error(f"Exeption : {err}")
                raise err

        logger.info(f"Instrument List - Len : {len(instrument_list)}")
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
    'ETL-Direct-Mabna-Instruments-2-PG',
    default_args=default_args,
    description='A simple HttpRequest DAG',
    schedule_interval=timedelta(days=30),
    start_date=days_ago(0),
    tags=['mabna', 'rest-api','ref-tables'],
) as dag:

    task_get_request = PythonOperator(
        task_id='etl_mabna_get_instruments_req',
        python_callable=get_inst_groups,
        dag=dag,
    )


    task_save_results = PythonOperator(
        task_id='etl_mabna_instrument_2_pg',
        python_callable=save_2_pg,
        dag=dag,
    )

    task_get_request >> task_save_results 
