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

from utility import AirflowConfigEmpty, loadInstrumentExchangeStates, loadCompanyStates, loadExchanges
from utility import loadAssetStates, loadAssetTypes
from airflow.exceptions import AirflowBadRequest
from airflow import DAG

import jdatetime
import pytz

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator 
# from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from requests.api import head
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

from airflow.utils.log.logging_mixin import LoggingMixin

from confluent_kafka import Producer
import socket
##----------------------------- Functions -----------------------------------

def get_inst_daily_trades(**kwargs): 
    try : 
        logger = LoggingMixin().log
        clientAPM = elasticapm.Client(service_name='ETL-Daily', service_version="1.0",
                                  server_url=Variable.get("APM-Server"))
        dagConfig=kwargs['dag_run'].conf
        logger.info(pprint.pformat(dagConfig))
        if len(dagConfig) < 3 :
            raise AirflowConfigEmpty("DAG Config Must have Instrument_Code, Start_Time, End_Time")
        else :
            for k,v in dagConfig.items():
                if len(v.strip())==0:
                    raise AirflowConfigEmpty(f"DAG Config : {k} Has Empty Value - Config Provided : {dagConfig}")
        conn = Variable.get("URL-API-Mabna")
        logger.info("^-"*25)
        logger.info(f"Base URL : {conn}")
        endpoint=f'/{json.loads(Variable.get("Endpoints-Daily")).get("Instrument-Trades")}'
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
            payload = {'_count': f'{count}', '_skip': f'{skip}','instrument.code':dagConfig["Instrument_Code"],
            'date_time':f'{dagConfig["End_Time"]},{dagConfig["Start_Time"]}', 'date_time_op':'bw'}
            logger.info(pprint.pformat(payload))
            r = requests.get(API_URL, params=payload, headers=headers)
            if r.status_code == 200:
                logger.info("-=-"*20)
                logger.info(f'Successfully read {count} data from {skip} to {skip+increment_size}')
                skip += increment_size
            else :
                raise AirflowBadRequest(r)
            return_data = r.json()["data"]
            if len(return_data) == 0 :
                flag = False
            else :
                data.extend(return_data)
                # logger.info(pprint.pp(return_data[0]))
        logger.info('-**'*20)
        logger.info(f"Len of received data : {len(data)}")
        logger.info('-**'*20)

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




def send_2_kafka(**kwargs):
    logger = LoggingMixin().log
    clientAPM = elasticapm.Client(service_name='ETL-Daily', service_version="1.0",
                                  server_url=Variable.get("APM-Server"), config={'transport_class':'elasticapm.transport.http.AsyncTransport'})
    logger.info("*-"*25)
    errors=0
    conf = {'bootstrap.servers': f"{Variable.get('Bootstrap-Servers')}",
            'client.id': socket.gethostname()}

    topic = json.loads(Variable.get("Topics-Daily")).get("Instrument-Daily-Trades","ETL-Instrument-Daily-Trades")
    producer = Producer(conf)
    ti = kwargs['ti']
    instrument_trades = ti.xcom_pull( task_ids="etl_daily_instruments_trades_req")
    pg_hook = PostgresHook(postgres_conn_id='postgres_lookups')
    lookupExchangeStates=loadInstrumentExchangeStates(pg_hook)
    lookupCompanyStates=loadCompanyStates(pg_hook)
    lookupExchanges=loadExchanges(pg_hook)
    lookupAssetTypes=loadAssetTypes(pg_hook)
    lookupAssetStates=loadAssetStates(pg_hook)

    utctz=pytz.timezone('UTC')
    tehrantz=pytz.timezone('Asia/Tehran')

    for trade in instrument_trades : 
        if trade.get("meta").get("state")=='deleted':
            continue
        # logger.info("-=-"*20)
        # logger.info(pprint.pp(trade))
        # logger.info("-=-"*20)
        trade_info={}
        trade_info["ins_code"]=trade.get("instrument").get("code")
        trade_info["ins_isin"]=trade.get("instrument").get("isin")
        trade_info["ins_name"]=trade.get("instrument").get("name")
        trade_info["ins_value_type"]=trade.get("instrument").get("value_type")
        trade_info["ins_base_volume"]=trade.get("instrument").get("base_volume")
        trade_info["ins_nominal_price"]=trade.get("instrument").get("nominal_price")
        trade_info["ins_price_tick"]=trade.get("instrument").get("price_tick")
        trade_info["ins_trade_tick"]=trade.get("instrument").get("trade_tick")
        trade_info["ins_payment_delay"]=trade.get("instrument").get("payment_delay")
        trade_info["ins_minimum_volume_permit"]=trade.get("instrument").get("minimum_volume_permit")
        trade_info["ins_maximum_volume_permit"]=trade.get("instrument").get("maximum_volume_permit")
        trade_info["ins_listing_date"]=trade.get("instrument").get("listing_date")
        trade_info["ins_share_count"]=trade.get("instrument").get("share_count")
        trade_info["ins_english_name"]=trade.get("instrument").get("english_name")
        trade_info["ins_short_name"]=trade.get("instrument").get("short_name")
        trade_info["ins_english_short_name"]=trade.get("instrument").get("english_short_name")
        trade_info["ins_type"]=trade.get("instrument").get("type")
        trade_info["ins_exch_code"]=trade.get("instrument").get("exchange").get("code")
        trade_info["ins_exch_title"]=trade.get("instrument").get("exchange").get("title")
        trade_info["ins_exch_english_title"]=trade.get("instrument").get("exchange").get("english_title")
        trade_info["ins_market_code"]=trade.get("instrument").get("market").get("code")
        trade_info["ins_market_title"]=trade.get("instrument").get("market").get("title")
        trade_info["ins_market_english_title"]=trade.get("instrument").get("market").get("english_title")
        trade_info["ins_board_code"]=trade.get("instrument").get("board").get("code")
        trade_info["ins_board_title"]=trade.get("instrument").get("board").get("title")
        trade_info["ins_board_english_title"]=trade.get("instrument").get("board").get("english_title")
        trade_info["ins_group_code"]=trade.get("instrument").get("group").get("code")
        trade_info["ins_group_title"]=trade.get("instrument").get("group").get("title")
        trade_info["ins_exchange_state_id"]=lookupExchangeStates.get((trade.get("instrument").get("exchange_state").get("id"))).get("id")
        trade_info["ins_exchange_state_title"]=lookupExchangeStates.get((trade.get("instrument").get("exchange_state").get("id"))).get("title")

        trade_info["ins_stock_company_name"]=trade.get("instrument").get("stock").get("company").get("name")
        trade_info["ins_stock_company_english_name"]=trade.get("instrument").get("stock").get("company").get("english_name")
        trade_info["ins_stock_company_short_name"]=trade.get("instrument").get("stock").get("company").get("short_name")
        trade_info["ins_stock_company_english_short_name"]=trade.get("instrument").get("stock").get("company").get("english_short_name")
        trade_info["ins_stock_company_trade_symbol"]=trade.get("instrument").get("stock").get("company").get("trade_symbol")
        trade_info["ins_stock_company_english_trade_symbol"]=trade.get("instrument").get("stock").get("company").get("english_trade_symbol")
        trade_info["ins_stock_company_state_code"]=lookupCompanyStates.get(trade.get("instrument").get("stock").get("company").get("state").get("id")).get("code")
        trade_info["ins_stock_company_state_title"]=lookupCompanyStates.get(trade.get("instrument").get("stock").get("company").get("state").get("id")).get("title")
        trade_info["ins_stock_company_exch_code"]=lookupExchanges.get(trade.get("instrument").get("stock").get("company").get("exchange").get("id")).get("code")
        trade_info["ins_stock_company_exch_title"]= lookupExchanges.get(trade.get("instrument").get("stock").get("company").get("exchange").get("id")).get("title")
        trade_info["ins_stock_company_fiscalyear"]=trade.get("instrument").get("stock").get("company").get("fiscalyear")

        trade_info["ins_asset_type_code"]=lookupAssetTypes.get(trade.get("instrument").get("asset").get("type").get("id")).get("code")
        trade_info["ins_asset_type_title"]=lookupAssetTypes.get(trade.get("instrument").get("asset").get("type").get("id")).get("title")
        trade_info["ins_asset_trade_symbol"]=trade.get("instrument").get("asset").get("trade_symbol")
        trade_info["ins_asset_english_trade_symbol"]=trade.get("instrument").get("asset").get("english_trade_symbol")
        trade_info["ins_asset_name"]=trade.get("instrument").get("asset").get("name")
        trade_info["ins_asset_english_name"]=trade.get("instrument").get("asset").get("english_name")
        trade_info["ins_asset_short_name"]=trade.get("instrument").get("asset").get("short_name")
        trade_info["ins_asset_short_name"]=trade.get("instrument").get("asset").get("english_short_name")
        trade_info["ins_asset_english_name"]=trade.get("instrument").get("asset").get("english_name")
        trade_info["ins_asset_english_short_name"]=trade.get("instrument").get("asset").get("english_short_name")
        trade_info["ins_asset_state_title"]=lookupAssetStates.get(trade.get("instrument").get("asset").get("state").get("id")).get("title")
        trade_info["ins_asset_state_english_title"]=lookupAssetStates.get(trade.get("instrument").get("asset").get("state").get("id")).get("english_title")
        trade_info["ins_asset_fingilish_name"]=trade.get("instrument").get("asset").get("fingilish_name")
        trade_info["ins_asset_fingilish_short_name"]=trade.get("instrument").get("asset").get("fingilish_short_name")
        trade_info["ins_asset_fingilish_trade_symbol"]=trade.get("instrument").get("asset").get("fingilish_trade_symbol")

        trade_info["date_time_persian"]=trade.get("date_time")
        trade_info["open_price"]=trade.get("open_price")
        trade_info["high_price"]=trade.get("high_price")
        trade_info["low_price"]=trade.get("low_price")
        trade_info["close_price"]=trade.get("close_price")
        trade_info["close_price_change"]=trade.get("close_price_change")
        trade_info["real_close_price"]=trade.get("real_close_price")
        trade_info["real_close_price_change"]=trade.get("real_close_price_change")
        trade_info["trade_count"]=trade.get("trade_count")
        trade_info["volume"]=trade.get("volume")
        trade_info["value"]=trade.get("value")
        trade_dt = trade.get("date_time")
        local_dt=jdatetime.datetime.strptime(trade_dt, '%Y%m%d%H%M%S')
        k=tehrantz.localize(local_dt)
        k2=utctz.normalize(k.togregorian())
        trade_info["date_time"]=k2.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(pprint.pformat(trade_info))
        producer.produce(topic, key=trade.get("id"), value=json.dumps(trade_info) , callback=delivery_report)

    producer.poll(10)
    return True


def save_2_pg(**kwargs):
    logger = LoggingMixin().log

    clientAPM = elasticapm.Client(service_name='ETL-Daily', service_version="1.0",
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
                logger.info(pprint.pformat(instrument))
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
    'ETL-Daily-Mabna-Instruments-2-PG',
    default_args=default_args,
    description='A simple HttpRequest DAG',
    schedule_interval=timedelta(days=30),
    start_date=days_ago(0),
    tags=['mabna', 'rest-api','ref-tables'],
) as dag:

    task_get_request = PythonOperator(
        task_id='etl_daily_instruments_trades_req',
        python_callable=get_inst_daily_trades,
        dag=dag,
    )


    task_save_results = PythonOperator(
        task_id='etl_daily_instrument_trades_2_kafka',
        python_callable=send_2_kafka,
        dag=dag,
    )

    task_get_request >> task_save_results 
