
from airflow.providers.postgres.hooks.postgres import PostgresHook, RealDictCursor
import json

TableInstrumentExchangeStates="lookups.instrument_exchange_states"
TableCompanyStates="lookups.company_states"
TableExchanges="lookups.exchanges"
TableAssetStates="lookups.asset_states"
TableAssetTypes="lookups.asset_types"



class TopicEmptyError(Exception):
    def __init__(self, message, topic):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
            
        # Now for your custom code...
        self.topic = topic


class KafkaSchemaNotFound(Exception):
    def __init__(self, message, schema_name):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
            
        # Now for your custom code...
        self.schema = schema_name

class NumberOfAffectedRowsMismatch(Exception):
    def __init__(self, message, etl):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
            
        # Now for your custom code...
        self.etl = etl


class AirflowConfigEmpty(Exception):
    def __init__(self,message):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
            
        # Now for your custom code...
        # self.etl = etl
def loadInstrumentExchangeStates(pg_hook):
    select_statement=f" select * from {TableInstrumentExchangeStates}"
    connection = pg_hook.get_conn()
    nt_cur = connection.cursor(cursor_factory=RealDictCursor)
    nt_cur.execute(select_statement)
    result = nt_cur.fetchall()
    dictResult={}
    for item in result :
        dict_item = {}
        dict_item[f'{item["id"]}'] = {**item}
        dictResult =  {**dictResult, **dict_item }

    return dictResult
    
def loadCompanyStates(pg_hook):
    select_statement=f"select * from {TableCompanyStates}"
    connection = pg_hook.get_conn()
    nt_cur = connection.cursor(cursor_factory=RealDictCursor)
    nt_cur.execute(select_statement)
    result = nt_cur.fetchall()
    dictResult={}
    for item in result :
        dict_item = {}
        dict_item[f'{item["id"]}'] = {**item}
        dictResult =  {**dictResult, **dict_item }

    return dictResult

def loadExchanges(pg_hook):
    select_statement=f"select * from {TableExchanges}"
    connection = pg_hook.get_conn()
    nt_cur = connection.cursor(cursor_factory=RealDictCursor)
    nt_cur.execute(select_statement)
    result = nt_cur.fetchall()
    dictResult={}
    for item in result :
        dict_item = {}
        dict_item[f'{item["id"]}'] = {**item}
        dictResult =  {**dictResult, **dict_item }

    return dictResult

def loadAssetTypes(pg_hook):
    select_statement=f"select * from {TableAssetTypes}"
    connection = pg_hook.get_conn()
    nt_cur = connection.cursor(cursor_factory=RealDictCursor)
    nt_cur.execute(select_statement)
    result = nt_cur.fetchall()
    dictResult={}
    for item in result :
        dict_item = {}
        dict_item[f'{item["id"]}'] = {**item}
        dictResult =  {**dictResult, **dict_item }

    return dictResult

def loadAssetStates(pg_hook):
    select_statement=f"select * from {TableAssetStates}"
    connection = pg_hook.get_conn()
    nt_cur = connection.cursor(cursor_factory=RealDictCursor)
    nt_cur.execute(select_statement)
    result = nt_cur.fetchall()
    dictResult={}
    for item in result :
        dict_item = {}
        dict_item[f'{item["id"]}'] = {**item}
        dictResult =  {**dictResult, **dict_item }

    return dictResult