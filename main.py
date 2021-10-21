from mysql_helper import SqlHelper
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.schema import MetaData
from dotenv import load_dotenv
import os
import pandas as pd
from getData import query_csv

load_dotenv()

MYSQL_CONN_STR = os.getenv('MYSQL_CONN_STR')
REDIS_CONN_STR = os.getenv('REDIS_CONN_STR')


sql_helper = SqlHelper(MYSQL_CONN_STR)
engine = sql_helper.init_connection()
metadata = MetaData(engine)
inspect_obj = inspect(engine)


def get_data_from_csv(res,csv_path):
    df = pd.read_csv(csv_path)
    query_dict = {}
    for i in res["query"]:
        query_dict['key'] = i
        query_dict['value'] = res["query"][i]
    data = query_csv(df,query_dict)
    return data