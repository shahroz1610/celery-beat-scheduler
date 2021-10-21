from main import engine,metadata,inspect_obj,sql_helper,get_data_from_csv,MYSQL_CONN_STR,REDIS_CONN_STR
# import main
from celery import Celery
from sqlalchemy.sql.expression import select


res = {
    "schedule_id": "001", 
    "query": {"country": "USA"}, 
    "chunk_size": 10, 
    "frequency": 30
}
csv_path = 'data/Assignment Sheet.csv'
# add conn string here
connection_str = MYSQL_CONN_STR
redis_str = REDIS_CONN_STR
data = get_data_from_csv(res,csv_path)

app = Celery('scheduler',broker=redis_str,backend=redis_str)


@app.task
def migrate_chunks(chunk_size:int,pointer_table_name:str,id:str):
    """[Function to insert data into the database]

    Args:
        chunk_size (int): [No of entries to be inserted at once]
        pointer_table_name (str): [Name of the table where the number of entries inserted is stored] 
        id (str): [Name of the target table]
    """
    conn = engine.connect()
    start = sql_helper.get_db_pointer(conn,engine,inspect_obj,metadata,'pointer',f'a{id}')
    sql_helper.update_db_pointer(conn,engine,metadata,'pointer',f'a{id}',start+chunk_size)
    values = data[start:start+chunk_size]
    sql_helper.insert_into_db(engine,inspect_obj,f'a{id}',values)
    conn.close()