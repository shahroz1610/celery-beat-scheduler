from sqlalchemy import create_engine,inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import select, update
from sqlalchemy.sql.schema import Column, MetaData, Table
from sqlalchemy.sql.sqltypes import DateTime, Float, Integer, LargeBinary, String

class SqlHelper:
    def __init__(self,connection_str) -> None:
        self.connection_str = connection_str
    def init_connection(self):
        """[Function to create connection with MySQL]

        Args:
            connection_str (str): [Connection string]

        Returns:
            [session]: [session object]
        """
        engine = create_engine(self.connection_str)
        return engine

    def create_table(self,engine,table_name,inspect_obj)->bool:
        """[Creates table for the given table name if it does not exist]

        Args:
            engine ([create_engine]): [Instance of create_engine]
            table_name ([str]): [Name of the table]

        Returns:
            bool: [Table object of the table_name]
        """
        metadata = MetaData(engine)
        if not inspect_obj.has_table(table_name):
            table = Table(table_name,metadata,
                Column('timestamp',DateTime),
                Column('ver',String(32)),
                Column('product_family',String(32)),
                Column('country',String(32)),
                Column('device_type',String(32)),
                Column('os',String(32)),
                Column('checkout_failure_count',Float),
                Column('payment_api_failure_count',Float),
                Column('purchase_count',Float),
                Column('revenue',Float)
            )
            table.create(engine)
        else:
            table = Table(table_name,metadata,autoload_with=engine)
        return table

    def insert_into_db(self,engine,inspect_obj,table_name:str,values:list)->bool:
        """[Function to insert chunks of data into the database]

        Args:
            engine(): [Engine object]
            table_name (str): [Name of the target table]
            values (list): [Values to be inserted in the table]

        Returns:
            bool: [Status of the operation]
        """
        table_obj = self.create_table(engine,table_name,inspect_obj)
        conn = engine.connect()
        conn.execute(table_obj.insert(),values)

    def get_db_pointer(self,conn,engine,inspect_obj,metadata,table_name:str,id:str)->int:
        """[Function to get the count of total documents that have been inserted into the database]

        Args:
            table_name (str): [Name of the target table]
        Returns:
            int : Count of the documents that have been inserted
        """
        if not inspect_obj.has_table(table_name):
            table_obj = Table('pointer',metadata,
                        Column('id',String(32)),
                        Column('count',Integer)
                )
            table_obj.create(engine)
        else:
            table_obj = Table(table_name,metadata,autoload_with=engine)
        stmt = select(table_obj.c.count).where(table_obj.c.id==id)
        res = conn.execute(stmt).fetchall()
        if len(res) == 0:
            self.insert_into_db(engine,inspect_obj,'pointer',[{'id':id,'count':0}])
            return 0
        return res[0][0]
    
    def update_db_pointer(self,conn,engine,metadata,table_name:str,id:str,final_value:int):
        """[Function to update the number of documents migrated]

        Args:
            metadata ([type]): [description]
            table_name (str): [description]
        """
        table_obj =Table(table_name,metadata,autoload_with=engine)
        stmt = update(table_obj).where(table_obj.c.id==id).values(count=final_value)
        return conn.execute(stmt)