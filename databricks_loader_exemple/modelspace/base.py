from enum import Enum

import psycopg2
import pyspark

from databricks.sdk.runtime import *

class PostgresDB(Enum):
    DW = 'DW'
    DB = 'DB'


class BaseModel:
    def __init__(self, model_query_path: str, post_creation_commands: list[str], db: PostgresDB = PostgresDB.DW) -> None:
        self.__set_datawarehouse_secrets()
        self.__set_application_db_secrets()
        if db == PostgresDB.DW:
            self.__set_application_datawarehouse_connection()
        if db == PostgresDB.DB:
            self.__set_application_db_connection()
        self.model_query_path = model_query_path
        self.post_creation_commands = post_creation_commands

    def get_model_query_string(self) -> str:
        with open(self.model_query_path, 'r') as file:
            return file.read()

    def run_post_creation_commands(self) -> None:
        cursor = self.db_conn.cursor()
        for command in self.post_creation_commands:
            cursor.execute(command)
        self.db_conn.commit()
        cursor.close()
        print('Comandos pós criação rodados com sucesso!')

    def run_command(self, command: str) -> None:
        cursor = self.db_conn.cursor()
        cursor.execute(command)
        self.db_conn.commit()
        cursor.close()
        
    def __set_datawarehouse_secrets(self) -> None:
        self.dw_url = dbutils.secrets.get("#####-secrets", "write-url")
        user_write = dbutils.secrets.get("#####-secrets", "write-user")
        password_write = dbutils.secrets.get("#####-secrets", "write-password")
        self.dw_properties = {"user": user_write,"password": password_write}

    def __set_application_db_secrets(self) -> None:
        self.db_url = dbutils.secrets.get("#####-secrets", "read-db")
        user = dbutils.secrets.get("#####-secrets", "read-user")
        password = dbutils.secrets.get("#####-secrets", "read-password")
        self.db_properties = {"user": user,"password": password}  

    def __set_application_datawarehouse_connection(self): 
        self.db_conn = psycopg2.connect(
            dbname="postgres",
            user=self.dw_properties['user'],
            password=self.dw_properties['password'],
            host="dashboard-#####.rds.amazonaws.com",
            port=5432
        )

    def __set_application_db_connection(self): 
        self.db_conn = psycopg2.connect(
            dbname="#####",
            user=self.dw_properties['user'],
            password=self.dw_properties['password'],
            host=self.db_url,
            port=5432
        )

    def __del__(self):
        self.db_conn.close()