from databricks.sdk.runtime import *
import pyspark
from .base import BaseModel
from enum import Enum

class Connector(Enum):
    POSTGRES = 1
    DELTA = 2

class MaterializedModel():

    def __init__(self, schema: str, table: str, model_query_path: str,  connector: Connector=Connector.POSTGRES, post_creation_commands: list[str] = [], force_commands: bool = False,) -> None:
        self.schema = schema
        self.table = table
        self.model_query_path = model_query_path
        self.post_creation_commands = post_creation_commands
        self.force_commands = force_commands
        self.creation = False
        self.base = BaseModel(model_query_path, post_creation_commands)
        self.connector = connector
        self.postgress = PostgressMaterialized()
        self.delta = DeltaMaterialized()

    def build(self) -> None:
        self.__create_table()
        print(f'Modelo {self.schema}.{self.table} criado/atualizado com sucesso')

    def __create_table(self) -> None:
        match self.connector.name:
            case 'POSTGRES':
                self.postgress.create_table(self.schema, self.table, self.base)
            case 'DELTA':
                self.delta.create_table(self.schema, self.table, self.base)
            case _:
                raise Exception('Conector não encontrado')

class PostgressMaterialized():
    def __init__(self) -> None:
        pass

    def create_table(self, schema: str, table: str, base: BaseModel) -> None:
        self.__drop_table(schema, table, base)
        command = f"""
            CREATE TABLE {schema}.{table} AS
            {base.get_model_query_string()}
        """
        base.run_command(command)
        base.run_post_creation_commands()

    def __drop_table(self, schema: str, table: str, base: BaseModel) -> None:
        command = f"""
        DROP TABLE IF EXISTS {schema}.{table} CASCADE;
        """
        base.run_command(command)


class DeltaMaterialized():
    def create_table(self, schema: str, table: str, base: BaseModel) -> None:
        df = self.__read(base)
        df.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{table}")

    def __read(self, base: BaseModel) -> pyspark.sql.DataFrame:
        return (
            spark.read.format("jdbc") 
            .option("url", base.dw_url) 
            .option("query", base.get_model_query_string()) 
            .option("user", base.dw_properties['user']) 
            .option("password", base.dw_properties['password']) 
            .load())