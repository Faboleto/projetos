from databricks.sdk.runtime import *
import pyspark

from .base import BaseModel


class ViewModel():
    def __init__(self, schema: str, table: str, model_query_path: str, post_creation_commands: list[str]) -> None:
        self.schema = schema
        self.table = table
        self.base = BaseModel(model_query_path, post_creation_commands)

    def build(self) -> None:
        query = self.base.get_model_query_string()
        self.__create_view(query)
        print(f'Modelo {self.schema}.{self.table} View criada com sucesso!')
        self.base.run_post_creation_commands()

    def __create_view(self, query: str) -> None:
        self.base.run_command(self.__create_view_command(query))

    def __create_view_command(self, query: str) -> str:
        return f"""
        CREATE OR REPLACE VIEW {self.schema}.{self.table} AS
        {query}
        """

