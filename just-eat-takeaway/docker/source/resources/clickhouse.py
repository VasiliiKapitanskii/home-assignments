import clickhouse_connect
from dagster import ConfigurableResource


class ClickHouseResource(ConfigurableResource):
    """Loads data to ClickHouse"""

    host: str
    port: int

    def execute_query(self, query):
        """Execute a ClickHouse query"""
        client = clickhouse_connect.get_client(host=self.host, port=self.port)

        # A real resource should do error handling here

        client.raw_query(query=query)
