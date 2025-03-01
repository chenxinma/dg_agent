from __future__ import annotations

import age
from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Pattern, Tuple, Union, Sequence

# GRAPH_NAME = settings.get_setting("age")["graph"]
# DSN = settings.get_setting("age")["dsn"]

class AGEQueryException(Exception):
    """Exception for the AGE queries."""

    def __init__(self, exception: Union[str, Dict]) -> None:
        if isinstance(exception, dict):
            self.message = exception["message"] if "message" in exception else "unknown"
            self.details = exception["details"] if "details" in exception else "unknown"
        else:
            self.message = exception
            self.details = "unknown"

    def get_message(self) -> str:
        """get message"""
        return self.message

    def get_details(self) -> Any:
        """get details"""
        return self.details

class AGEGraph:
    """
    Apache AGE 操作类
    """

    def __init__(
        self, graph_name: str, dns: str
    ) -> None:
        self.graph_name = graph_name
        self.dns = dns
        self.age:age.Age = age.connect(graph=graph_name, dsn=dns)

        with self.age.connection.cursor() as curs:
            curs.execute(f"""SELECT graphid FROM ag_catalog.ag_graph WHERE name = '{graph_name}'""")
            data = curs.fetchone()

            self.graphid = data.graphid
            self.refresh_schema()

    def query(self, query: str, params:Sequence=None) -> List[Dict[str, Any]]:
        """
        执行查询
        """

        # execute the query, rolling back on an error
        with self.age.connection.cursor() as curs:
            try:
                if params is None:
                    query = query.replace("{GRAPH_NAME}", self.graph_name)
                    curs.execute(query)
                else:
                    curs.execute(query, [self.graph_name].extend(params))
            except age.SqlExecutionError as e:
                self.age.rollback()
                raise AGEQueryException(
                    {
                        "message": f"Error executing graph query: {query}",
                        "detail": str(e),
                    }
                ) from e

            return curs.fetchall()


    def _get_labels(self) -> Tuple[List[str], List[str]]:
        """
        获取labels
        """
        e_labels_records = self.query(
            """MATCH ()-[e]-() RETURN collect(distinct label(e)) as labels"""
        )
        return e_labels_records

    def refresh_schema(self) -> None:
        """
        刷新schema
        更新 labels, relationships, and properties
        """
        n_labels, e_labels = self._get_labels()
        triple_schema = self._get_triples(e_labels)
