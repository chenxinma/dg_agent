"""
KuzuGraph class.
用于生成 Kuzu 图数据库的图谱结构
"""
from __future__ import annotations

from typing import Dict, List, Any

import kuzu
from .base_graph import BaseGraph

class KuzuQueryException(Exception):
    """Exception for the Kuzu queries."""

    def __init__(self, exception: str | dict[str, Any]) -> None:
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


class KuzuGraph(BaseGraph):
    """
    Kuzu 图数据库操作类
    """
    # python type mapping for providing readable types to LLM
    types = {
        "str": "STRING",
        "float": "DOUBLE",
        "int": "INTEGER",
        "list": "LIST",
        "dict": "MAP",
        "bool": "BOOLEAN",
    }

    def __init__(self, db_path: str) -> None:
        self.db_path: str = db_path
        self.db = kuzu.Database(db_path, read_only=True)
        self.conn = kuzu.Connection(self.db)
        self._schema:str = ""
        self.refresh_schema()

    def explain(self, query: str, params: Dict[str, Any] | None = None):
        """
        执行查询计划
        """
        try:
            if params is None:
                self.conn.execute(query)
            else:
                self.conn.execute(query, params)
        except Exception as e:
            raise KuzuQueryException(
                {
                    "message": f"Error executing graph query: {query}",
                    "detail": str(e),
                }
            ) from e

    def query(self, query: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
        """
        执行查询
        """
        try:
            if params is None:
                result = self.conn.execute(query)
            else:
                result = self.conn.execute(query, params)
            # 假设 Kuzu 的查询结果对象有类似获取列名的方法，这里需要根据实际的 Kuzu API 来调整
            # 以下代码假设 result 有一个正确的方法来获取列名
            if isinstance(result, kuzu.QueryResult):                
                # column_names = result.get_column_names()
                _df = result.get_as_df()
                return _df.to_dict(orient="records")
            return []  # 确保在所有代码路径上返回 List[Dict[str, Any]] 类型的值
        except Exception as e:
            raise KuzuQueryException(
                {
                    "message": f"Error executing graph query: {query}",
                    "detail": str(e),
                }
            ) from e

    def _wrap_name(self, name: str) -> str:
        """Wrap name with backticks."""
        if name in ['Column']:
            return f"`{name}`"
        return name

    def refresh_schema(self) -> None:
        """Refreshes the Kùzu graph schema information"""
        node_properties = []
        node_table_names = self.conn._get_node_table_names()
        for table_name in node_table_names:
            current_table_schema = {"properties": [], "label": self._wrap_name(table_name)}
            properties = self.conn._get_node_property_names(table_name)
            for property_name in properties:
                property_type = properties[property_name]["type"]
                list_type_flag = ""
                if properties[property_name]["dimension"] > 0:
                    if "shape" in properties[property_name]:
                        for s in properties[property_name]["shape"]:
                            list_type_flag += "[%s]" % s
                    else:
                        for i in range(properties[property_name]["dimension"]):
                            list_type_flag += "[]"
                property_type += list_type_flag
                current_table_schema["properties"].append(
                    (property_name, property_type)
                )
            node_properties.append(current_table_schema)

        relationships = []
        rel_tables = self.conn._get_rel_table_names()
        for table in rel_tables:
            relationships.append(
                "(:%s)-[:%s]->(:%s)" % (self._wrap_name(table["src"]), table["name"], self._wrap_name(table["dst"]))
            )

        rel_properties = []
        for table in rel_tables:
            table_name = self._wrap_name(table["name"])
            current_table_schema = {"properties": [], "label": table_name}
            query_result = self.conn.execute(
                f"CALL table_info('{table_name}') RETURN *;"
            )
            while query_result.has_next(): # pyright: ignore[reportAttributeAccessIssue]
                row = query_result.get_next()# pyright: ignore[reportAttributeAccessIssue]
                prop_name = row[1]
                prop_type = row[2]
                current_table_schema["properties"].append((prop_name, prop_type))
            rel_properties.append(current_table_schema)

        self._schema = (
            "## 图数据库结构:\n"
            f"节点：{node_properties}\n"
            f"关联: {rel_properties}\n"
            f"节点关联关系: {relationships}\n"
        )
    
    @property
    def schema(self) -> str:
        """Returns the schema of the Graph"""
        return self._schema
