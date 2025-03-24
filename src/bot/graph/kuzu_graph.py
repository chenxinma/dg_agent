"""
KuzuGraph class.
用于生成 Kuzu 图数据库的图谱结构
"""
from __future__ import annotations

import re
from typing import Dict, List, Tuple, Union, Sequence, Any

from age.builder import DFA
import kuzu
import logfire

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
        self.db = kuzu.Database(db_path)
        self.conn = kuzu.Connection(self.db)
        self.refresh_schema()

    @staticmethod
    def _format_triples(triples: List[Dict[str, str]]) -> List[str]:
        """
        Convert a list of relationships from dictionaries to formatted strings
        to be better readable by an llm

        Args:
            triples (List[Dict[str,str]]): a list relationships in the form
                {'start':<from_label>, 'type':<edge_label>, 'end':<from_label>}

        Returns:
            List[str]: a list of relationships in the form
                "(:`<from_label>`)-[:`<edge_label>`]->(:`<to_label>`)"
        """
        triple_template = "(:`{start}`)-[:`{type}`]->(:`{end}`)"
        triple_schema = [triple_template.format(**triple) for triple in triples]

        return triple_schema

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


    def _get_labels(self) -> Tuple[List[str], List[str]]:
        """
        获取 labels
        """
        # 这里需要根据 Kuzu 的 API 来实现获取节点和边的标签
        # 示例代码，需要根据实际情况修改
        node_labels = []
        edge_labels = []
        return node_labels, edge_labels

    def _get_triples(self, e_labels: List[str]) -> List[Dict[str, str]]:
        """
        Get a set of distinct relationship types (as a list of dicts) in the graph
        to be used as context by an llm.

        Args:
            e_labels (List[str]): a list of edge labels to filter for

        Returns:
            List[Dict[str, str]]: relationships as a list of dicts in the format
                "{'start':<from_label>, 'type':<edge_label>, 'end':<from_label>}"
        """
        triple_schema = []
        for label in e_labels:
            # 这里需要根据 Kuzu 的 API 来实现查询关系类型
            # 示例代码，需要根据实际情况修改
            query = f"MATCH (a)-[e:{label}]->(b) RETURN labels(a) AS from, type(e) AS edge, labels(b) AS to"
            result = self.query(query)
            for row in result:
                # Bug 修复：确保字典正确添加到 triple_schema 列表中
                triple_schema.append(
                    {
                        "start": row["from"][0],
                        "type": row["edge"],
                        "end": row["to"][0],
                    }
                )
        return triple_schema

    def _get_node_properties(self, n_labels: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch a list of available node properties by node label to be used
        as context for an llm

        Args:
            n_labels (List[str]): a list of node labels to filter for

        Returns:
            List[Dict[str, Any]]: a list of node labels and
                their corresponding properties in the form
                "{
                    'labels': <node_label>,
                    'properties': [
                        {
                            'property': <property_name>,
                            'type': <property_type>
                        },...
                        ]
                }"
        """
        node_properties = []
        for label in n_labels:
            # 这里需要根据 Kuzu 的 API 来实现查询节点属性
            # 示例代码，需要根据实际情况修改
            query = f"MATCH (a:{label}) RETURN properties(a) AS props"
            result = self.query(query)
            s = set()
            for row in result:
                for k, v in row["props"].items():
                    s.add((k, self.types[type(v).__name__]))
            np = {
                "properties": [{"property": k, "type": v} for k, v in s],
                "labels": label,
            }
            node_properties.append(np)
        return node_properties

    def _get_edge_properties(self, e_labels: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch a list of available edge properties by edge label to be used
        as context for an llm

        Args:
            e_labels (List[str]): a list of edge labels to filter for

        Returns:
            List[Dict[str, Any]]: a list of edge labels
                and their corresponding properties in the form
                "{
                    'labels': <edge_label>,
                    'properties': [
                        {
                            'property': <property_name>,
                            'type': <property_type>
                        },...
                        ]
                }"
        """
        edge_properties = []
        for label in e_labels:
            # 这里需要根据 Kuzu 的 API 来实现查询边属性
            # 示例代码，需要根据实际情况修改
            query = f"MATCH ()-[e:{label}]->() RETURN properties(e) AS props"
            result = self.query(query)
            s = set()
            for row in result:
                for k, v in row["props"].items():
                    s.add((k, self.types[type(v).__name__]))
            np = {
                "properties": [{"property": k, "type": v} for k, v in s],
                "type": label,
            }
            edge_properties.append(np)
        return edge_properties

    def refresh_schema(self) -> None:
        """
        刷新 schema
        更新 labels, relationships, and properties
        """
        n_labels, e_labels = self._get_labels()
        triple_schema = self._get_triples(e_labels)

        # node_properties = self._get_node_properties(n_labels)
        edge_properties = self._get_edge_properties(e_labels)

        # 生成图谱结构描述
        self._schema : str = f"""
## 图数据库结构:
### 节点：
{n_labels}
### 关联:
{e_labels}
{edge_properties}
## 节点关联关系:
{KuzuGraph._format_triples(triple_schema)}"""
        logfire.info("Refresh schema completed.")

    @property
    def schema(self) -> str:
        """Returns the schema of the Graph"""
        return self._schema

    # @property
    # def structured_schema(self) -> Dict[str, Any]:
    #     """Returns the structured schema of the Graph"""
    #     return self._structured_schema

