"""AGEGraph class.
Apache AGE 操作工具类
用于生成图谱结构
借鉴了 langchain 的AGEGraph
langchain/libs/community/langchain_community/graphs/age_graph.py
"""
from __future__ import annotations

import re
import json

from typing import Dict, List, Tuple, Union, Sequence, Any

import age


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
    # python type mapping for providing readable types to LLM
    types = {
        "str": "STRING",
        "float": "DOUBLE",
        "int": "INTEGER",
        "list": "LIST",
        "dict": "MAP",
        "bool": "BOOLEAN",
    }

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

    @staticmethod
    def _get_col_name(field: str, idx: int) -> str:
        """
        Convert a cypher return field to a pgsql select field
        If possible keep the cypher column name, but create a generic name if necessary

        Args:
            field (str): a return field from a cypher query to be formatted for pgsql
            idx (int): the position of the field in the return statement

        Returns:
            str: the field to be used in the pgsql select statement
        """
        # remove white space
        field = field.strip()
        # if an alias is provided for the field, use it
        if " as " in field:
            return field.split(" as ")[-1].strip()
        # if the return value is an unnamed primitive, give it a generic name
        elif field.isnumeric() or field in ("true", "false", "null"):
            return f"column_{idx}"
        # otherwise return the value stripping out some common special chars
        else:
            return field.replace("(", "_").replace(")", "")

    @staticmethod
    def _wrap_query(query: str, graph_name: str) -> str:
        """
        Convert a Cyper query to an Apache Age compatible Sql Query.
        Handles combined queries with UNION/EXCEPT operators

        Args:
            query (str) : A valid cypher query, can include UNION/EXCEPT operators
            graph_name (str) : The name of the graph to query

        Returns :
            str : An equivalent pgSql query wrapped with ag_catalog.cypher

        Raises:
            ValueError : If query is empty, contain RETURN *, or has invalid field names
        """

        if not query.strip():
            raise ValueError("Empty query provided")

        # pgsql template
        template = """SELECT {projection} FROM ag_catalog.cypher('{graph_name}', $$
            {query}
        $$) AS ({fields});"""

        # split the query into parts based on UNION and EXCEPT
        parts = re.split(r"\b(UNION\b|\bEXCEPT)\b", query, flags=re.IGNORECASE)

        all_fields = []

        for part in parts:
            if part.strip().upper() in ("UNION", "EXCEPT"):
                continue

            # if there are any returned fields they must be added to the pgsql query
            return_match = re.search(r'\breturn\b(?![^"]*")', part, re.IGNORECASE)
            if return_match:
                # Extract the part of the query after the RETURN keyword
                return_clause = part[return_match.end() :]

                # parse return statement to identify returned fields
                fields = (
                    return_clause.lower()
                    .split("distinct")[-1]
                    .split("order by")[0]
                    .split("skip")[0]
                    .split("limit")[0]
                    .split(",")
                )

                # raise exception if RETURN * is found as we can't resolve the fields
                clean_fileds = [f.strip() for f in fields if f.strip()]
                if "*" in clean_fileds:
                    raise ValueError(
                        "Apache Age does not support RETURN * in Cypher queries"
                    )

                # Format fields and maintain order of appearance
                for idx, field in enumerate(clean_fileds):
                    field_name = AGEGraph._get_col_name(field, idx)
                    if field_name not in all_fields:
                        all_fields.append(field_name)

        # if no return statements found in any part
        if not all_fields:
            fields_str = "a agtype"

        else:
            fields_str = ", ".join(f"{field} agtype" for field in all_fields)

        return template.format(
            graph_name=graph_name,
            query=query,
            fields=fields_str,
            projection="*",
        )

    def query(self, query: str, params:Sequence=None) -> List[Dict[str, Any]]:
        """
        执行查询
        """
        _wrap_query = AGEGraph._wrap_query(query, self.graph_name)

        # execute the query, rolling back on an error
        with self.age.connection.cursor() as curs:
            try:
                if params is None:
                    curs.execute(_wrap_query)
                else:
                    curs.execute(_wrap_query, params)
            except age.SqlExecutionError as e:
                self.age.rollback()
                raise AGEQueryException(
                    {
                        "message": f"Error executing graph query: {query}",
                        "detail": str(e),
                    }
                ) from e

            return curs.fetchall()

    def explain(self, query: str):
        """
        执行查询计划 验证SQL
        """
        _wrap_query = "EXPLAIN " + AGEGraph._wrap_query(query, self.graph_name)
        with self.age.connection.cursor() as curs:
            curs.execute(_wrap_query)

    def _get_labels(self) -> Tuple[List[str], List[str]]:
        """
        获取labels
        """
        e_labels_records = self.query(
            """MATCH ()-[e]-() RETURN collect(distinct label(e)) as labels"""
        )
        e_labels = e_labels_records[0]["labels"] if e_labels_records else []

        n_labels_records = self.query(
            """MATCH (n) RETURN collect(distinct label(n)) as labels"""
        )
        n_labels = n_labels_records[0]["labels"] if n_labels_records else []

        return n_labels, e_labels

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

        # age query to get distinct relationship types
        triple_query = """
        SELECT * FROM ag_catalog.cypher('{graph_name}', $$
            MATCH (a)-[e:`{e_label}`]->(b)
            WITH a,e,b LIMIT 3000
            RETURN DISTINCT labels(a) AS from, type(e) AS edge, labels(b) AS to
            LIMIT 10
        $$) AS (f agtype, edge agtype, t agtype);
        """

        triple_schema = []

        # iterate desired edge types and add distinct relationship types to result
        with self.age.connection.cursor() as curs:
            for label in e_labels:
                q = triple_query.format(graph_name=self.graph_name, e_label=label)
                try:
                    curs.execute(q)
                    data = curs.fetchall()
                    for d in data:
                        # use json.loads to convert returned
                        # strings to python primitives
                        triple_schema.append(
                            {
                                "start": json.loads(d.f)[0],
                                "type": json.loads(d.edge),
                                "end": json.loads(d.t)[0],
                            }
                        )
                except age.SqlExecutionError as e:
                    raise AGEQueryException(
                        {
                            "message": "Error fetching triples",
                            "detail": str(e),
                        }
                    ) from e

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

        # cypher query to fetch properties of a given label
        node_properties_query = """
        SELECT * FROM ag_catalog.cypher('{graph_name}', $$
            MATCH (a:`{n_label}`)
            RETURN properties(a) AS props
            LIMIT 100
        $$) AS (props agtype);
        """

        node_properties = []
        with self.age.connection.cursor() as curs:
            for label in n_labels:
                q = node_properties_query.format(
                    graph_name=self.graph_name, n_label=label
                )

                try:
                    curs.execute(q)
                except age.SqlExecutionError as e:
                    raise AGEQueryException(
                        {
                            "message": "Error fetching node properties",
                            "detail": str(e),
                        }
                    ) from e
                data = curs.fetchall()

                # build a set of distinct properties
                s = set({})
                for d in data:
                    # use json.loads to convert to python
                    # primitive and get readable type
                    for k, v in json.loads(d.props).items():
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
        # cypher query to fetch properties of a given label
        edge_properties_query = """
        SELECT * FROM ag_catalog.cypher('{graph_name}', $$
            MATCH ()-[e:`{e_label}`]->()
            RETURN properties(e) AS props
            LIMIT 100
        $$) AS (props agtype);
        """
        edge_properties = []
        with self.age.connection.cursor() as curs:
            for label in e_labels:
                q = edge_properties_query.format(
                    graph_name=self.graph_name, e_label=label
                )

                try:
                    curs.execute(q)
                except age.SqlExecutionError as e:
                    raise AGEQueryException(
                        {
                            "message": "Error fetching edge properties",
                            "detail": str(e),
                        }
                    ) from e
                data = curs.fetchall()

                # build a set of distinct properties
                s = set({})
                for d in data:
                    # use json.loads to convert to python
                    # primitive and get readable type
                    for k, v in json.loads(d.props).items():
                        s.add((k, self.types[type(v).__name__]))

                np = {
                    "properties": [{"property": k, "type": v} for k, v in s],
                    "type": label,
                }
                edge_properties.append(np)

        return edge_properties
    def refresh_schema(self) -> None:
        """
        刷新schema
        更新 labels, relationships, and properties
        """
        n_labels, e_labels = self._get_labels()
        triple_schema = self._get_triples(e_labels)

        node_properties = self._get_node_properties(n_labels)
        edge_properties = self._get_edge_properties(e_labels)

        # 生成图谱结构描述
        self.schema = f"""
        ## 图数据库结构:
        ### 节属性：
        {node_properties}
        ### 关联属性:
        {edge_properties}
        节点关联关系:
        {AGEGraph._format_triples(triple_schema)}
        """

        self.structured_schema = {
            "node_props": {el["labels"]: el["properties"] for el in node_properties},
            "rel_props": {el["type"]: el["properties"] for el in edge_properties},
            "relationships": triple_schema,
            "metadata": {},
        }

    @property
    def get_schema(self) -> str:
        """Returns the schema of the Graph"""
        return self.schema

    @property
    def get_structured_schema(self) -> Dict[str, Any]:
        """Returns the structured schema of the Graph"""
        return self.structured_schema
