from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Any, Sequence

class BaseGraph(ABC):
    # python type mapping for providing readable types to LLM
    types = {
        "str": "STRING",
        "float": "DOUBLE",
        "int": "INTEGER",
        "list": "LIST",
        "dict": "MAP",
        "bool": "BOOLEAN",
    }

    @property
    @abstractmethod
    def schema(self) -> str:
        """获取schema"""
        ...

    @abstractmethod
    def query(self, query: str, params: Sequence | Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
        """执行查询"""
        pass

    @abstractmethod
    def refresh_schema(self) -> None:
        """刷新schema"""
        pass

    @staticmethod
    def _format_triples(triples: List[Dict[str, str]]) -> List[str]:
        """
        Convert a list of relationships from dictionaries to formatted strings
        to be better readable by an llm
        """
        triple_template = "(:`{start}`)-[:`{type}`]->(:`{end}`)"
        triple_schema = [triple_template.format(**triple) for triple in triples]
        return triple_schema

class BaseMetadataHelper(ABC):
    @abstractmethod
    def query(self, cypher:str, graph:BaseGraph)-> list:
        """执行查询"""
        pass
