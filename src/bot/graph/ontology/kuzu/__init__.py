from typing import Any
from cachetools import TTLCache, cachedmethod

from .Application import Application
from .BusinessDomain import BusinessDomain
from .BusinessTerm  import BusinessTerm
from .Column import Column
from .DataEntity import DataEntity
from .FlowsTo import FlowsTo
from .PhysicalTable import PhysicalTable
from .RelatedTo import RelatedTo
from .. import MetaObject
from .ExampleCypher import EXAMPLES

from bot.graph.kuzu_graph import KuzuGraph

class Others(MetaObject):
    @classmethod
    def parse(cls, cell):
        return str(cell)
    
    @classmethod
    def fit(cls, cell) -> bool:
        return True

_meta_factories:dict[str, type] = {
    "BusinessTerm": BusinessTerm,
    "BusinessDomain": BusinessDomain,
    "Application": Application,
    "DataEntity": DataEntity,
    "PhysicalTable": PhysicalTable,
    "Column": Column,
    "FLOWS_TO": FlowsTo,
    "RELATED_TO": RelatedTo,
}

def _age_obj_key(_, c:Any, _g:Any) -> int:
    if isinstance(c, dict):
        return hash(f"{c['_label']}:{c['_id']['offset']}:{c['_id']['table']}")
    return hash(c)

meta_obj_cache = TTLCache(maxsize=200, ttl=3000)

class MetadataHelper:
    def _load_columns(self, table:PhysicalTable, graph:KuzuGraph):
        """加载物理表的列信息
        """
        cypher = f"""
            MATCH (t:PhysicalTable {{full_table_name: '{table.full_table_name}'}})-[:HAS_COLUMN]->(c:`Column`) 
            RETURN c"""
        columns = graph.query(cypher)
        table.columns.extend([Column.parse(c['c']) for c in columns])
    
    @cachedmethod(lambda _: meta_obj_cache, key=_age_obj_key)
    def _parse_kuzu2model(self, c:Any, graph:KuzuGraph):
        """将kuzu数据库类型转换为元模型对象
        
        Args:
            c: 待转换的AGE数据库对象（顶点或边）
        Returns:
            Any: 转换后的元模型对象
        """
        cls = _meta_factories.get(c['_label'], Others)
        obj = cls.parse(c)
        if cls is PhysicalTable:
            self._load_columns(obj, graph)
        return obj

    def _traverse_age_result(self,
                             contents,
                             metaobj_list:list,
                             graph:KuzuGraph):
        """遍历kuzu查询结果提取元模型对象
        
        Args:
            contents: kuzu查询结果内容
            resp: DataGovResponse对象，用于存储转换后的元模型对象
        """
        for row in contents:
            _row = []
            for cell in row.values():
                if not isinstance(cell, dict):
                    _row.append(cell)
                    continue
                if '_nodes' in cell:
                    for _r in cell['_nodes']:
                        d = self._parse_kuzu2model(_r, graph)
                        _row.append(d)
                if '_rels' in cell:
                    for _r in cell['_rels']:
                        d = self._parse_kuzu2model(_r, graph)
                        _row.append(d)
                elif '_label' in cell:
                    d = self._parse_kuzu2model(cell, graph)
                    _row.append(d)
            metaobj_list.append(_row)
    
    def query(self, cypher:str, graph:KuzuGraph)-> list:
        """按照Cypher脚本进行AGE元数据查询
    
        Args:
            query: Cypher
            
        Returns:
            list: 包含查询结果的响应对象
        """
        result = graph.query(cypher)

        collect_metaobjs = []
        self._traverse_age_result(result, collect_metaobjs, graph)
        return collect_metaobjs
