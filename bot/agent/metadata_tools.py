"""
元模型 处理
"""
import json
from typing import List, Any, Dict

import age
import logfire

try:
    from .metadata import (MetaFactory,
                           PhysicalTable,
                           DataEntity,
                           BusinessDomain,
                           RelatedTo,
                           Application)
    from . import (
               CypherQuery,
               DataGovResponse
            )
finally:
    pass

from bot.graph.age_graph import AGEGraph

class BusinessDomainMetaFactory(MetaFactory):
    """业务领域元模型工厂类，用于处理BusinessDomain类型顶点"""

    def fit(self, cell)->bool:
        """判断单元格是否为BusinessDomain类型的顶点
        
        Args:
            cell: 待判断的单元（顶点或边）
            
        Returns:
            bool: 如果cell是BusinessDomain类型的顶点返回True，否则False
        """
        return isinstance(cell, age.models.Vertex) and  cell.label == "BusinessDomain"

    def convert(self, cell, graph:AGEGraph ):
        """将BusinessDomain顶点转换为业务领域元模型对象
        
        Args:
            cell: BusinessDomain顶点
            graph: AGEGraph实例
            
        Returns:
            BusinessDomain: 业务领域元模型对象
        """
        return BusinessDomain(id=cell.id,
                        name=cell.properties["name"],
                        node=cell.label,
                        code=cell.properties["code"])

class DataEntityMetaFactory(MetaFactory):
    """数据实体元模型工厂类，用于处理DataEntity类型顶点"""

    def fit(self, cell)->bool:
        """判断单元格是否为DataEntity类型的顶点
        
        Args:
            cell: 待判断的单元（顶点或边）
            
        Returns:
            bool: 如果cell是DataEntity类型的顶点返回True，否则False
        """
        return isinstance(cell, age.models.Vertex) and  cell.label == "DataEntity"

    def convert(self, cell, graph:AGEGraph):
        """将DataEntity顶点转换为数据实体元模型对象
        
        Args:
            cell: DataEntity顶点
            graph: AGEGraph实例
            
        Returns:
            DataEntity: 数据实体元模型对象
        """
        tables = self.load_tables(graph, cell.id)

        return DataEntity(id=cell.id,
                      name=cell.properties["name"],
                      node=cell.label,
                      tables=tables)

    def load_tables(self, graph:AGEGraph, entity_id:int):
        """加载数据实体对应的物理表
        
        Args:
            graph: AGEGraph实例
            entity_id: 数据实体ID
            
        Returns:
            List[PhysicalTable]: 物理表对象列表
        """
        tables = []
        tbl_factory = PhysicalTableMetaFactory()
        result = graph.query("""
                MATCH (e:DataEntity)-[:IMPLEMENTS]->(t:PhysicalTable) WHERE ID(e)=%s
                RETURN t
            """, (entity_id, ))

        for _r in result:
            t = _r[0]
            d = tbl_factory.convert(t, graph)
            tables.append(d)
        return tables

class PhysicalTableMetaFactory(MetaFactory):
    """物理表元模型工厂类，用于处理PhysicalTable类型顶点"""

    def fit(self, cell)->bool:
        """判断单元格是否为PhysicalTable类型的顶点
        
        Args:
            cell: 待判断的单元（顶点或边）
            
        Returns:
            bool: 如果cell是PhysicalTable类型的顶点返回True，否则False
        """
        return isinstance(cell, age.models.Vertex) and cell.label == "PhysicalTable"

    def convert(self, cell, graph:AGEGraph):
        """将PhysicalTable顶点转换为物理表元模型对象
        
        Args:
            cell: PhysicalTable顶点
            graph: AGEGraph实例
            
        Returns:
            PhysicalTable: 物理表元模型对象
        """
        columns = self.load_columns(graph, cell.properties["full_table_name"])
        return PhysicalTable(id=cell.id,
                      name=cell.properties["name"],
                      db_schema=cell.properties["schema"],
                      table_name=cell.properties["table_name"],
                      full_table_name=cell.properties["full_table_name"],
                      columns=columns,
                      node=cell.label)

    def load_columns(self, graph:AGEGraph, full_table_name:str) ->  List[Dict]:
        """加载物理表对应的列信息
        
        Args:
            graph: AGEGraph实例
            full_table_name: 物理表全名
            
        Returns:
            List[Dict]: 列信息字典列表，包含列名和数据类型
        """
        result = graph.query("""
                MATCH (t:PhysicalTable {full_table_name: %s})-[:HAS_COLUMN]->(c:Column)
                RETURN c
            """, (full_table_name, ))
        columns:List[Dict] = []

        for _r in result:
            col = _r[0]
            d = dict(name=col.properties["name"], dtype=col.properties["dtype"])
            columns.append(d)
        return columns

class OtherMetaFactory(MetaFactory):
    """其他元模型工厂类，用于处理未特殊处理的顶点类型"""

    def fit(self, cell)->bool:
        """判断单元格是否为顶点
        
        Args:
            cell: 待判断的单元（顶点或边）
            
        Returns:
            bool: 如果cell是顶点返回True，否则False
        """
        return isinstance(cell, age.models.Vertex)

    def convert(self, cell, graph:AGEGraph):
        """将顶点转换为对应的元模型对象
        
        Args:
            cell: 顶点
            graph: AGEGraph实例
            
        Returns:
            Any: 根据顶点类型动态创建的元模型对象
        """
        # 使用字典映射标签到类
        class_map = {
            "Application": Application,
            # 添加其他可能的标签和对应的类
        }

        cls = class_map.get(cell.label)
        if cls:
            return cls(id=cell.id, name=cell.properties["name"], node=cell.label)
        else:
            raise ValueError(f"Unknown vertex label: {cell.label}")

class RelatedToMetaFactory(MetaFactory):
    """关系元模型工厂类，用于处理RELATED_TO类型边"""

    def fit(self, cell)->bool:
        """判断单元格是否为RELATED_TO类型的边
        
        Args:
            cell: 待判断的单元（顶点或边）
            
        Returns:
            bool: 如果cell是RELATED_TO类型的边返回True，否则False
        """
        return isinstance(cell, age.models.Edge)  and  cell.label == "RELATED_TO"

    def convert(self, cell, graph:AGEGraph):
        """将RELATED_TO边转换为关系元模型对象
        
        Args:
            cell: RELATED_TO边
            graph: AGEGraph实例
            
        Returns:
            RelatedTo: 关系元模型对象
        """
        if "rel" in cell.properties:
            return RelatedTo(id=cell.id,
                             from_id=cell.start_id,
                             to_id=cell.end_id,
                             rel=cell.properties["rel"])
        return RelatedTo(id=cell.id, from_id=cell.start_id, to_id=cell.end_id)


class MetadataHelper:
    """元数据查询类，用于执行元数据查询并返回结果"""
    meta_factories:List[MetaFactory] = [
        BusinessDomainMetaFactory(),
        DataEntityMetaFactory(),
        PhysicalTableMetaFactory(),
        OtherMetaFactory(),
        RelatedToMetaFactory()
    ] # 元模型工厂列表

    def __init__(self, graph:AGEGraph):
        self.graph = graph

    def _convert_age2model(self, c:Any):
        """将AGE数据库类型转换为元模型对象
        
        Args:
            c: 待转换的AGE数据库对象（顶点或边）
        Returns:
            Any: 转换后的元模型对象
        """
        for factory in MetadataHelper.meta_factories:
            if factory.fit(c):
                d = factory.convert(c, self.graph)
                return d
        return c

    def _traverse_age_result(self, contents,
                        resp:DataGovResponse):
        """遍历AGE查询结果提取元模型对象
        
        Args:
            contents: AGE查询结果内容
            resp: DataGovResponse对象，用于存储转换后的元模型对象
        """
        for row in contents:
            _row = []
            for cell in row:
                if isinstance(cell, list):
                    for _c in cell:
                        d = self._convert_age2model(_c)
                        _row.append(d)
                else:
                    d = self._convert_age2model(cell)
                    _row.append(d)
            resp.contents.append(_row)


    def query(self, query: CypherQuery)-> DataGovResponse:
        """按照Cypher脚本进行AGE元数据查询
    
        Args:
            query: Cypher查询对象
            
        Returns:
            DataGovResponse: 包含查询结果的响应对象
        """

        with logfire.span("Age Query"):
            logfire.info("query: {explanation} {cypher}",
                        explanation=query.explanation,
                        cypher=query.cypher)
            result = self.graph.query(query.cypher)

            resp = DataGovResponse(description=query.explanation)
            self._traverse_age_result(result, resp)

            logfire.info("result contents: {cnt} ", cnt=len(resp.contents))
            return resp


class PhysicalTableEncoder(json.JSONEncoder):
    """物理表JSON编码器，用于将PhysicalTable对象编码为DDL语句"""

    def default(self, o):
        """将PhysicalTable对象编码为DDL语句
        
        Args:
            obj: 待编码对象
            
        Returns:
            str: 如果obj是PhysicalTable类型，返回其DDL语句；否则调用父类方法
        """
        if isinstance(o, PhysicalTable):
            # ddl = f"CREATE TABLE IF NOT EXISTS {o.full_table_name} ("
            # ddl += ',\n'.join([f"`{c['name']}` {c['dtype']}" for c in o.columns])
            # ddl += ");\n"
            # return ddl
            return o.model_dump()
        return super().default(o)
