import pytest

# 假设kuzu_graph.py里有一个KuzuGraph类
from bot.graph.kuzu_graph import KuzuGraph
from bot.graph.ontology.kuzu import (
    BusinessDomain, 
    Application, 
    DataEntity, 
    PhysicalTable, 
    Column, 
    FlowsTo, 
    RelatedTo,
    MetadataHelper
    )
from bot.settings import settings

class TestKuzuGraph:
    @pytest.fixture(scope="class")
    def graph(self):
        """初始化"""
        database = settings.get_setting("kuzu.database")
        return KuzuGraph(database)
    
    def test_schema(self, graph):
        """获取schema"""
        schema = graph.schema
        print(schema)
        assert schema is not None

    def test_query(self, graph):
        """查询"""
        result = graph.query("MATCH (n:BusinessDomain) RETURN n LIMIT 1")
        print(result)
        assert result[0]['n']['_label'] == "BusinessDomain"

    def test_get_business_domain(self, graph):
        """获取业务域"""
        helper = MetadataHelper()
        result = helper.query("MATCH (n:BusinessDomain) RETURN n LIMIT 1", graph)
        print(result)
        assert isinstance(result[0][0], BusinessDomain)

    def test_get_application(self, graph):
        """获取应用程序"""
        helper = MetadataHelper()
        result = helper.query("MATCH (n:Application) RETURN n LIMIT  1", graph)
        print(result)
        assert isinstance(result[0][0], Application)

    def test_get_data_entity(self, graph):
        """获取数据实体"""
        helper = MetadataHelper()
        result = helper.query("MATCH (n:DataEntity) RETURN n LIMIT   1", graph)
        print(result)
        assert isinstance(result[0][0], DataEntity)

    def test_get_physical_table(self, graph):
        """获取物理表"""
        helper = MetadataHelper()
        result = helper.query("MATCH (n:PhysicalTable) RETURN n LIMIT  1", graph)
        print(result)
        assert isinstance(result[0][0], PhysicalTable)

    def test_get_column(self, graph):
        """获取列"""
        helper = MetadataHelper()
        result = helper.query("MATCH (c:`Column`) RETURN c LIMIT 1", graph)
        print(result)
        assert isinstance(result[0][0], Column)

    def test_get_flows_to(self, graph):
        """获取流向"""
        helper = MetadataHelper()
        result = helper.query("MATCH (a:DataEntity)-[r:FLOWS_TO]->(b:DataEntity) RETURN r LIMIT  1", graph)
        print(result)
        assert isinstance(result[0][0], FlowsTo)
    
    def test_get_related_to(self, graph):
        """获取相关"""
        helper = MetadataHelper()
        result = helper.query("MATCH (a:DataEntity)-[r:RELATED_TO]->(b:DataEntity) RETURN r LIMIT  1", graph)
        print(result)
        assert isinstance(result[0][0], RelatedTo)

    def test_path(self, graph):
        """创建业务域"""
        helper = MetadataHelper()
        result = \
            helper.query("""MATCH 
                (e1:DataEntity {name: '银行'})-[r:RELATED_TO*1..2]->(e2:DataEntity {name: '资金账户'})
                RETURN e1,r,e2""", graph)
        print(result)

    def test_count(self, graph):
        """创建业务域"""
        helper = MetadataHelper()
        result = \
            helper.query("""
            MATCH (d:BusinessDomain {name: '财务'})-[:CONTAINS]-(a:Application)
            RETURN count(a) AS application_count""", graph)
        print(result)

    def test_relate_tables(self, graph):
        """创建业务域"""
        helper = MetadataHelper()
        result = \
            helper.query("""
            MATCH (e1:DataEntity {name: '银行'})-[r]->(e2:DataEntity),
                (e1)-[:IMPLEMENTS]->(t1:PhysicalTable),
                (e2)-[:IMPLEMENTS]->(t2:PhysicalTable)
            RETURN e1, e2, r, t1, t2""", graph)
        print(result)
