"""MetadataHelper tests"""

from typing import List

import logfire
from bot.graph.age_graph import AGEGraph
from bot.agent.metadata import DataEntity, PhysicalTable, RelatedTo
from bot.agent.metadata_tools import MetadataHelper
from bot.agent import CypherQuery
from bot.settings import settings

# 配置日志
logfire.configure(environment='local', send_to_logfire=False)

class TestMetadataHelper:
    """MetadataHelper测试"""
    metadata_helper:MetadataHelper

    def setup_method(self):
        """初始化"""
        g_name = settings.get_setting("age")["graph"]
        dsn = settings.get_setting("age")["dsn"]

        age_graph = AGEGraph(graph_name=g_name, dsn=dsn)
        self.metadata_helper = MetadataHelper(age_graph)

    def test_query(self):
        """测试query"""
        cypher = """
        MATCH 
            (e1:DataEntity {name: '资金账户收入流水'})-[r:RELATED_TO*1..3]-(e2:DataEntity {name: '客户账单'})
            RETURN e1, r, e2
        """
        query = CypherQuery(cypher=cypher, explanation="测试")
        resp = self.metadata_helper.query(query)
        self.collect_table_defines(resp["contents"])
        assert resp["contents"] is not None

    def collect_table_defines(self,
                              contents: List | DataEntity | PhysicalTable | RelatedTo) -> None:
        """收集表定义
        """
        if isinstance(contents, list):
            for element in contents:
                self.collect_table_defines(element)
        else:
            if isinstance(contents, DataEntity):
                print("DataEntity", contents)
            if isinstance(contents, PhysicalTable):
                print("PhysicalTable", contents)
            if isinstance(contents, RelatedTo):
                print("RelatedTo", contents)
