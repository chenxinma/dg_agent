"""AGEGraph tests"""
import pytest

import logfire
from bot.graph.age_graph import AGEGraph, AGEQueryException

from bot.settings import settings

# 配置日志
logfire.configure(environment='local', send_to_logfire=False)

class TestAGEGraph:
    """AGEGraph tests"""
    age_graph:AGEGraph

    def setup_method(self):
        """初始化"""
        g_name = settings.get_setting("age")["graph"]
        dsn = settings.get_setting("age")["dsn"]

        self.age_graph = AGEGraph(graph_name=g_name, dsn=dsn)


    def test_query(self):
        """查询"""
        result = self.age_graph.query("MATCH (n:BusinessDomain) RETURN n LIMIT 1")
        assert result[0][0].label == "BusinessDomain"

    def test_query_with_params(self):
        """参数查询"""
        result = self.age_graph.query("MATCH (n:BusinessDomain {name:%s}) RETURN n",
                                      params=("财务", ))
        assert result[0][0].properties["name"] == "财务"

    def test_query_with_error(self):
        """错误查询"""
        with pytest.raises(AGEQueryException,
                           match="Error executing graph query: MATCH (n) RETURN n"):
            self.age_graph.query("MATCH (n) RETURN n")

    def test_explain(self):
        """执行计划"""
        self.age_graph.explain("MATCH (n) RETURN n")


    def test_get_relates(self):
        """获取relate_to"""
        result = self.age_graph.query(
            """MATCH 
            (e1:DataEntity {name: '资金账户收入流水'})-[r:RELATED_TO*1..3]-(e2:DataEntity {name: '客户账单'})
            RETURN e1, r, e2""")
        logfire.info("result: {r}", r=result)
        assert len(result) == 1
