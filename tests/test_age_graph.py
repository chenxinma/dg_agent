"""AGEGraph tests"""
import pytest

# import age
from bot.graph.age_graph import AGEGraph, AGEQueryException

from bot.settings import settings

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
