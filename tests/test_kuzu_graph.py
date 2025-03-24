import pytest

# 假设kuzu_graph.py里有一个KuzuGraph类
from src.bot.graph.kuzu_graph import KuzuGraph
from src.bot.settings import settings

class TestKuzuGraph:
    def setup_method(self):
        """初始化"""
        database = settings.get_setting("kuzu.database")
        self.kuzu_graph = KuzuGraph(database)

    def test_query(self):
        """查询"""
        result = self.kuzu_graph.query("MATCH (n:BusinessDomain) RETURN n LIMIT 1")
        print(result)
        assert result[0]['n']['_label'] == "BusinessDomain"
