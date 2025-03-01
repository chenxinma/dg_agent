"""测试序列化结果，用于确认传递给大模型的文本形式"""
from bot.agent.metadata import BusinessDomain, Application, DataEntity

def test_01():
    """BusinessDomain"""
    d = BusinessDomain(id="1", name="财务", code="FIN", node="BusinessDomain")
    print(d)

def test_02():
    """Application"""
    app = Application(name="收付费", id="cdps", node="Applicatin")
    print(app)

def test_03():
    """DataEntity"""
    e = DataEntity(name="银行", id="1", node="DataEntity")
    print(e)
