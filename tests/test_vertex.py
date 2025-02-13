from bot.agent import Domain, Application, Entity

def test_01():
    d = Domain(id="1", name="财务", code="FIN", node="Domain")
    print(d)

def test_02():   
    app = Application(name="收付费", id="cdps", node="Applicatin")
    print(app)

def test_03():   
    e = Entity(name="银行", id="1", node="Entity")
    print(e)