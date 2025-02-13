from bot.settings import settings

def test_01():
    print(settings.get_setting("agents"))
    
def test_02():
    print(settings.get_setting("age")["graph"])
