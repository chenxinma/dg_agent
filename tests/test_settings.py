"""settings test"""
# pylint: disable=E0401
from bot.settings import settings

def test_01():
    """agents"""
    print(settings.get_setting("agents"))
    
def test_02():
    """age"""
    print(settings.get_setting("age.graph"))
