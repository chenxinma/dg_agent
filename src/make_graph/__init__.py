"""
graph模块用户生成图数据库的内容
"""
import hashlib
from bot.settings import settings

GRAPH_NAME = settings.get_setting("age")["graph"]
DSN = settings.get_setting("age")["dsn"]

class ConceptModel:
    """概念模型"""
    # pylint: disable=W0622
    def __init__(self, id, name, x, y, w, h):
        self.name = name
        self.id = id
        self.x = float(x)
        self.y = float(y)
        self.w = float(w)
        self.h = float(h)

    @staticmethod
    def load(xml_root) -> list:
        """读取设计稿"""

def generate_unique_id(name):
    """生成唯一id"""
    return hashlib.sha256(name.encode()).hexdigest()
