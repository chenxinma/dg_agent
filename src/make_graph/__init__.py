"""
graph模块用户生成图数据库的内容
"""
from typing import Self


import time
import hashlib
from abc import ABC, abstractmethod

class ConceptModel(ABC):
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
    def load(xml_root) -> list | None:
        """读取设计稿"""

    @abstractmethod
    def get_nid(self) -> str:
        """节点id"""
        

def generate_unique_id(string: str):
    """生成唯一id"""
    hash_part = hashlib.sha256(string.encode()).hexdigest()[:16]
    return hash_part
