from .. import MetaObject

class DataEntity(MetaObject):
    """数据实体"""

    @classmethod
    def parse(cls, cell):
        return cls(id=f"{cell['id']}", 
                name=cell['name'], 
                node=cell['label'])

    @classmethod
    def fit(cls, cell) -> bool:
        return cell['label'] == 'DataEntity'