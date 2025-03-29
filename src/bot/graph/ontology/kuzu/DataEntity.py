from .. import MetaObject

class DataEntity(MetaObject):
    """数据实体"""

    @classmethod
    def parse(cls, cell):
        return cls(id=f"{cell['_id']['offset']}:{cell['_id']['table']}", 
                name=cell['name'], 
                node=cell['_label'])

    @classmethod
    def fit(cls, cell) -> bool:
        return cell['_label'] == 'DataEntity'