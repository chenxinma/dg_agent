from .. import MetaObject

class Application(MetaObject):
    """应用程序"""

    @classmethod
    def parse(cls, cell):
        return cls(id=f"{cell['_id']['offset']}:{cell['_id']['table']}", 
        name=cell['name'], 
        node=cell['_label'])

    @classmethod
    def fit(cls, cell) -> bool:
        return cell['_label'] == 'Application'
