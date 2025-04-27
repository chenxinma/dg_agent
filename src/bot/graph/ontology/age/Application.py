from .. import MetaObject

class Application(MetaObject):
    """应用程序"""

    @classmethod
    def parse(cls, cell):
        return cls(id=f"{cell['id']}", 
        name=cell['name'], 
        node=cell['label'])

    @classmethod
    def fit(cls, cell) -> bool:
        return cell['label'] == 'Application'
