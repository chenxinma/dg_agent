from . import ConceptModel, generate_unique_id
from .domain import Domain
from .dataentity import DataEntity

class Application(ConceptModel):
    def __init__(self, id, name, x, y, w, h):
        super().__init__(id, name, x, y, w, h)
        self._entities = []
        self._domain = None

    def __str__(self):
        return "App: " + self.name + \
                "\tid: " + self.id + \
                "\t(x: " + str(self.x) + \
                " y: " + str(self.y) + \
                " w: " + str(self.w) + \
                " h: " + str(self.h) + ")"

    def __repr__(self):
        return '<App id: %s, entities: %s>' % \
            (self.id, len(self._entities))

    @property
    def entities(self):
        return self._entities

    @property
    def domain(self) -> Domain | None:
        # 修改返回类型注解以匹配实际返回值，允许返回 None
        return self._domain

    @domain.setter
    def domain(self, d:Domain):
        self._domain = d

    def get_nid(self) -> str:
        return generate_unique_id(self.name)

    def contains(self, e:DataEntity):
        rs = e.x >= self.x and (e.x + e.w) <= (self.x + self.w) \
            and e.y >= self.y and (e.y + e.h) <= (self.y + self.h)
        return rs
    
    @staticmethod
    def load(xml_root) -> list:
        app_list = []
        for node in xml_root.xpath('//UserObject'):
            if not 'app' in node.attrib:
                continue
            shape = node.find('mxCell/mxGeometry')
            _app = Application(node.attrib['app'], node.attrib['full_name'], shape.attrib['x'], 
                    shape.attrib['y'], shape.attrib['width'], shape.attrib['height'])
            app_list.append(_app)
            
        return app_list