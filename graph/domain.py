from . import ConceptModel, generate_unique_id

class Domain(ConceptModel):
    def __init__(self, name, x, y, w, h, code):
        super().__init__(0, name, x, y, w, h)
        self._apps = []
        self.code = code

    @property
    def apps(self) -> list:
        return self._apps

    def contains(self, a):
        rs = a.x >= self.x and (a.x + a.w) <= (self.x + self.w) \
            and a.y >= self.y and (a.y + a.h) <= (self.y + self.h)
        return rs
    
    @staticmethod
    def load(xml_root) -> list:
        domain_list = []
        for d in xml_root.xpath('//object[contains(@label, "domain")]'):
            name = d.attrib['name']
            code = d.attrib['domain']
            shape = d.find('mxCell/mxGeometry')
            _domain = Domain(name, shape.attrib['x'], shape.attrib['y'],  
                            shape.attrib['width'], shape.attrib['height'], code)
            _domain.id = generate_unique_id(code)
            domain_list.append(_domain)
        return domain_list