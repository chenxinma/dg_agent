from . import ConceptModel, generate_unique_id

class Entity(ConceptModel):
    def __init__(self, id, name, x, y, w, h, gid):
        super().__init__(id, name, x, y, w, h)
        self._app = None
        self.gid = gid

    def __str__(self):
        return "Entity: " + self.name + \
                "(x: " + str(self.x) + \
                " y: " + str(self.y) + \
                " w: " + str(self.w) + \
                " h: " + str(self.h) + ")"

    def __repr__(self):
        return '<Entity id: %s, name: %s, x: %s, y: %s, w: %s, h: %s>' % \
            (self.id, self.name, self.x, self.y, self.w, self.h)

    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, a):
        self._app = a
        self.id = generate_unique_id("%s.%s"%(a.name, self.name))
    
    @staticmethod
    def load(xml_root) -> list:
        entity_list = []
        for node in xml_root.xpath('//mxCell[contains(@style, "rounded=1")]'):
            e_node = node
            if not 'value' in e_node.attrib:
                e_node = node.getparent()
                name = e_node.attrib['label']
                # print(etree.tostring(e_node))
            else:
                name = node.attrib['value']
            gid = e_node.attrib['id']
            shape = node.find('mxGeometry')
            _entity = Entity(gid, name, shape.attrib['x'], shape.attrib['y'], 
                            shape.attrib['width'], shape.attrib['height'],
                            gid)
            entity_list.append(_entity)

        return entity_list
        