import hashlib

GRAPH_NAME = "fsg_model"
DSN = "host=172.16.149.14 port=32718 dbname=postgres user=admin password=pass1234"

class ConceptModel:
    def __init__(self, id, name, x, y, w, h):
        self.name = name
        self.id = id
        self.x = float(x)
        self.y = float(y)
        self.w = float(w)
        self.h = float(h)

    @staticmethod
    def load(xml_root) -> list:
        pass
    
    
def generate_unique_id(name):
    return hashlib.sha256(name.encode()).hexdigest()
