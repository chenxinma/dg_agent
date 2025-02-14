import age
from pydantic import BaseModel, Field
from dataclasses import dataclass
from typing import List, Dict

class AGEVertex(BaseModel):
    id: int
    name: str = Field(description="名称")
    node: str = Field(description="节点类型")
    
    def __eq__(self, other):
        if isinstance(other, AGEVertex):
            return self.id == other.id
        return False
    
class Application(AGEVertex):
    """应用程序"""
    pass

class Domain(AGEVertex):
    """业务域"""
    code: str = Field(description="业务域代码")
        
 
class Entity(AGEVertex):
    """数据实体"""
    pass

class Table(AGEVertex):
    """物理表"""
    db_schema: str = Field(description="schema")
    full_table_name: str = Field(description="完整表名")
    table_name: str = Field(description="表名")
    columns: List[Dict] = Field(default=None)

class Entity(AGEVertex):
    """数据实体"""
    tables: List[Table] = []
    
@dataclass
class Link:
    from_id:int
    to_id:int
    id:int
    
@dataclass(init=False)
class MetaFactory:
    def __init__(self, graph_name:str):
        self.graph_name = graph_name
    def fit(self, cell)->bool:
        pass

    def convert(self, cell, conn):
        pass

class DomainMetaFactory(MetaFactory):
    def fit(self, cell)->bool:
        return isinstance(cell, age.models.Vertex) and  cell.label == "Domain"

    def convert(self, cell, conn):
        return Domain(id=cell.id, 
                        name=cell.properties["name"], 
                        node=cell.label,
                        code=cell.properties["code"])

class EntityMetaFactory(MetaFactory):
    def fit(self, cell)->bool:
        return isinstance(cell, age.models.Vertex) and  cell.label == "Entity"

    def convert(self, cell, conn):
        tables = self.load_tables(conn, cell.id)
        
        return Entity(id=cell.id, 
                      name=cell.properties["name"], 
                      node=cell.label,
                      tables=tables)

    def load_tables(self, conn, entity_id:int):
        tables = []
        tbl_factory = TableMetaFactory(self.graph_name)
        with conn.cursor() as _cursor:
            _cursor.execute("""SELECT * FROM cypher(%s, $$
                MATCH (e:Entity)-[:DEFINE]->(t:Table) WHERE ID(e)=%s
                RETURN t
            $$) AS (t agtype);
            """, (self.graph_name, entity_id))
            result = _cursor.fetchall()
            for _r in result:
                t = _r[0]
                d = tbl_factory.convert(t, conn)
                tables.append(d)
        return tables

class TableMetaFactory(MetaFactory):
    def fit(self, cell)->bool:
        return isinstance(cell, age.models.Vertex) and  cell.label == "Table"

    def convert(self, cell, conn):
        columns = self.load_columns(conn, cell.properties["full_table_name"])
        return Table(id=cell.id, 
                      name=cell.properties["name"],
                      db_schema=cell.properties["schema"],
                      table_name=cell.properties["table_name"],
                      full_table_name=cell.properties["full_table_name"],
                      columns=columns,
                      node=cell.label)

    def load_columns(self, conn, full_table_name:str) ->  List[Dict]:
        with conn.cursor() as _cursor:
            _cursor.execute("""SELECT * FROM cypher(%s, $$
                MATCH (t:Table {full_table_name: %s})-[:BELONG]-(c:Column)
                RETURN c
            $$) AS (c agtype);
            """, (self.graph_name, full_table_name))
            columns:List[Dict] = []
            result = _cursor.fetchall()
            for _r in result:
                col = _r[0]
                d = dict(name=col.properties["name"], dtype=col.properties["dtype"])
                columns.append(d)
            return columns

class OtherMetaFactory(MetaFactory):
    def fit(self, cell)->bool:
        return isinstance(cell, age.models.Vertex)

    def convert(self, cell, conn):
        return eval("{}(id='{}', name='{}', node='{}')".format(cell.label, 
                    cell.id, 
                    cell.properties["name"],
                    cell.label))

class LinkMetaFactory(MetaFactory):
    def fit(self, cell)->bool:
        return isinstance(cell, age.models.Edge)  and  cell.label == "LINK" 

    def convert(self, cell, conn):
        return Link(id=cell.id, from_id=cell.start_id, to_id=cell.end_id)