import json
from typing import List, Union
import logfire



try:
    from .metadata import *
    from . import ( 
               CypherQuery, 
               DataGovResponse
            )
finally:
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
    

def create_factory_chain(graph_name:str)-> List[MetaFactory]: 
    meta_factories = [
        DomainMetaFactory(graph_name),
        EntityMetaFactory(graph_name),
        TableMetaFactory(graph_name),
        OtherMetaFactory(graph_name),
        LinkMetaFactory(graph_name)
    ]
    return meta_factories

def age_metadata_query(
        age_conn: age.Age,
        graph_name: str,
        meta_factories: List[MetaFactory],
        query: CypherQuery)-> DataGovResponse:
    """按照cypher脚本进行查询

    Args:
        age_conn: AGE数据库连接
        graph_name: AGE数据库名称
        query: ·cypher_query·给出的CypherQuery
    """

    _conn = age_conn.connection
    with logfire.span("Age Query"):
        with _conn.cursor() as _cursor:
            q = query.sql.replace("{GRAPH_NAME}", graph_name)
            logfire.info("query: %s " % (query.explanation.replace("{", "{{").replace("}", "}}")))
            logfire.info("query: %s " % (q.replace("{", "{{").replace("}", "}}")))
            _cursor.execute(q)            
            result = _cursor.fetchall()
            resp = DataGovResponse(description=query.explanation)
            for _r in result:
                row: List[Union[str, BaseModel]] = []
                for c in _r:
                    for factory in meta_factories:
                        if factory.fit(c):
                            d = factory.convert(c, _conn)
                            row.append(d)
                            break
                resp.add(row)
            logfire.info("result rows: %d " % (len(resp.contents)))
            return resp
        

class TableEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Table):
            return obj.model_dump()
        return super().default(obj)