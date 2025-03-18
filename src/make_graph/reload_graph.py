"""
元模型导入图数据库
"""
from io import StringIO

import click
import age
from lxml import etree
from tqdm import tqdm

from .domain import Domain
from .entity import Entity
from .application import Application
from . import GRAPH_NAME, DSN

class ConceptSturture:
    """构建元模型 业务域、应用、数据实体"""
    def __init__(self, xml_root):
        self._domain_list = Domain.load(xml_root)
        self._app_list = Application.load(xml_root)
        self._entity_list = Entity.load(xml_root)
        self._links = []

        for _app in self._app_list:
            for _domain in self._domain_list:
                if _domain.contains(_app):
                    _domain.apps.append(_app)
                    _app.domain = _domain

        for _entity in self._entity_list:
            for _app in self._app_list:
                if _app.contains(_entity):
                    _app.entities.append(_entity)
                    _entity.app = _app

        self._dup = {}
        for _e1 in self._entity_list:
            for _e2 in self._entity_list:
                if _e1.name == _e2.name and _e1.app != _e2.app:
                    ids = sorted([_e1.id, _e2.id])
                    _key = str(ids)
                    if  _key not in self._dup:
                        self._dup[_key] = (_e1, _e2)

    def link_entities(self, xml_root):
        """数据实体间的关联关系"""
        _gids = [ e.gid for e in self._entity_list ]
        _entities = dict(zip(_gids, self._entity_list))

        skips = ["x4Fgb3jFKm9Y57xnfF53-16"]
        for _node in xml_root.xpath('//mxCell[contains(@style, "endArrow")]'):
            style = _node.attrib['style']
            if "edgeStyle=entityRelationEdgeStyle" in style \
                or "endArrow=ER" in style:
                try:
                    _source = ""
                    _target = ""
                    _source = _node.attrib['source']
                    _target = _node.attrib['target']
                    if _source in skips or _target in skips:
                        continue
                    self._links.append((_entities[_source], _entities[_target]))
                except (KeyError, AttributeError) as ex:
                    print(_node.attrib['id'], _source, _target, ex)

    def make_graph(self, ag):
        """写入图数据库"""
        print("    Saving BusinessDomain list")
        conn_1 = ag.connection
        with conn_1.cursor() as _cursor:
            for _domain in self._domain_list:
                _cursor.execute('''SELECT * from cypher(%s, $$
                  CREATE (n:BusinessDomain {name: %s, nid: %s, code: %s}) 
                $$) as (v agtype); ''', (GRAPH_NAME, _domain.name, _domain.id, _domain.code))

        print("    Saving app_list")
        with conn_1.cursor() as _cursor:
            for _app in self._app_list:
                _cursor.execute('''SELECT * from cypher(%s, $$
                  CREATE (n:Application {name: %s, nid: %s}) 
                $$) as (v agtype); ''', (GRAPH_NAME, _app.name, _app.id))

        print("    Saving domain-app belong")
        with conn_1.cursor() as _cursor:
            for _app in self._app_list:
                _cursor.execute('''SELECT * from cypher(%s, $$
                  MATCH (a:Application), (d:BusinessDomain)
                  WHERE a.nid = %s AND d.nid = %s
                  CREATE (d)-[e:CONTAINS]->(a)
                  RETURN e
                $$) as (e agtype); ''',
                (GRAPH_NAME, _app.id, _app.domain.id))
        ag.commit()

        print("    Saving entity_list and entity-app REL")
        conn_1 = ag.connection
        with conn_1.cursor() as _cursor:
            for _entity in self._entity_list:
                _cursor.execute('''SELECT * from cypher(%s, $$
                  CREATE (n:DataEntity {name: %s, nid: %s}) 
                $$) as (v agtype); ''', (GRAPH_NAME, _entity.name, _entity.id))

                _cursor.execute('''SELECT * from cypher(%s, $$
                  MATCH (a:Application), (b:DataEntity)
                  WHERE a.nid = %s AND b.nid = %s
                  CREATE (a)-[e:USES]->(b)
                  RETURN e
                $$) as (e agtype);''', (GRAPH_NAME, _entity.app.id, _entity.id))

                _cursor.execute('''SELECT * from cypher(%s, $$
                  MATCH (a:BusinessDomain), (b:DataEntity)
                  WHERE a.nid = %s AND b.nid = %s
                  CREATE (b)-[e:BELONGS_TO]->(a)
                  RETURN e
                $$) as (e agtype);''', (GRAPH_NAME, _entity.app.domain.id , _entity.id))

        print("    Saving entity FLOWS_TO")
        with conn_1.cursor() as _cursor:
            for _e1, _e2 in self._dup.values():
                _cursor.execute('''SELECT * from cypher(%s, $$
                    MATCH (a:DataEntity), (b:DataEntity)
                    WHERE a.nid = %s AND b.nid = %s
                    CREATE (a)-[e:FLOWS_TO]->(b)
                    RETURN e
                $$) as (e agtype);''', (GRAPH_NAME, _e1.id, _e2.id))

        print("    Saving entity-entity RELATED_TO")
        with conn_1.cursor() as _cursor:
            for _e1, _e2 in tqdm(self._links):
                _cursor.execute('''SELECT * from cypher(%s, $$
                    MATCH (a:DataEntity), (b:DataEntity)
                    WHERE a.nid = %s AND b.nid = %s
                    CREATE (a)-[e:RELATED_TO]->(b)
                    RETURN e
                $$) as (e agtype);''', (GRAPH_NAME, _e1.id, _e2.id))

        ag.commit()


def clear_graph(ag):
    """清除图数据库"""
    conn = ag.connection
    with conn.cursor() as _cursor:
        _cursor.execute('''SELECT * from cypher(%s, $$
            MATCH (v:BusinessDomain)
            DETACH DELETE v
        $$) as (v agtype); ''', (GRAPH_NAME,))

        _cursor.execute('''SELECT * from cypher(%s, $$ 
            MATCH (v:Application)
            DETACH DELETE v
        $$) as (v agtype); ''', (GRAPH_NAME,))
        _cursor.execute('''SELECT * from cypher(%s, $$
            MATCH (v:DataEntity)                
            DETACH DELETE v                   
        $$) as (v agtype); ''', (GRAPH_NAME,))
    ag.commit()


@click.command()
@click.argument('fname', type=click.Path(exists=True))
def main(fname:str=None):
    """
    从XML文件中读取数据结构，并写入图数据库
    """
    print(f"Reading file:{fname}")
    # 读取XML文件
    with open(fname, 'rb') as f:  # 使用'rb'模式读取文件
        tree = etree.parse(f)

    diagram = tree.xpath('//diagram[@name="数据架构Lv1"]')[0]

    xml_str = etree.tostring(diagram, pretty_print=True, encoding='unicode')
    f = StringIO(xml_str)
    root = etree.parse(f)
    print("Creating concept structure.")
    cs = ConceptSturture(root)
    print("Linking relationships.")
    cs.link_entities(root)

    print(f"Save to -> {GRAPH_NAME}")
    ag = age.connect(graph=GRAPH_NAME, dsn=DSN)
    clear_graph(ag)
    cs.make_graph(ag)


if __name__ == '__main__':
    main()
