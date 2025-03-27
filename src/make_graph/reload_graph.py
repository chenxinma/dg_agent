"""
元模型导入图数据库
"""
from io import StringIO
from pathlib import Path

from lxml.etree import tostring, parse, fromstring
from tqdm import tqdm

from .domain import Domain
from .dataentity import DataEntity
from .application import Application
from . import generate_unique_id


SCRIPT_PAHT = Path(__file__).parent

class ConceptSturture:
    """构建元模型 业务域、应用、数据实体"""
    def __init__(self, xml_root):
        self._domain_list = Domain.load(xml_root)
        self._app_list = Application.load(xml_root)
        self._entity_list = DataEntity.load(xml_root)
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


    def save_csv(self, output_dir=None):
        """将结构数据写入CSV文件"""
        import csv
        
        # 写入实体CSV文件
        path = Path(output_dir) if output_dir else Path.cwd()
        path.mkdir(parents=True, exist_ok=True)
        with open(path / 'v_BusinessDomain.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['name', 'nid', 'code'])
            for _domain in self._domain_list:
                writer.writerow([_domain.name, _domain.get_nid(), _domain.code])
        
        with open(path / 'v_Application.csv', 'w',encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['name', 'nid'])
            for _app in self._app_list:
                writer.writerow([_app.name, _app.get_nid()])
        
        with open(path / 'v_DataEntity.csv', 'w',  encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['name', 'nid'])
            for _entity in self._entity_list:
                writer.writerow([_entity.name, _entity.get_nid()])
        
        # 写入关系CSV文件
        with open(path / 'e_CONTAINS.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['from_BusinessDomain', 'to_Application'])
            for _app in self._app_list:
                writer.writerow([_app.domain.get_nid(), _app.get_nid()])
        
        with open(path / 'e_USES.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['from_Application', 'to_DataEntity'])
            for _entity in self._entity_list:
                writer.writerow([_entity.app.get_nid(), _entity.get_nid()])

        with open(path / 'e_BELONGS_TO.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['from_DataEntity', 'to_BusinessDomain'])
            for _entity in self._entity_list:
                writer.writerow([_entity.get_nid(), _entity.app.domain.get_nid()])
        
        with open(path / 'e_FLOWS_TO.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['from_DataEntity', 'to_DataEntity'])
            for _e1, _e2 in self._dup.values():
                writer.writerow([_e1.get_nid(), _e2.get_nid()])
        
        with open(path / 'e_RELATED_TO.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['from_DataEntity', 'to_DataEntity'])
            for _e1, _e2 in self._links:
                writer.writerow([_e1.get_nid(), _e2.get_nid()])


def main():
    """
    从XML文件中读取数据结构，并写入图数据库
    """
    fname = SCRIPT_PAHT / "files/数据架构.drawio.xml"
    print(f"Reading file:{fname}")
    # 读取XML文件
    with open(fname, 'rb') as f:  # 使用'rb'模式读取文件
        tree = parse(f)

    diagram = tree.xpath('//diagram[@name="数据架构Lv1"]')[0]

    xml_str = tostring(diagram, pretty_print=True, encoding='unicode')
    f = StringIO(xml_str)
    root = parse(f)
    print("Creating concept structure.")
    cs = ConceptSturture(root)
    print("Linking relationships.")
    cs.link_entities(root)

    print("Save to -> csv files")
    cs.save_csv(SCRIPT_PAHT / 'files/data')


if __name__ == '__main__':
    main()
