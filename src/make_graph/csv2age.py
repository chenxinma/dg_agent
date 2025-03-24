"""
读取指定目录下的CSV文件，并将数据写入AGE图数据库。
"v_"开头的文件作为节点先导入，"e_"开头的文件作为边后导入。
"""
from typing import Sequence


import os
import csv
from pathlib import Path
import age

from bot.settings import settings

SCRIPT_PAHT = Path(__file__).parent

def create_node_sql(label:str, params:Sequence[str] | None) -> str:
    """create node sql"""
    if params is None:
        raise ValueError("params is None")

    properties: str = "{"
    mark = ""
    for i, param in enumerate(params):
        properties += mark + f"{param}: %s"
        mark = ", "
    properties += "}"

    sql: str = \
    f'''SELECT * from cypher(%s, $$
                            CREATE (n:{label} {properties}) 
                            $$) as (v agtype); '''

    return sql

def create_edge_sql(label:str, from_label:str, to_label:str, params:Sequence[str] | None) -> str:
    """create edge sql"""
    if params is None:
        properties = ""
    else:
        properties = "{"
        mark = ""
        for i, param in enumerate(params):
            properties += mark + f"{param}: %s"
            mark = ", "
        
        properties += "}"

    sql: str = \
        f'''SELECT * from cypher(%s, $$
                  MATCH (a:{from_label}), (b:{to_label})
                  WHERE a.nid = %s AND b.nid = %s
                  CREATE (a)-[e:{label} {properties}]->(b)
                  RETURN e
                $$) as (e agtype); '''
    return sql


def import_csv_to_age(directory:str, graph_name:str, dsn:str):
    # 连接到AGE数据库
    graph: age.Age = age.connect(graph=graph_name, dsn=dsn)

    # 获取目录下所有CSV文件
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

    # 先导入节点文件
    node_files = [f for f in csv_files if f.startswith('v_')]
    for node_file in node_files:
        _tmp: str = node_file[2:]
        label: str = _tmp[:-4]
        _conn = graph.connection
        # 确保_conn不为None
        if _conn is not None:
            with _conn.cursor() as cursor:
                with open(os.path.join(directory, node_file), 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    sql = create_node_sql(label, reader.fieldnames)
                    for row in reader:
                        _vars = [graph_name]
                        _vars.extend(row.values())
                        # 将节点数据插入AGE数据库
                        cursor.execute(sql, _vars)
        graph.commit()

    # 后导入边文件
    edge_files = [f for f in csv_files if f.startswith('e_')]
    for edge_file in edge_files:
        _tmp2: str = edge_file[2:]
        e_label: str = _tmp2[:-4]
        _conn = graph.connection
        # 确保_conn不为None
        if _conn is not None:
            with _conn.cursor() as cursor:
                with open(os.path.join(directory, edge_file), 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    fields: list[str] = list(reader.fieldnames) if reader.fieldnames is not None else []
                    props = None
                    from_label = fields[0][len('from_'):]
                    to_label = fields[1][len('to_'):]
                    if len(fields) > 2:
                        props = fields[2:]
                    sql = create_edge_sql(e_label, from_label, to_label, props)
                    for row in reader:
                        _vars = [graph_name]
                        _vars.extend(row.values())
                        # 将节点数据插入AGE数据库
                        cursor.execute(sql, _vars)
            graph.commit()

    # 关闭数据库连接
    graph.close()


def clear_graph(graph_name:str, dsn:str):
    """清除图数据库"""
     # 连接到AGE数据库
    graph: age.Age = age.connect(graph=graph_name, dsn=dsn)

    conn = graph.connection
    if conn is not None:
        with conn.cursor() as _cursor:
            _cursor.execute("""SELECT graphid FROM ag_catalog.ag_graph WHERE name = %s""", (graph_name,))
            data = _cursor.fetchone()
            gid = data[0] if data else None
            _cursor.execute("""SELECT "name"
                             FROM ag_catalog.ag_label WHERE kind = 'v' and graph = %s""", (gid,))
            labels = _cursor.fetchall()
            n_labels = [l[0] for l in labels]
            # 清空所有节点和边
            for label in n_labels:
                _cursor.execute(f'''SELECT * from cypher(%s, $$
                    MATCH (v:{label})
                    DETACH DELETE v
                $$) as (v agtype); ''', (graph_name,))           
        graph.commit()
    graph.close()

if __name__ == '__main__':
    # 配置数据库连接信息
    graph_name = settings.get_setting("age.graph")
    dsn = settings.get_setting("age.dsn")

    # 指定CSV文件目录
    directory = SCRIPT_PAHT / 'files/data'

    # 导入数据
    # 确保 graph_name 是字符串类型
    if isinstance(graph_name, list):
        graph_name = graph_name[0] if graph_name else ""
    if isinstance(dsn, list):
        dsn = dsn[0] if dsn else ""

    clear_graph(graph_name, dsn)
    import_csv_to_age(str(directory.absolute()), graph_name, dsn)
