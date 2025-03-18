"""物理模型导入图数据库
"""
from pathlib import Path
import os
import age
import pandas as pd

from tqdm import tqdm
from . import GRAPH_NAME, DSN, generate_unique_id

SCRIPT_PAHT = Path(__file__).parent

class MetadataSturture:
    """构建元模型 物理表、列"""

    def __init__(self, df_entities:pd.DataFrame, df_columns:pd.DataFrame):
        self._df_entities = df_entities
        self._df_columns = df_columns

    def save_columns(self, cursor, full_table_name):
        """保存列"""
        _df = self._df_columns[self._df_columns['Table'] == full_table_name]
        for row in _df.itertuples():
            nid = generate_unique_id(full_table_name +"."+ row.Column)
            cursor.execute('''SELECT * from cypher(%s, $$
                    CREATE (n:Column {name: %s, dtype: %s, nid: %s}) 
                    $$) as (v agtype); ''', (GRAPH_NAME,
                                            row.Column,
                                            row.Type,
                                            nid))
            cursor.execute('''SELECT * from cypher(%s, $$
                    MATCH (a:PhysicalTable), (b:Column)
                    WHERE a.full_table_name = %s AND b.nid = %s
                    CREATE (a)-[e:HAS_COLUMN]->(b)
                    RETURN e
                    $$) as (e agtype); ''',
                    (GRAPH_NAME, full_table_name, nid))

    def save_tables(self, ag, db:pd.DataFrame):
        """保持物理表"""
        conn_1 = ag.connection
        with conn_1.cursor() as _cursor:
            for row in tqdm(db.itertuples(), total=db.shape[0]):
                _full_table_name = f"{row.schema}.{row.table_name}"
                _entity_name = row.name

                _cursor.execute('''SELECT * from cypher(%s, $$
                    CREATE (n:PhysicalTable {name: %s, table_name: %s, schema: %s, full_table_name: %s}) 
                    $$) as (v agtype); ''', (GRAPH_NAME,
                                            _entity_name,
                                            row.table_name,
                                            row.schema,
                                            _full_table_name))

                _cursor.execute('''SELECT * from cypher(%s, $$
                    MATCH (a:Application {name: %s} )-[r]->(e:DataEntity {name: %s}), (b:PhysicalTable)
                    WHERE b.full_table_name = %s
                    CREATE (e)-[i:IMPLEMENTS]->(b)
                    RETURN i
                    $$) as (i agtype); ''',
                    (GRAPH_NAME, row.app_name, _entity_name, _full_table_name))

                self.save_columns(_cursor, _full_table_name)

        ag.commit()

    def make_graph(self, ag):
        """构建物理模型"""
        self.save_tables(ag, self._df_entities)

def clear_graph(ag):
    """清除图数据库"""
    conn = ag.connection
    with conn.cursor() as _cursor:
        _cursor.execute('''SELECT * from cypher(%s, $$
            MATCH (v:PhysicalTable)
            DETACH DELETE v
        $$) as (v agtype); ''', (GRAPH_NAME,))
        _cursor.execute('''SELECT * from cypher(%s, $$
            MATCH (v:Column)
            DETACH DELETE v
        $$) as (v agtype); ''', (GRAPH_NAME,))
    ag.commit()

def main():
    """
    加载数据实体-物理表
    """
    # 构建 db 目录的路径
    db_dir = SCRIPT_PAHT / "files/db"
    # 检查 db 目录是否存在
    if not os.path.exists(db_dir):
        print(f"db 目录不存在: {db_dir}")
        return

    # 列出 db 目录下所有 .xlsx 文件
    xlsx_files = [f for f in os.listdir(db_dir) if f.endswith('.xlsx')]
    df_all_entities = pd.DataFrame()
    df_all_columns = pd.DataFrame()
    for f in xlsx_files:
        df = pd.read_excel(os.path.join(db_dir, f), sheet_name="Columns")
        df_all_columns = pd.concat([df_all_columns, df])

        df_entity = pd.read_excel(os.path.join(db_dir, f), sheet_name="Entities")
        df_all_entities = pd.concat([df_all_entities, df_entity])

    print(f"Save to -> {GRAPH_NAME}")
    ag = age.connect(graph=GRAPH_NAME, dsn=DSN)
    clear_graph(ag)

    ms = MetadataSturture(df_all_entities, df_all_columns)
    ms.make_graph(ag)

if __name__ == '__main__':
    main()
