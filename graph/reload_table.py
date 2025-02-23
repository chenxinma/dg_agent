"""物理模型导入图数据库
"""
import os
import sys
import click
import age
try:
    import pandas as pd
except ImportError:
    print("Pandas is not installed. Please install it using 'pip install pandas'.")
    sys.exit(1)

from tqdm import tqdm
from . import GRAPH_NAME, DSN, generate_unique_id

class MetadataSturture:
    """构建元模型 物理表、列"""

    def __init__(self, metadatas:dict, df_columns:pd.DataFrame):
        self._db_set = metadatas
        self._df_columns = df_columns

    def save_columns(self, cursor, full_table_name):
        """保持列"""
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
                _cursor.execute('''SELECT * from cypher(%s, $$
                    CREATE (n:PhysicalTable {name: %s, table_name: %s, schema: %s, full_table_name: %s}) 
                    $$) as (v agtype); ''', (GRAPH_NAME,
                                            row.entity,
                                            row.table_name,
                                            row.schema,
                                            row.full_table_name))

                _cursor.execute('''SELECT * from cypher(%s, $$
                    MATCH (a:DataEntity), (b:PhysicalTable)
                    WHERE a.nid = %s AND b.full_table_name = %s
                    CREATE (a)-[e:IMPLEMENTS]->(b)
                    RETURN e
                    $$) as (e agtype); ''',
                    (GRAPH_NAME, row.nid, row.full_table_name))

                self.save_columns(_cursor, row.full_table_name)

        ag.commit()

    def make_graph(self, ag):
        """构建物理模型"""
        for db in self._db_set.values():
            self.save_tables(ag, db)

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

@click.command()
@click.argument('fname', type=click.Path(exists=True))
def main(fname:str=None):
    """
    从 fname 文件中读取元数据，并构建物理模型
    """
    print("Starting...")
    print(f"Reading file:{fname}")
    metadatas = pd.read_excel(fname, sheet_name=None)

    # 获取 fname 所在的目录
    fname_dir = os.path.dirname(fname)
    # 构建 db 目录的路径
    db_dir = os.path.join(fname_dir, "db")
    # 检查 db 目录是否存在
    if not os.path.exists(db_dir):
        print(f"db 目录不存在: {db_dir}")
        return

    # 列出 db 目录下所有 .xlsx 文件
    xlsx_files = [f for f in os.listdir(db_dir) if f.endswith('.xlsx')]
    df_all_columns = pd.DataFrame()
    for f in xlsx_files:
        df = pd.read_excel(os.path.join(db_dir, f), sheet_name="Columns")
        df_all_columns = pd.concat([df_all_columns, df])

    print(f"Save to -> {GRAPH_NAME}")
    ag = age.connect(graph=GRAPH_NAME, dsn=DSN)
    clear_graph(ag)

    ms = MetadataSturture(metadatas, df_all_columns)
    ms.make_graph(ag)

if __name__ == '__main__':
    main()
