import os
import click
import age
import pandas as pd
from tqdm import tqdm

from . import GRAPH_NAME, DSN, generate_unique_id

class MetadataSturture:
    def __init__(self, metadatas:dict, df_columns:pd.DataFrame):
        self._db_set = metadatas
        self._df_columns = df_columns
        
    def save_columns(self, cursor, full_table_name):
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
                    MATCH (a:Table), (b:Column)
                    WHERE a.full_table_name = %s AND b.nid = %s
                    CREATE (b)-[e:BELONG]->(a)
                    RETURN e
                    $$) as (e agtype); ''', 
                    (GRAPH_NAME, full_table_name, nid))
        
    def save_tables(self, ag, db:pd.DataFrame):
        conn_1 = ag.connection
        with conn_1.cursor() as _cursor:
            for row in tqdm(db.itertuples(), total=db.shape[0]):
                _cursor.execute('''SELECT * from cypher(%s, $$ 
                    CREATE (n:Table {name: %s, table_name: %s, schema: %s, full_table_name: %s}) 
                    $$) as (v agtype); ''', (GRAPH_NAME, 
                                            row.entity,
                                            row.table_name, 
                                            row.schema, 
                                            row.full_table_name))

                _cursor.execute('''SELECT * from cypher(%s, $$ 
                    MATCH (a:Entity), (b:Table)
                    WHERE a.nid = %s AND b.full_table_name = %s
                    CREATE (a)-[e:DEFINE]->(b)
                    RETURN e
                    $$) as (e agtype); ''', 
                    (GRAPH_NAME, row.nid, row.full_table_name))
                
                self.save_columns(_cursor, row.full_table_name)

        ag.commit()

    def make_graph(self, ag):
        for db in self._db_set.values():
            self.save_tables(ag, db)
        
def clear_graph(ag):
    conn = ag.connection
    with conn.cursor() as _cursor:
        _cursor.execute('''SELECT * from cypher(%s, $$
            MATCH (v:Table)
            DETACH DELETE v
        $$) as (v agtype); ''', (GRAPH_NAME,))
        _cursor.execute('''SELECT * from cypher(%s, $$
            MATCH (v:Column)
            DETACH DELETE v
        $$) as (v agtype); ''', (GRAPH_NAME,))
    ag.commit()

@click.command()
@click.argument('fname', type=click.Path(exists=True))
def main(fname:str):
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
