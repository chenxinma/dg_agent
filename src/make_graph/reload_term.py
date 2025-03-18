"""
元模型导入图数据库 业务术语
"""
from pathlib import Path
import pandas as pd
from tqdm import tqdm

import age
from . import GRAPH_NAME, DSN

SCRIPT_PAHT = Path(__file__).parent

def clear_graph(ag):
    """清除图数据库"""
    conn = ag.connection
    with conn.cursor() as _cursor:
        _cursor.execute('''SELECT * from cypher(%s, $$
            MATCH (v:BusinessTerm)
            DETACH DELETE v
        $$) as (v agtype); ''', (GRAPH_NAME,))

    ag.commit()

def make_term(cur, row):
    """写入图数据库"""
    cur.execute('''SELECT * from cypher(%s, $$
        CREATE (n:BusinessTerm  {name: %s, definition: %s, owner: %s, status:  %s}) 
    $$) as (v agtype); ''', (GRAPH_NAME,
                             row["术语"],
                             row["定义"],
                             "",
                             row["状态"]))

def main():
    """从excel读入业务术语清单，写入图数据库"""
    fname = SCRIPT_PAHT / "files/业务术语.xlsx"
    print(f"Reading file:{fname}")
    df = pd.read_excel(fname)

    print(f"Save to -> {GRAPH_NAME}")
    ag = age.connect(graph=GRAPH_NAME, dsn=DSN)
    clear_graph(ag)

    conn_1 = ag.connection
    with conn_1.cursor() as cur:
        tqdm.pandas(desc="Saving BusinessTerms")
        df.progress_apply(lambda x: make_term(cur, x), axis=1)
    ag.commit()

if __name__ == '__main__':
    main()
