"""加载数据实体关联信息
"""

import json
import click
import age
from tqdm import tqdm
from . import GRAPH_NAME, DSN

def sql_decode(s):
    """
    将字符串转义，用于sql注入
    """
    return s.replace("'", "\\'")

@click.command()
@click.argument('fname', type=click.Path(exists=True))
def main(fname:str=None):
    """
    加载数据实体关联信息
    """

    print("加载数据实体关联信息")
    ag = age.connect(graph=GRAPH_NAME, dsn=DSN)
    conn = ag.connection

    with open(fname, "r", encoding="utf-8") as _f:
        links = json.load(_f)
        with conn.cursor() as cur:
            for lnk in tqdm(links):
                cur.execute(f"""SELECT * FROM cypher(%s, $$
                    MATCH ()-[l:RELATED_TO]->() WHERE ID(l)=%s
                    SET l.rel = '{sql_decode(lnk["rel"])}'
                    RETURN l
                $$) AS (l agtype);
                """, (GRAPH_NAME, lnk["id"]))
    ag.commit()

if __name__ == '__main__':
    main()
