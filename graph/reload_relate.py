"""加载数据实体关联信息
"""

import json
import click
import age
from tqdm import tqdm

from bot.agent import GRAPH_NAME, DSN

def sql_decode(s):
    return s.replace("'", "\\'")

@click.command()
@click.argument('fname', type=click.Path(exists=True))
def main(fname:str=None):
    ag = age.connect(graph=GRAPH_NAME, dsn=DSN)
    conn = ag.connection

    with open(fname, "r", encoding="utf-8") as _f:
        links = json.load(_f)
        with conn.cursor() as _cursor:
            for lnk in tqdm(links):
                _cursor.execute(f"""SELECT * FROM cypher(%s, $$
                    MATCH ()-[l:RELATED_TO]->() WHERE ID(l)=%s
                    SET l.rel = '{sql_decode(lnk["rel"])}'
                    RETURN l
                $$) AS (l agtype);
                """, (GRAPH_NAME, lnk["id"])):RELATED_TO*1..
    ag.commit()
            
if __name__ == '__main__':
    main()