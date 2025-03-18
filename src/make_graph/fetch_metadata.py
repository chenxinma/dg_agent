"""获取 数据实体-物理表"""
from pathlib import Path
import yaml
import pandas as pd
import age
import click

from sqlalchemy import create_engine, Engine
from trino.auth import BasicAuthentication
from . import GRAPH_NAME, DSN

SCRIPT_PAHT = Path(__file__).parent
def skip_table(t_name:str) -> bool:
    """过滤日志表、备份表"""
    if t_name.endswith("_log"):
        return False

    if t_name[-8:].isdigit() or t_name[-6:].isdigit():
        return False
    return True

def fetch_columns(table_name:str, conn:Engine) -> pd.DataFrame:
    """获取表字段信息"""
    # print(f"fetch_columns: {table_name}")
    return pd.read_sql(f"SHOW COLUMNS FROM {table_name}", con=conn)

def fetch_rawdata(schema:str, app_name:str,
                  url:str, username:str, password:str):
    """获取元数据"""
    engine = create_engine(url,
                       connect_args={
                            "auth": BasicAuthentication(username, password),
                            "http_scheme": "https",
                        })
    ag = age.connect(graph=GRAPH_NAME, dsn=DSN)

    df_tables = pd.read_sql(f"SHOW TABLES FROM {schema}", con=engine)
    df_tables["mark"] = df_tables["Table"].apply(skip_table)
    df_tables = df_tables[df_tables["mark"]]
    df_all_columns = pd.DataFrame()
    for t in df_tables.itertuples():
        t_name = f"{schema}.{t.Table}"
        df = fetch_columns(t_name, engine)
        df["Table"] = t_name
        df_all_columns = pd.concat([df_all_columns, df])

    _conn = ag.connection
    with _conn.cursor() as _cursor:
        sql = f"""
        SELECT * FROM cypher('{GRAPH_NAME}', $$
        MATCH (a:Application {{name: '{app_name}'}})-[r]->(e:DataEntity)
        RETURN a,e
        $$) AS (a agtype, e agtype);
        """
        _cursor.execute(sql)
        result = _cursor.fetchall()
        df_entities = pd.DataFrame([
                                    dict(app_name=v[0].properties["name"],
                                         name=v[1].properties["name"]) for v in result
                                    ])
        df_entities["schema"] = schema
        df_entities["table_name"] = ""

    with pd.ExcelWriter(SCRIPT_PAHT / f'./files/db/{schema}.xlsx') as writer:
        df_tables.to_excel(writer, sheet_name='Tables', index=False)
        df_entities.to_excel(writer, sheet_name='Entities', index=False)
        df_all_columns.to_excel(writer, sheet_name='Columns', index=False)

@click.command()
@click.argument('schema', type=click.STRING)
@click.argument('appname', type=click.STRING)
def main(schema:str="", appname:str=""):
    """main"""
    config_file = SCRIPT_PAHT / "./files/db/trino.yaml"
    print(config_file)

    config = yaml.safe_load(open(config_file, encoding="utf-8"))
    db_url = config["trino"]["url"]
    username = config["trino"]["username"]
    passowrd = config["trino"]["password"]

    fetch_rawdata(schema, appname, db_url, username, passowrd)

if __name__ == '__main__':
    main()
