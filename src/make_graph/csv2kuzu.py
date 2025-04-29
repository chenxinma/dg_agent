"""
读取指定目录下的CSV文件，并将数据写入kuzu图数据库。
"v_"开头的文件作为节点先导入，"e_"开头的文件作为边后导入。
"""
import os
import csv
from pathlib import Path
import warnings

import kuzu
from tqdm.rich import tqdm_rich
import tqdm
warnings.filterwarnings("ignore", category=tqdm.TqdmExperimentalWarning)


SCRIPT_PAHT = Path(__file__).parent

def to_field_define(fields: list[str]) -> str:
    """
    将字段名转换为kuzu的字段定义
    """
    int_fields = ["size"]
    # fields_set = [f"`{field}` STRING" for field in fields]
    fields_set = [ ]
    for field in fields:
        if field in int_fields:
            fields_set.append(f"`{field}` INT64")
        else:
            fields_set.append(f"`{field}` STRING")
    fields_set.append("PRIMARY KEY (nid)")
    return ", ".join(fields_set)


def load_data(directory: str) -> dict[str, list[str]]:
    """
    创建schema
    """
    ddl_set = {
        "drop_node": [],
        "drop_edge": [],
        "create_node": [],
        "create_edge": [],
        "insert_node": [],
        "insert_edge": [],
    }
    # 获取目录下所有CSV文件
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

    # 先导入节点文件
    node_files = [f for f in csv_files if f.startswith('v_')]
    for node_file in node_files:
        _tmp: str = node_file[2:]
        label: str = _tmp[:-4]
        with open(os.path.join(directory, node_file), 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                continue
            columns = to_field_define(list(reader.fieldnames))
            ddl_set["drop_node"].append(f"DROP TABLE IF EXISTS `{label}`")
            ddl = f"CREATE NODE TABLE `{label}` ({columns})"
            ddl_set["create_node"].append(ddl)

        fpath = Path(os.path.join(directory, node_file))
        ddl_set["insert_node"].append(f"COPY `{label}` FROM \"{fpath.as_posix()}\" (header=true, parallel=false)")


    # 后导入边文件
    edge_files = [f for f in csv_files if f.startswith('e_')]
    for edge_file in edge_files:
        _tmp2: str = edge_file[2:]
        e_label: str = _tmp2[:-4]
        with open(os.path.join(directory, edge_file), 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                continue
            fields: list[str] = list(reader.fieldnames) if reader.fieldnames is not None else []
            props = ""
            from_label = fields[0][len('from_'):]
            to_label = fields[1][len('to_'):]
            if len(fields) > 2:
                props = ", " + ", ".join([ f"{f} STRING" for f in fields[2:]])

            ddl_set["drop_edge"].append(f"DROP TABLE IF EXISTS {e_label}")
            ddl_set["create_edge"].append(f"CREATE REL TABLE {e_label} (FROM `{from_label}` TO `{to_label}`{props})")

        fpath = Path(os.path.join(directory, edge_file))
        ddl_set["insert_edge"].append(f"COPY {e_label} FROM \"{fpath.as_posix()}\" (header=true)")

    return ddl_set

def main() -> None:
    # Create an empty on-disk database and connect to it
    db = kuzu.Database(SCRIPT_PAHT / 'files/kuzu')
    conn = kuzu.Connection(db)

    directory = SCRIPT_PAHT / 'files/data'
    # Create schema
    ddl_set = load_data(str(directory.absolute()))
    # Drop tables
    for ddl in tqdm_rich(ddl_set["drop_edge"], desc="Drop edge"):
        conn.execute(ddl)
    for ddl in tqdm_rich(ddl_set["drop_node"], desc="Drop node"):
        conn.execute(ddl)
    
    # Create tables
    for ddl in tqdm_rich(ddl_set["create_node"], desc="Create node"):
        conn.execute(ddl)
    for ddl in tqdm_rich(ddl_set["create_edge"], desc="Create edge"):
        conn.execute(ddl)

    # Insert data
    for ddl in tqdm_rich(ddl_set["insert_node"], desc="Insert node"):
        conn.execute(ddl)
    for ddl in tqdm_rich(ddl_set["insert_edge"], desc="Insert edge"):
        conn.execute(ddl)
    

if __name__ == '__main__':
    main()