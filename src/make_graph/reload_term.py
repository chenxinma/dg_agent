"""
元模型导入图数据库 业务术语
"""
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from . import generate_unique_id

SCRIPT_PAHT = Path(__file__).parent

def main():
    """从excel读入业务术语清单，写入图数据库"""
    fname = SCRIPT_PAHT / "files/业务术语.xlsx"
    print(f"Reading file:{fname}")
    df = pd.read_excel(fname)
    df["nid"] = df["术语"].apply(generate_unique_id)
    df["owner"] = ""
    df.rename(columns={"术语": "name", "定义": "definition", "状态": "status"}, inplace=True)

    df[["name", "nid", "definition", "owner", "status"]] \
        .to_csv(SCRIPT_PAHT / "files/data/v_BusinessTerm.csv", index=False)

if __name__ == '__main__':
    main()
