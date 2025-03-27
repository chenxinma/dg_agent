"""物理模型导入图数据库
"""
from pandas.core.frame import DataFrame


from pandas.core.series import Series


from typing import Any


from pathlib import Path
import os
import pandas as pd

from . import generate_unique_id

SCRIPT_PAHT = Path(__file__).parent

class MetadataSturture:
    """构建元模型 物理表、列"""

    def __init__(self, df_entities:pd.DataFrame, df_columns:pd.DataFrame):
        self._df_entities: pd.DataFrame = df_entities
        self._df_columns: pd.DataFrame = df_columns


    def save_csv(self, output_dir):
        """将物理表、列及其关联关系保存到CSV文件中"""
         # 写入实体CSV文件
        path = Path(output_dir) if output_dir else Path.cwd()
        path.mkdir(parents=True, exist_ok=True)

        # 保存物理表信息
        # 从 self._df_entities 中选择所需列并确保结果为 DataFrame 类型
        tables_df: pd.DataFrame = pd.DataFrame(self._df_entities[["app_name", "name", "schema", "table_name"]])
        tables_df["full_table_name"] = tables_df["schema"] + "." + tables_df["table_name"]
        tables_df["dataentity_nid"] = tables_df.apply(lambda x: generate_unique_id(x.app_name +"."+ x["name"]), axis=1)
        tables_df["nid"] = tables_df["full_table_name"].apply(generate_unique_id)
        # tables_df.rename(columns={"name": "entity_name"}, inplace=True)
        tables_df[["name", "nid", "schema", "table_name", "full_table_name"]] \
            .to_csv(os.path.join(output_dir, "v_PhysicalTable.csv"), index=False)

        # 保存列信息
        columns_df: pd.DataFrame = pd.DataFrame(self._df_columns[["Table", "Column", "Type"]])
        columns_df["nid"] = columns_df.apply(lambda x: generate_unique_id(x.Table +"."+ x.Column), axis=1)
        columns_df["physicaltable_nid"] = columns_df["Table"].apply(generate_unique_id)
        columns_df.rename(columns={"Column": "name", "Type": "data_type"}, inplace=True)
        columns_df = pd.DataFrame(columns_df[
            columns_df["Table"].isin(tables_df["schema"] + "." + tables_df["table_name"])])
        columns_df[["name", "nid", "data_type"]].to_csv(os.path.join(output_dir, "v_Column.csv"), index=False)

        # 保存关联关系
        implements = []
        def apply_func(row):
            """apply func"""
            
            implements.append({
                "from_DataEntity": row.dataentity_nid,
                "to_PhysicalTable": row.nid,
            })
        tables_df.apply(apply_func, axis=1)        
        implements_df = pd.DataFrame(implements)
        implements_df.to_csv(os.path.join(output_dir, "e_IMPLEMENTS.csv"), index=False)

        has_columns_df: DataFrame = pd.DataFrame(columns_df[["physicaltable_nid", "nid"]].copy())
        has_columns_df.rename(columns={
                                        "physicaltable_nid": "from_PhysicalTable",
                                        "nid": "to_Column"
                                      }, inplace=True)
        has_columns_df.to_csv(os.path.join(output_dir, "e_HAS_COLUMN.csv"), index=False)


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

    print(f"Save to csv files...")
    metadata_sturture = MetadataSturture(df_all_entities, df_all_columns)
    metadata_sturture.save_csv(output_dir=SCRIPT_PAHT / 'files/data')

if __name__ == '__main__':
    main()
