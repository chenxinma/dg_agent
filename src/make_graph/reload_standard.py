"""读入数据标准"""
from pathlib import Path
import os
import pandas as pd
from . import generate_unique_id

SCRIPT_PAHT = Path(__file__).parent

class StandardStructure:
    """构建元模型 数据标准、属性"""

    def __init__(self, df_standard:pd.DataFrame, df_attribute:pd.DataFrame):
        self.df_standard: pd.DataFrame = df_standard
        self.df_attribute: pd.DataFrame = df_attribute


    def save_csv(self, output_dir):
        """将数据标准、属性及其关联关系保存到CSV文件中"""
         # 写入实体CSV文件
        path = Path(output_dir) if output_dir else Path.cwd()
        path.mkdir(parents=True, exist_ok=True)
        # 保存数据标准信息
        self.df_standard["nid"] = self.df_standard["code"].apply(generate_unique_id)
        self.df_standard["business_owner"] = ""

        self.df_standard.rename(columns={
            "BASIC_DEFINITION": "definition", 
            "BASIC_DATA_CATEGORY": "data_type",
            "BASIC_DATA_EXPRESSION": "data_expression",
            "BASIC_BUSINESS_RULE": "business_rule",
            "BASIC_CORE_SYSTEM": "core_system",
            "BASIC_STANDARD_STATUS": "status",
        }, inplace=True)

        self.df_standard[[
            "name", 
            "nid", 
            "definition", 
            "data_type", 
            "data_expression", 
            "business_rule", 
            "core_system",
            "business_owner", 
            "status"]] \
            .to_csv(os.path.join(output_dir, "v_DataStandard.csv"), index=False)
        
        # 保存属性信息
        df_attribute = self.df_attribute.copy()
        df_attribute["nid"] = df_attribute["name"].apply(generate_unique_id)
        df_attribute["data_type"] = df_attribute.apply(
            lambda x: f"{x['data_type'].strip().lower()}({x['data_length']})", axis=1)
        df_attribute["dataentity_nid"] = \
            df_attribute.apply(lambda x: generate_unique_id(x["app_name"] +"."+ x["data_entity"]), axis=1)
        df_attribute["column_nid"] = \
            df_attribute.apply(lambda x: generate_unique_id(x['schema'] +
                                                                   "."+ x['table_name']+
                                                                   "."+ x['column_name']), axis=1)

        df_attribute[[
            "name", 
            "nid", 
            "data_type",
            "value_set_code"
        ]] \
            .drop_duplicates() \
            .to_csv(os.path.join(output_dir, "v_Attribute.csv"), index=False)

        # 保存关联关系
        complies_with = []
        df_attr_std = pd.merge(
            self.df_standard, df_attribute, 
            on="code", how="inner", suffixes=("_standard", "_attribute"))
        
        df_attr_std.apply(
            lambda row: complies_with.append({
                "from_Attribute": row.nid_attribute,
                "to_DataStandard": row.nid_standard,
            }), axis=1)
        complies_with_df = pd.DataFrame(complies_with)
        complies_with_df.to_csv(os.path.join(output_dir, "e_COMPLIES_WITH.csv"), index=False)

        has_attribute_df: pd.DataFrame = pd.DataFrame(df_attribute[["dataentity_nid", "nid"]].drop_duplicates())
        has_attribute_df.rename(columns={
                                        "dataentity_nid": "from_DataEntity",
                                        "nid": "to_Attribute"
                                      }, inplace=True)
        has_attribute_df.to_csv(os.path.join(output_dir, "e_HAS_ATTRIBUTE.csv"), index=False)

        map_to_df = pd.DataFrame(df_attribute[["nid", "column_nid"]].drop_duplicates())

        map_to_df.rename(columns={
                                "nid": "from_Attribute",
                                "column_nid": "to_Column"
                                }, inplace=True)
        map_to_df.to_csv(os.path.join(output_dir, "e_MAP_TO.csv"), index=False)


def main():
    """
    加载数据标准
    """
    # 构建 standard 目录的路径
    standard_dir = SCRIPT_PAHT / "files/standard"
    # 检查 standard 目录是否存在
    if not os.path.exists(standard_dir):
        print(f"standard 目录不存在: {standard_dir}")
        return
    
    # 列出 db 目录下所有 .xlsx 文件
    xlsx_files = [f for f in os.listdir(standard_dir) if f.endswith('.xlsx')]
    df_all_standard = pd.DataFrame()
    df_all_attribute = pd.DataFrame()
    for f in xlsx_files:
        file_path = os.path.join(standard_dir, f)
        # 读取 Excel 文件
        excel_file = pd.ExcelFile(file_path)
        # 获取所有表名
        sheet_names = excel_file.sheet_names
        excel_file.close()

        # 检查 '数据标准' 工作表是否存在
        if "数据标准" in sheet_names:
            df = pd.read_excel(file_path, sheet_name = "数据标准")
            df_all_standard = pd.concat([df_all_standard, df])


        # 检查 '属性' 工作表是否存在
        if "属性" in sheet_names:
            df1 = pd.read_excel(file_path, sheet_name = "属性")
            if "data_entity" in df1.columns:  # 完成了手工映射
                df_all_attribute = pd.concat([df_all_attribute, df1])
        else:
            print(f"文件 {file_path} 中不存在 '属性' 工作表。")

    ss = StandardStructure(df_all_standard, df_all_attribute)
    ss.save_csv(SCRIPT_PAHT / "files/data")


if __name__ == '__main__':
    main()