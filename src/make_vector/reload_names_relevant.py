"""
将图数据库的名称导入chroma，实现一定程度的语义消歧
"""
import os
import sys
from pathlib import Path
import chromadb
import pandas as pd
from tqdm import tqdm

SCRIPT_PAHT = Path(__file__).parent
sys.path.append(str(SCRIPT_PAHT.parent))

from bot.settings import settings
from bot.models.embedding import GTEEmbeddingFunction

skip_csv =["v_Column.csv"]
SOURCE_DIR = str(SCRIPT_PAHT.parent / r"make_graph\files\data")

def get_client():
    client = chromadb.PersistentClient(path=settings.get_setting("chromadb.persist_directory"))
    return client

def get_collection():
    client = get_client()
    cn = settings.get_setting("chromadb.names_collection")
    if cn in client.list_collections():
        client.delete_collection(cn)

    collection = client.create_collection(cn, 
                          embedding_function=GTEEmbeddingFunction())
    return collection

def list_all_csv():
    # 遍历SOURCE_DIR下所有文件
    for root, _, files in os.walk(SOURCE_DIR):
        for file in files:
            if file.startswith('v_') and file.endswith('.csv') \
                and file not in skip_csv:
                file_path = os.path.join(root, file)
                yield file_path, file[2:-4]

def main():
    docs = []
    ids = []
    # 读取CSV文件
    for file_path, node_name in tqdm(list_all_csv()):
        tqdm.write(f"Processing {file_path}...")
        df = pd.read_csv(file_path)
        # 提取name列
        names = df['name'].tolist()
        nid = df['nid'].tolist()

        docs.extend([ f"({node_name} {{name:{name}}})" for name in names])
        ids.extend(nid)

    # 连接到ChromaDB
    collection = get_collection()
    collection.add(
        documents=docs,
        ids= ids
    )

if __name__ == "__main__":
    main()
