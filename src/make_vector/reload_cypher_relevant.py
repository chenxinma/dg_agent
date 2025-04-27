"""
将一些查询数据治理的cypher语句，写入chromadb
"""
import sys
from pathlib import Path
import hashlib
import chromadb

SCRIPT_PAHT = Path(__file__).parent
sys.path.append(str(SCRIPT_PAHT.parent))

from bot.settings import settings
from bot.models.embedding import GTEEmbeddingFunction

examples = [
"""按数据实体名查找数据实体和应的物理表
MATCH (e:DataEntity {name: 'EntityName'})-[:IMPLEMENTS]->(t:PhysicalTable)
RETURN e, t
-- 替换 EntityName 为目标数据实体的名称。
""",
"""按数据实体名查找关联数据实体及其物理表
查询：
MATCH (e1:DataEntity {name: 'EntityName'})-[r]->(e2:DataEntity),
    (e1)-[:IMPLEMENTS]->(t1:PhysicalTable),
    (e2)-[:IMPLEMENTS]->(t2:PhysicalTable)
RETURN e1, e2, r, t1, t2
-- 替换 EntityName 为目标数据实体的名称。
""",
"""按应用名称获取应用和关联的所有数据实体
查询：
MATCH (app:Application {name: 'ApplicationName'})-[r]-(e:DataEntity)
RETURN app, e
-- 替换 ApplicationName 为目标应用程序的名称。
""",
"""按应用名称获取应用、关联的所有数据实体和其物理表
查询：
MATCH (app:Application {name: 'ApplicationName'})-[r]-(e:DataEntity)-[:IMPLEMENTS]->(t:PhysicalTable)
RETURN app, e, t
-- 替换 ApplicationName 为目标应用程序的名称。
""",
"""查找业务域下的所有实体
查询：
MATCH (d:BusinessDomain {name: 'DomainName'})-[:CONTAINS]-(a:Application)-[r]-(e:DataEntity)
RETURN e
-- 替换 DomainName 为目标业务域的名称。
""",
"""列出前 n 个数据实体
查询：
MATCH (e:DataEntity)
RETURN e
LIMIT n
-- 替换 n 为目标数量（例如 10）
""",
"""统计某个业务域下所有应用程序的数量
查询：
MATCH (d:BusinessDomain {name: 'DomainName'})-[:CONTAINS]-(a:Application)
RETURN count(a) AS application_count
-- 替换 DomainName 为目标业务域的名称。
""",
"""查找两个数据实体之间的连接关系
查询：
MATCH (e1:DataEntity {name: 'Entity1'})-[r:RELATED_TO*1..2]->(e2:DataEntity {name: 'Entity2'})
RETURN e1,r,e2
-- 替换 Entity1 和 Entity2 为目标数据实体的名称。
""",
"""查找某个数据实体的所有复制实体。
查询：
MATCH (e1:DataEntity {name: 'EntityName'})-[:FLOWS_TO]-(e2:DataEntity)
RETURN e2
-- 替换 EntityName 为目标数据实体的名称。
"""
]

def generate_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()[:10]

def get_client():
    client = chromadb.PersistentClient(path=settings.get_setting("chromadb.persist_directory"))
    return client

def get_collection():
    client = get_client()
    cn = settings.get_setting("chromadb.cypher_collection")
    if cn in client.list_collections():
        client.delete_collection(cn)
    
    collection = client.create_collection(cn, embedding_function=GTEEmbeddingFunction())

    return collection

def add_examples():
    collection = get_collection()
    collection.add(
        documents=examples,
        ids= [generate_hash(example) for example in examples]
    )

def main():
    add_examples()

if __name__ == "__main__":
    main()
