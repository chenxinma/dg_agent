EXAMPLES = """
## 参考以下示例生成Cypher查询语句。

需求：查询某个数据实体对应的物理表
查询：
MATCH (e:DataEntity {name: 'EntityName'})-[:IMPLEMENTS]->(t:PhysicalTable)
RETURN e, t
-- 替换 EntityName 为目标数据实体的名称。

需求：查找与某个数据实体对应的物理表名称
查询：
MATCH (e:DataEntity {name: 'EntityName'})-[:IMPLEMENTS]->(t:PhysicalTable)
RETURN t.full_table_name as full_table_name
-- 替换 EntityName 为目标数据实体的名称。

需求：查询两个关联实体及其物理表
查询：
MATCH (e1:DataEntity {name: 'EntityName'})-[r]->(e2:DataEntity),
    (e1)-[:IMPLEMENTS]->(t1:PhysicalTable),
    (e2)-[:IMPLEMENTS]->(t2:PhysicalTable)
RETURN e1, e2, r, t1, t2
-- 替换 EntityName 为目标数据实体的名称。

需求：查询某个应用关联的所有数据实体
查询：
MATCH (app:Application {name: 'ApplicationName'})-[r]-(e:DataEntity)
RETURN app, e
-- 替换 ApplicationName 为目标应用程序的名称。

需求：查询某个应用关联的所有数据实体及其物理表
查询：
MATCH (app:Application {name: 'ApplicationName'})-[r]-(e:DataEntity)-[:IMPLEMENTS]->(t:PhysicalTable)
RETURN app, e, t
-- 替换 ApplicationName 为目标应用程序的名称。

需求：查找业务域下的所有实体
查询：
MATCH (d:BusinessDomain {name: 'DomainName'})-[:CONTAINS]-(a:Application)-[r]-(e:DataEntity)
RETURN e
-- 替换 DomainName 为目标业务域的名称。

需求：列出前 n 个数据实体
查询：
MATCH (e:DataEntity)
RETURN e
LIMIT n
-- 替换 n 为目标数量（例如 10）

需求：统计某个业务域下所有应用程序的数量
查询：
MATCH (d:BusinessDomain {name: 'DomainName'})-[:CONTAINS]-(a:Application)
RETURN count(a) AS application_count
-- 替换 DomainName 为目标业务域的名称。

需求：查找两个数据实体之间的连接关系。
查询：
MATCH (e1:DataEntity {name: 'Entity1'})-[r:RELATED_TO*1..2]->(e2:DataEntity {name: 'Entity2'})
RETURN e1,r,e2
-- 替换 Entity1 和 Entity2 为目标数据实体的名称。

需求：查找某个数据实体的所有复制实体。
查询：
MATCH (e1:DataEntity {name: 'EntityName'})-[:FLOWS_TO]-(e2:DataEntity)
RETURN e2
-- 替换 EntityName 为目标数据实体的名称。
"""