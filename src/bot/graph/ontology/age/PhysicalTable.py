from typing import Annotated, List, Dict
from pydantic import Field

from .Column import Column

from .. import MetaObject

class PhysicalTable(MetaObject):
    """物理表"""
    db_schema: Annotated[str, Field(description="schema")]
    full_table_name: Annotated[str, Field(description="完整表名")]
    table_name: Annotated[str, Field(description="表名")]
    columns: Annotated[List[Column], Field(default=None)]

    @classmethod
    def parse(cls, cell):
        return cls(
                id=f"{cell['id']}", 
                name=cell['name'],
                db_schema=cell['schema'], 
                table_name=cell['table_name'],
                full_table_name=cell['full_table_name'],
                node=cell['label'],
                columns=cell.get('columns', []))

    @classmethod
    def fit(cls, cell) -> bool:
        return cell['label'] == 'PhysicalTable'
