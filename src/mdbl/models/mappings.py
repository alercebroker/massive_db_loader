from pydantic import BaseModel, Field

# Using `from_` because `from` is a reserved keyword, same with schema_


class ColumnMapping(BaseModel):
    from_: str = Field(alias="from")
    to: str = Field()


class TableMappings(BaseModel):
    from_: str = Field(alias="from")
    to: str = Field()
    columns: list[ColumnMapping] = []


class DBMappings(BaseModel):
    tables: list[TableMappings] = []
