from pydantic import BaseModel, Field

# Using `from_` because `from` is a reserved keyword


class ColumnMapping(BaseModel):
    from_: str = Field(alias="from")
    to: str = Field()


class TableMappings(BaseModel):
    from_: str = Field(alias="from")
    to: str = Field()
    columns: list[ColumnMapping] = []


class DBMappings(BaseModel):
    tables: list[TableMappings] = []


a = {
    "tables": [
        {
            "from": "parquet_obj",
            "to": "obj",
            "columns": [{"from": "col_tabla", "to": "col_db"}],
        },
        {
            "from": "parquet_det",
            "to": "det",
            "columns": [
                {"from": "col_tabla", "to": "col_db"},
                {"from": "col_tabla", "to": "col_db"},
                {"from": "col_tabla", "to": "col_db"},
            ],
        },
    ]
}
