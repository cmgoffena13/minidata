from poldantic import to_polars_schema as poldantic_to_polars_schema
from pydantic import BaseModel, ConfigDict


class BaseSchema(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    @classmethod
    def to_polars_schema(cls):
        return poldantic_to_polars_schema(cls)


class SourceConfig(BaseModel):
    name: str
    source_schema: type[BaseSchema]
    grain: list[str]
    partition_by: list[str] | None = None
