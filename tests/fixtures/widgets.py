from src.sources.base import BaseSchema, SourceConfig


class WidgetSchema(BaseSchema):
    widget_id: int
    color: str


WIDGET_ROWS = {"widget_id": [1, 2, 3], "color": ["red", "blue", "red"]}

WIDGET_SOURCE = SourceConfig(
    name="widgets",
    source_type="inline",
    source_schema=WidgetSchema,
    grain=["widget_id"],
)
