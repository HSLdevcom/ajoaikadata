from typing import Any, Callable, List, Literal, Union


class FieldParser:
    """
    A helper class for parsing the fields.

    Attributes:
    - field_names: Names where parsed data should be stored.
    - start_byte: The first byte index of content to be sent to parser, starting from 0.
    - end_byte: The last byte index of content to be sent to parser. If only one byte will be read, use `end_byte = start_byte`.
    - parser_function: The function that will be used to parse the content.
        Note that the parser should return tha same amount of values in the same exact order than defined in `field_names`.
    """

    def __init__(self, field_names: List[str], start_byte: int, end_byte: int, parser_function: Callable) -> None:
        self.field_names = field_names
        self.start_byte = start_byte
        self.end_byte = end_byte
        self.parser_function = parser_function


class DataContentParser:
    """
    A helper class for parsing the data content.

    Attributes:
    - start_byte: The first byte index of content to be sent to parser. The rest of the message will be passed to the content.
    - schema_mapping: Dict to get the right schema for data. If only one schema is possible, do not use the dict and keep `selector_field` empty.
        Use `None` to skip parsing and store only raw data.
    - selector_field: The field name of the header that will be used to select the right schema from mapping. Use `None` if only one schema is possible.
    - unpack_to_header_level: Whether to unpack data fields to the same level than other fields,
        or to store them as a separated object under the field name defined by `data_field_name`.
    """

    def __init__(
        self,
        start_byte: int,
        schema_mapping: Union[dict[Any, "Schema"], "Schema", None] = None,
        selector_field: str | None = None,
        unpack_to_header_level: bool = False,
        data_field_name: str = "content",
    ) -> None:
        self.start_byte = start_byte
        self.schema_mapping = schema_mapping
        self.selector_field = selector_field
        self.unpack_to_header_level = unpack_to_header_level
        self.data_field_name = data_field_name


class Schema:
    """Base class for schemas. Methods to parse bytes to a object."""

    FIELDS: List[FieldParser] = []
    DATA_CONTENT: DataContentParser | None = None

    def __init__(
        self,
        limit_fields: List[str] | None = None,
        limit_type: Literal["include", "exclude"] | None = "include",
        ignore: bool = False,
    ) -> None:
        self.limit_fields = limit_fields
        self.limit_type = limit_type or "include"
        self.ignore = ignore

    def parse_content(self, content: bytes) -> dict | None:
        parsed = {}

        # Parse fields first
        for field in self.FIELDS:
            fields_to_parse = field.field_names
            if self.limit_fields and self.limit_type == "include":
                fields_to_parse = [f for f in field.field_names if f in self.limit_fields]
            if self.limit_fields and self.limit_type == "exclude":
                fields_to_parse = [f for f in field.field_names if f not in self.limit_fields]

            if not fields_to_parse:
                continue

            field_content = content[field.start_byte : field.end_byte + 1]

            try:
                values = field.parser_function(field_content)
                # Iterate over values and store them. Values might not be tuple, so wrap it in one if necessary
                for f, value in zip(field.field_names, values if type(values) is tuple else (values,)):
                    if f not in fields_to_parse:
                        # Field might have been filtered
                        continue
                    parsed[f] = value
            except ValueError as e:
                # Attach extra information to the error to make it easier to debug schema parsers
                raise ValueError(
                    f"{e} Problem occured while parsing {field.field_names} from {field_content} in {self.__class__}"
                ) from e

        # Parse data content, if configured so
        if self.DATA_CONTENT:
            # Assume the rest of the payload belongs to data schema
            data_content = content[self.DATA_CONTENT.start_byte :]

            if not self.DATA_CONTENT.schema_mapping:
                # Store just raw data
                parsed[self.DATA_CONTENT.data_field_name] = data_content

            else:
                # Find the schema
                if isinstance(self.DATA_CONTENT.schema_mapping, Schema):
                    schema = self.DATA_CONTENT.schema_mapping
                elif isinstance(self.DATA_CONTENT.schema_mapping, dict) and self.DATA_CONTENT.selector_field:
                    schema = self.DATA_CONTENT.schema_mapping.get(parsed[self.DATA_CONTENT.selector_field])
                else:
                    raise TypeError(
                        "Invalid configuration. `schema_mapping` should be either a single class, or if dict, `selector_field` should be given."
                    )
                
                # If schema should be ignored, do not send any data
                if schema and schema.ignore:
                    return None
                
                # Data content if received, otherwise empty dict
                data = schema.parse_content(data_content) or {} if schema else {}

                if self.DATA_CONTENT.unpack_to_header_level:
                    parsed = {**parsed, **data}
                else:
                    parsed[self.DATA_CONTENT.data_field_name] = data

        return parsed
