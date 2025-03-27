# ekeparser module

This directory contains the code for parsing binary content of the messages sent by EKE device.

## Usage

TODO


## Content

- `main.py`: Module that can be used to run ekeparser on its own.
- `config.py`: Configuration, which can be used to limit the fields which are parsed from messages, or to limit certain message types.
- `ekeparser.py`: The endpoint for building schema parser. Having the function `parse_eke_data`, which is the starting point for converting binary data to Python dicts.
- `schemas/`: Directory for schema parsers

## Developing schemas

Schema object are class objects which have `FIELDS`-class variable to contain parsers for fields, and `parse_content`-method which parses binary data. All message types have different kind of Schema objects, but some of them are not yet implemented (having just the base schema class configured) .
https://github.com/HSLdevcom/ajoaikadata/blob/ec97f624969e50d47032843c51893d88dff93194/src/ekeparser/schemas/eke_message.py#L17-L28

Field parsing is configured as 'FieldParser`-objects, which have information about: 
- field names to be parsed (can be multiple)
- start and end byte index (inclusive end, meaning that `start: 0, end : 1` parses 2 bytes, 0 and 1)
- parser function, that takes bytes as input and returns the tuple of field contents

Example:
```
 FieldParser(["msg_type", "msg_name", "msg_version", "ntp_time_valid"], 0, 1, header_parser),
```
This field parser reads two bytes from the data (0-1), sends them to `header_parser`function and which returns values `["msg_type", "msg_name", "msg_version", "ntp_time_valid"]`

To parse new field from the binary data content:
- create a new FieldParser object to FIELDS -list, or add the field to the existing FieldParser.
- select the suitable parser function for it, or modify the current parser to be able to parse the data.
