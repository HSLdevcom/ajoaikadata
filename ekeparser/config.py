SINK = "DummySink"  # ConsoleSink, DummySink, PostgresSink
POSTGRES_CONN_STR = "postgresql://postgres:password@localhost:5433"
HEADER_SETTINGS = {
    "limit_fields": ["msg_type", "msg_name", "eke_timestamp"]
}
SCHEMA_SETTINGS = {
    1: {"ignore": True},
    2: {"ignore": True},
    3: {"ignore": True},
    4: {"ignore": True},
    5: {},
    6: {"ignore": True},
    7: {"ignore": True},
    8: {"ignore": True},
    9: {"ignore": True},
    10: {"ignore": True},
}
