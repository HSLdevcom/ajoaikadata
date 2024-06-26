SINK = "DummySink"  # ConsoleSink, DummySink, PostgresSink
POSTGRES_CONN_STR = "postgresql://postgres:password@localhost:5433"
HEADER_SETTINGS = {"limit_fields": ["msg_type", "msg_name", "eke_timestamp", "ntp_timestamp", "ntp_time_valid"]}
SCHEMA_SETTINGS = {
    1: {},
    2: {"ignore": True},
    3: {},
    4: {},
    5: {},
    6: {"ignore": True},
    7: {},
    8: {"ignore": True},
    9: {"ignore": True},
    10: {},
}
