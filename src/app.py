from .config import read_from_env

(app,) = read_from_env(("APP_NAME",))


match app:
    case "contentparser":
        from .flows.contentparser.contentparser import flow
    case "eventcreator":
        from .flows.eventcreator.eventcreator import flow
    case "pgsink":
        from .flows.pgsink.pgsink import flow
    case "reader":
        from .flows.reader.reader import flow
    case _:
        raise ValueError(f"Unknown app: {app}")
