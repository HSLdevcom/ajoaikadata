from .config import read_from_env

(app,) = read_from_env(("APP_NAME",))


match app:
    case "contentparser":
        from .flows.contentparser import flow
    case "eventcreator":
        from .flows.eventcreator import flow
    case "pgsink":
        from .flows.pgsink import flow
    case "reader":
        from .flows.reader import flow
    case _:
        raise ValueError(f"Unknown app: {app}")
