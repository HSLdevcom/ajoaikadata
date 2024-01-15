""" Helper module for configuration related stuff """

from itertools import zip_longest
import logging
import os
from typing import Iterable, Tuple

# Init logging
log_level = os.environ.get("LOG_LEVEL", logging.INFO)  # Use INFO as default.
logging.basicConfig(level=log_level, format="%(levelname)s:%(module)s:%(message)s")
logger = logging.getLogger()


def read_from_env(env_names: Iterable[str], defaults: Iterable[str] = (), required: bool = True) -> Tuple[str, ...]:
    """
    Get env variables by names. If required is False, missing variables are returned as empty strings.
    If True, ValueError is raised. Defaults to True.
    """
    envs = tuple(os.environ.get(name, default) for (name, default) in zip_longest(env_names, defaults, fillvalue=""))
    if required and not all(envs):
        raise ValueError(f"Missing some of envs {env_names}")
    env_str = "\n".join([f"{name}={value}" for (name, value) in zip(env_names, envs)])
    logging.info(f"Read the following env variables:\n{env_str}")
    return envs
