"""Configuration loaded from environment variables.

Reads CQ_BASE_URL and CQ_CANDIDATE_KEY from the process environment. If they
are not already set, values are loaded from a .env file next to this module
(a minimal parser, no third-party dependency). Real environment variables
always win over .env.
"""

import os
from pathlib import Path

_ENV_PATH = Path(__file__).resolve().parent / ".env"


def _load_dotenv(path: Path) -> None:
    """Populate os.environ from a .env file without overriding existing vars."""
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"{name} is not set. Add it to the environment or to {_ENV_PATH}."
        )
    return value


_load_dotenv(_ENV_PATH)

CQ_BASE_URL = _require("CQ_BASE_URL").rstrip("/")
CQ_CANDIDATE_KEY = _require("CQ_CANDIDATE_KEY")
