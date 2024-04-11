from pathlib import Path
from typing import Any

from sqlalchemy.types import TypeDecorator
from sqlalchemy import String, Dialect


class PathType(TypeDecorator):
    impl = String

    cache_ok = True

    def process_bind_param(self, value: Any | None, dialect: Dialect) -> Any:
        if isinstance(value, Path):
            return str(value)
        return value

    def process_result_value(self, value: Any | None, dialect: Dialect) -> Any | None:
        return Path(value) if value else None
