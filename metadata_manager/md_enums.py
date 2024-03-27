import enum
from typing import Optional


@enum.unique
class FileStatus(enum.Enum):
    ACTIVE = "ACTIVE"
    REMOVED = "REMOVED"
    UNTRACKED = "UNTRACKED"
    TRACKED_IN_SUBREPOSITORY = "TRACKED_IN_SUBREPOSITORY"

    def to_str(self) -> str:
        match self:
            case FileStatus.ACTIVE:
                return "ACTIVE"
            case FileStatus.REMOVED:
                return "REMOVED"
            case FileStatus.UNTRACKED:
                return "UNTRACKED"
            case FileStatus.TRACKED_IN_SUBREPOSITORY:
                return "TRACKED IN SUBREPOSITORY"
            case _:
                raise Exception("Unreachable")


@enum.unique
class BuildType(enum.Enum):
    PROD = "PROD"
    DEV = "DEV"

    @staticmethod
    def from_str(build_type_str: str) -> Optional["BuildType"]:
        if build_type_str.upper() == BuildType.PROD.value:
            return BuildType.PROD
        elif build_type_str.upper() == BuildType.DEV.value:
            return BuildType.DEV
        else:
            return None
