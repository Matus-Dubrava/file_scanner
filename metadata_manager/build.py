import subprocess
from datetime import datetime
from pathlib import Path
import argparse
import sys
from typing import Optional
import os

from md_enums import BuildType
from md_models import VersionInfo


def validate_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    args = parser.parse_args()

    maybe_build_type = BuildType.from_str(args.build_type)
    if not maybe_build_type:
        print(
            f"Failed to parse build type. Allowed values: {[member.value for member in list(BuildType)]}",
            file=sys.stderr,
        )
        sys.exit(1)

    args.build_type = maybe_build_type
    return args


def write_build_info(version: str, build_type: BuildType) -> Optional[Exception]:
    try:
        version_path = Path(__file__).parent / "version.json"
        os.chdir(Path(__file__).parent)
        commit_id = (
            subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
        )

        version_info = VersionInfo(
            version=version,
            commit_id=commit_id,
            build_type=build_type,
            build_date=datetime.now(),
        )

        with open(version_path, "w") as f:
            f.write(version_info.model_dump_json())
    except Exception as err:
        return err

    return None


def build(version: str, build_type: BuildType) -> Optional[Exception]:
    # TODO: This is just a draft version to pupulate version info
    maybe_err = write_build_info(version=version, build_type=build_type)
    if maybe_err:
        return maybe_err

    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--build-type", required=True)
    args = validate_args(parser)
    maybe_err = build(version=args.version, build_type=args.build_type)
    if maybe_err:
        print(maybe_err, file=sys.stderr)
        print("Build failed.", file=sys.stderr)
        sys.exit(1)
