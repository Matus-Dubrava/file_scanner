from pathlib import Path

from md_utils import get_mdm_root_or_exit
from md_models import Config


def validate_cwd_is_in_mdm_repository(config: Config):
    get_mdm_root_or_exit(path=Path.cwd(), config=config)
