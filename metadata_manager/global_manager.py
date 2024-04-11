from pathlib import Path

from models.local_models import Config


class GlobalManager:
    def __init__(self, config: Config):
        # Set path to global database. By Default, user's home directory
        # will be used.
        if config.global_path.startswith("~"):
            self.dir_path = Path(config.global_path).expanduser()
        else:
            self.dir_path = Path(config.global_path)

        self.db_path = self.dir_path.joinpath(config.global_db_name)
