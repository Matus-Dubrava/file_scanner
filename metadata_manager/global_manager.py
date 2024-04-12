from models.local_models import Config
import md_utils


class GlobalManager:
    def __init__(self, config: Config):
        self.dir_path, self.db_path = md_utils.get_global_paths(config)
