"""bot settings"""
from pathlib import Path
import yaml

SETTING_FILE = "settings.yaml"
setting_files = [
    Path(SETTING_FILE),
    Path.home().joinpath(SETTING_FILE),
]

class Settings:
    """settings"""
    def __init__(self, path: str = None) -> None:
        self.path:Path = None
        files = setting_files
        if path:
            files.insert(0, Path(path))
        for p in setting_files:
            print("settings:", p)
            if p.exists():
                self.path = p.absolute()
                break
        self.settings = self.load_settings()

    def load_settings(self) -> dict:
        """load settings"""
        if not self.path:
            return {}
        with self.path.open(mode="r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def get_setting(self, key: str) -> str:
        """get setting"""
        return self.settings[key]

settings = Settings()
