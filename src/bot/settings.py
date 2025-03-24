"""bot settings"""
from typing import Dict, Any
from pathlib import Path
import yaml
from typing import Optional

SETTING_FILE = "settings.yaml"
setting_files = [
    Path(SETTING_FILE),
    Path.home().joinpath(SETTING_FILE),
]

class Settings:
    """settings"""
    def __init__(self, path: str | None = None) -> None:
        # Bug 修复：将类型从 Path 改为 Optional[Path]，允许赋值为 None
        self.path: Optional[Path] = None
        files = setting_files
        if path:
            files.insert(0, Path(path))
        for p in setting_files:
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
        keys = key.split('.')
        value = self.settings
        for k in keys:
            value = value[k]
        if isinstance(value, str):
            return value
        raise ValueError(f"Expected a string value for key '{key}', but got {type(value).__name__} instead.")

settings = Settings()
