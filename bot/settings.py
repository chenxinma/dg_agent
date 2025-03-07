import yaml

class Settings:
    def __init__(self, path: str = "settings.yaml") -> None:
        self.path = path
        self.settings = self.load_settings()
        
    def load_settings(self) -> dict:
        with open(self.path, "r") as f:
            return yaml.safe_load(f)
        
    def get_setting(self, key: str) -> str:
        return self.settings[key]

settings = Settings()