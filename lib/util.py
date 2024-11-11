import enum
import logging
import logging.config
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Environment(enum.Enum):
    DEV = "dev"
    CI = "ci"
    PROD = "prod"


@dataclass(frozen=True)
class Config:
    root_dir = Path(__file__).parent.parent
    data_dir = root_dir / "data"

    @property
    def env(self) -> Environment:
        if (env_str := os.getenv("ENVIRONMENT")) is None:
            raise EnvironmentError("ENVIRONMENT is not set")
        try:
            return Environment(env_str)
        except ValueError:
            raise EnvironmentError(f"Invalid environment: {env_str}")

    @property
    def gcs_bucket(self) -> str:
        if (bucket := os.getenv("GCS_BUCKET")) is None:
            raise EnvironmentError("GCS_BUCKET is not set")
        return bucket


config = Config()


def setup_logging():
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {'format': '[%(asctime)s] %(levelname)s %(name)s: %(message)s'},
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
            },
            'file': {
                'level': 'INFO',
                'class': 'logging.FileHandler',
                'formatter': 'standard',
                'filename': 'app.log',
                'encoding': 'utf8',
            },
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['console', 'file'],
                'level': 'INFO',
                'propagate': True,
            },
        },
    }

    logging.config.dictConfig(logging_config)


setup_logging()


def getLogger(name: str) -> logging.Logger:
    return logging.getLogger(name)
