"""Runtime configuration loaded from ``.env`` (PLANNING.md §13).

``.env`` is the single place a user edits to get started. Real ``.env`` is
git-ignored; ``.env.example`` is the committed template.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.getenv(key)
    if val is None or val == "":
        raise RuntimeError(f"Missing required env var {key!r} (see .env.example)")
    return val


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    db: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )


@dataclass(frozen=True)
class CDSConfig:
    url: str
    key: str


@dataclass(frozen=True)
class PathsConfig:
    """On-disk layout (PLANNING.md §7). ``DATA_DIR`` defaults to ``/data`` (the SSD
    mount on the Pi target); override via env for local runs outside the container.
    """

    data_dir: Path

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def bronze_static_dir(self) -> Path:
        """Holds the two static .nc inputs: geopotential + ERA5-Land mask (§6.4)."""
        return self.bronze_dir / "static"


@dataclass(frozen=True)
class Config:
    postgres: PostgresConfig
    cds: CDSConfig
    paths: PathsConfig


def load_config() -> Config:
    """Build a Config from the current environment."""
    return Config(
        postgres=PostgresConfig(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            db=os.getenv("POSTGRES_DB", "era5"),
            user=os.getenv("POSTGRES_USER", "era5"),
            password=_require("POSTGRES_PASSWORD"),
        ),
        cds=CDSConfig(
            url=os.getenv("CDS_URL", "https://cds.climate.copernicus.eu/api"),
            key=_require("CDS_KEY"),
        ),
        paths=PathsConfig(
            data_dir=Path(os.getenv("DATA_DIR", "/data")),
        ),
    )
