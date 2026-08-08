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
class GEEConfig:
    """Google Earth Engine bronze backend (alternative to CDS; see docs/gee_setup.md).

    All fields are optional at load time so CDS-only users need no GEE setup — the GEE
    client (``src/gee/client.py``) validates what it actually needs at use time.
    ``service_account_file`` is the path to the SA JSON key for headless/Airflow auth; if
    unset, the client falls back to stored user OAuth creds (local dev).
    """

    project: str | None
    service_account_file: str | None
    gcs_bucket: str | None
    gcs_prefix: str
    tz_asset: str | None  # EE FeatureCollection of tz polygons w/ `offset` (min); §5.3


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
    gee: GEEConfig
    paths: PathsConfig


def resolve_bronze_dir(data_root: str | Path | None = None) -> Path:
    """Bronze directory for an optional per-run ``data_root`` override.

    Lets a DAG run point at a different data root (e.g. one country per folder) from its
    trigger config alone — no container recreate, no ``.env`` edit. With ``data_root``
    unset (the common case) this is exactly ``load_config().paths.bronze_dir``, so
    existing single-region runs are unaffected. Each root carries its own ``bronze/`` tree
    and its own manifest, so regions never collide or overwrite each other.

    **Pass the folder name.** ``data_root="TO"`` means ``$DATA_DIR/TO`` — the mounted volume
    (default ``/data``; on the Pi, the SSD). An absolute path under ``$DATA_DIR`` also works
    and means the same thing, so ``"TO"`` and ``"/data/TO"`` are interchangeable.

    A bare name is anchored to ``$DATA_DIR`` rather than to the process's working directory,
    which is the whole point. Anchoring it to the CWD is what silently put a full year of
    CHIRPS bronze in ``/opt/airflow/TO/bronze`` — off the bind mount, DAG green, one
    ``docker compose down`` from gone, and undiscoverable until it was. The subsequent
    "must be absolute" attempt only moved the trap: ``"/TO"`` is absolute, is the container
    root, and died in ``mkdir`` with ``PermissionError`` after the task had started.

    Anything that lands outside ``$DATA_DIR`` is refused, since bronze written there does
    not survive a container recreate. That is a lexical prefix check, not a filesystem
    probe, so this stays a pure function of its inputs and behaves identically on the Pi, in
    CI, and in a test. To use a different disk, point ``DATA_DIR`` at it.
    """
    if data_root:
        # normpath, not resolve(): collapses ".." lexically without touching the filesystem
        # or following symlinks, so "../TO" and "/data/../TO" both land outside and are
        # caught by the check below rather than escaping it.
        data_dir = Path(os.path.normpath(os.environ.get("DATA_DIR", "/data")))
        root = Path(os.path.normpath(data_root))
        if not root.is_absolute():
            root = Path(os.path.normpath(data_dir / root))
        if root != data_dir and not root.is_relative_to(data_dir):
            raise ValueError(
                f"data_root {str(data_root)!r} resolves to {str(root)!r}, outside DATA_DIR "
                f"({str(data_dir)!r}) — bronze written there is not on the mounted volume "
                "and is lost when the container is recreated. Pass a folder name, e.g. "
                f"{root.name!r}, which means {str(data_dir / root.name)!r}."
            )
        return root / "bronze"
    return load_config().paths.bronze_dir


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
        gee=GEEConfig(
            project=os.getenv("GEE_PROJECT") or None,
            service_account_file=os.getenv("GEE_SERVICE_ACCOUNT_FILE") or None,
            gcs_bucket=os.getenv("GEE_GCS_BUCKET") or None,
            gcs_prefix=os.getenv("GEE_GCS_PREFIX", "bronze-gee"),
            tz_asset=os.getenv("GEE_TZ_ASSET") or None,
        ),
        paths=PathsConfig(
            data_dir=Path(os.getenv("DATA_DIR", "/data")),
        ),
    )
