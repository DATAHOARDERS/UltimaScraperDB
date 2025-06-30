from pathlib import Path

ALEMBICA_PATH = (
    Path(__file__).parent.resolve().joinpath("databases/ultima_archive").as_posix()
)
