import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def save_to_file(file_name: str, data, directory: str = None):
    try:
        if directory is not None:
            Path.mkdir(Path(directory), parents=True, exist_ok=True)
        else:
            directory = Path.cwd()

        with Path.open(Path(directory) / file_name, "w", encoding="utf-8") as f:
            f.write(data)
    except (OSError, IOError) as e:
        logger.error("Failed to save file %s to %s: %s", file_name, directory, e)
        raise


def load_from_file(file_name):
    try:
        with Path.open(Path(file_name), "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        logger.error("File not found: %s", file_name)
        raise
    except (OSError, IOError) as e:
        logger.error("Failed to read file %s: %s", file_name, e)
        raise
