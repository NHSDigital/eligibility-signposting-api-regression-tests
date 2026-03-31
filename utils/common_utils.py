import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def save_to_file(file_name: str, data: str, directory: str | None = None) -> None:
    """Write ``data`` to ``directory/file_name``, creating the directory if needed.

    Args:
        file_name: Name of the file to write.
        data: String content to write.
        directory: Optional directory path. Defaults to the current working directory.

    Raises:
        OSError: If the file cannot be written.
    """
    target_dir = Path(directory) if directory is not None else Path.cwd()
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        with (target_dir / file_name).open("w", encoding="utf-8") as f:
            f.write(data)
    except (OSError, IOError) as e:
        logger.error("Failed to save file %s to %s: %s", file_name, target_dir, e)
        raise


def load_from_file(file_name: str) -> str:
    """Read and return the full text content of ``file_name``.

    Args:
        file_name: Path to the file to read.

    Returns:
        The file contents as a string.

    Raises:
        FileNotFoundError: If the file does not exist.
        OSError: If the file cannot be read.
    """
    try:
        with Path(file_name).open("r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        logger.error("File not found: %s", file_name)
        raise
    except (OSError, IOError) as e:
        logger.error("Failed to read file %s: %s", file_name, e)
        raise
