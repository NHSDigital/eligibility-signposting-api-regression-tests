from pathlib import Path


def save_to_file(file_name, data, directory=None):
    if directory is not None:
        Path.mkdir(directory, parents=True, exist_ok=True)
    else:
        directory = Path.cwd()

    with Path.open(directory / file_name, "w", encoding="utf-8") as f:
        f.write(data)


def load_from_file(file_name):
    with Path.open(file_name, "r", encoding="utf-8") as f:
        return f.read()
