from pathlib import Path


def save_to_file(file_name: str, data, directory: str = None):
    if directory is not None:
        Path.mkdir(Path(directory), parents=True, exist_ok=True)
    else:
        directory = Path.cwd()

    with Path.open(Path(directory) / file_name, "w") as f:
        f.write(data)


def load_from_file(file_name):
    with Path.open(file_name, "r") as f:
        return f.read()
