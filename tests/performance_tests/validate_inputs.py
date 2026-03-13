import os
import re
import sys


MAX_USERS = 400
MAX_SPAWN_RATE = 100
MAX_RUN_TIME_SECONDS = 1800  # 30 minutes


def fail(title: str, message: str) -> None:
    print(f"::error title={title}::{message}")
    sys.exit(1)


def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        fail("Missing input", f"{name} was not provided.")
    return value


def validate_range(name: str, value: int, min_value: int, max_value: int) -> None:
    if value < min_value or value > max_value:
        fail(
            f"{name} out of range",
            f"{name} must be between {min_value} and {max_value}. Got: {value}",
        )


def parse_run_time_to_seconds(run_time: str) -> int:
    match = re.fullmatch(r"(\d+)([sm])", run_time)
    if not match:
        fail(
            "Invalid run_time format",
            "run_time must look like 30s or 5m (seconds or minutes only). "
            f"Got: '{run_time}'",
        )

    value = int(match.group(1))
    unit = match.group(2)

    if unit == "s":
        return value
    return value * 60


def main() -> None:
    users = int(get_env("USERS"))
    spawn_rate = int(get_env("SPAWN_RATE"))
    run_time = get_env("RUN_TIME")

    validate_range("users", users, 1, MAX_USERS)
    validate_range("spawn_rate", spawn_rate, 1, MAX_SPAWN_RATE)

    run_time_seconds = parse_run_time_to_seconds(run_time)
    if run_time_seconds > MAX_RUN_TIME_SECONDS:
        fail(
            "Run time too long",
            f"run_time must be 30m or less. Got: {run_time}",
        )

    print("Performance test inputs validated successfully.")


if __name__ == "__main__":
    main()
