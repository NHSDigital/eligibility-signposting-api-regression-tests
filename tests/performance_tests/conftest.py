import pytest


def pytest_addoption(parser):
    """Performance-test-specific CLI options.

    This conftest only affects tests under tests/performance_tests/.
    """

    group = parser.getgroup("performance")

    group.addoption(
        "--perf-users",
        action="store",
        type=str,
        default=10,
        help="Number of concurrent locust users (default: 10)",
    )
    group.addoption(
        "--perf-spawn-rate",
        action="store",
        type=str,
        default=2,
        help="Locust spawn rate (users per second) (default: 2)",
    )
    group.addoption(
        "--perf-run-time",
        action="store",
        default="10s",
        help="Locust run time (e.g. 10s, 2m, 1h) (default: 10s)",
    )


@pytest.fixture(scope="session")
def perf_users(request) -> str:
    return str(request.config.getoption("--perf-users"))


@pytest.fixture(scope="session")
def perf_spawn_rate(request) -> str:
    return str(request.config.getoption("--perf-spawn-rate"))


@pytest.fixture(scope="session")
def perf_run_time(request) -> str:
    return str(request.config.getoption("--perf-run-time"))
