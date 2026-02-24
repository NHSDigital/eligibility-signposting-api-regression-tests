"""Preloads all DynamoDB test data from all suites in one step.

Runs after clear-db and before individual test suites.
Uses batch writes for efficiency (~34 batch calls instead of ~850 individual puts).
"""

from tests import test_config
from utils.data_helper import preload_all_dynamo_data


def test_preload_all_dynamo_data():
    preload_all_dynamo_data(
        [
            test_config.STORY_TEST_DATA,
            test_config.VITA_INTEGRATION_TEST_DATA,
            test_config.NBS_INTEGRATION_TEST_DATA,
        ]
    )
