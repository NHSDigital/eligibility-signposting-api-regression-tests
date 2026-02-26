# Smoke Test Data Paths
SMOKE_TEST_DATA: str = "data/dynamoDB/smokeTestData/"
SMOKE_TEST_RESPONSES: str = "data/responses/smokeTestResponses/"
SMOKE_TEST_CONFIGS: str = "data/configs/smokeTestConfigs"
# Story Test Data Paths
STORY_TEST_DATA: str = "data/dynamoDB/storyTestData/"
STORY_TEST_RESPONSES: str = "data/responses/storyTestResponses/"
STORY_TEST_CONFIGS: str = "data/configs/storyTestConfigs"
# Story Test Data Paths
REGRESSION_TEST_DATA: str = "data/dynamoDB/regressionTestData/"
REGRESSION_RESPONSES: str = "data/responses/regressionTestResponses/"
REGRESSION_CONFIGS: str = "data/configs/regressionTestConfigs"

# In Progress Test Data Paths
IN_PROGRESS_TEST_DATA: str = "data/dynamoDB/inProgressTestData/"
IN_PROGRESS_RESPONSES: str = "data/responses/inProgressTestResponses/"
IN_PROGRESS_CONFIGS: str = "data/configs/inProgressTestConfigs"

# Vita Integration Test Data Paths
VITA_INTEGRATION_TEST_DATA: str = "data/dynamoDB/vitaIntegrationTestData/"
VITA_INTEGRATION_RESPONSES: str = "data/responses/vitaIntegrationTestResponses/"
VITA_INTEGRATION_CONFIGS: str = "data/configs/vitaIntegrationTestConfigs"

# NBS Integration Test Data Paths
NBS_INTEGRATION_TEST_DATA: str = "data/dynamoDB/nbsIntegrationTestData/"
NBS_INTEGRATION_RESPONSES: str = "data/responses/nbsIntegrationTestResponses/"
NBS_INTEGRATION_CONFIGS: str = "data/configs/nbsIntegrationTestConfigs"

# Performance Test Data Paths
PERFORMANCE_TEST_DATA: str = "data/dynamoDB/performanceTestData/"
PERFORMANCE_TEST_CONFIGS: str = "data/configs/performanceTestConfigs"

# Consumer Mapping file - this is a single one for all environments but this may change
CONSUMER_MAPPING_FILE = "data/configs/consumerMappings/consumer_mapping_config.json"

# Location of Integration Test Configs
INT_TEST_CONFIG_PATHS: list[str] = [
    "data/configs/nbsIntegrationTestConfigs/NBS_RSV_Config_April2026_v0.7WIP.json",
    "data/configs/nbsIntegrationTestConfigs/NBS_COVID_Config_Spring2026_v0.4WIP.json",
    "data/configs/vitaIntegrationTestConfigs/RSV_Config_v2.0WIP_v0.2.json",
]
