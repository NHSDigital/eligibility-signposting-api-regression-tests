from tests import test_config
from utils.s3_config_manager import upload_configs_to_s3


def test_reset_db():
    configs = test_config.INT_TEST_CONFIG_PATHS
    upload_configs_to_s3(configs)
