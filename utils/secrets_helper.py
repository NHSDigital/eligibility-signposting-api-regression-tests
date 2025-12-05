import boto3
from typing import Optional
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class SecretsManagerClient:

    def __init__(self, region: str):
        self.region = region
        self.client = boto3.client("secretsmanager", region_name=region)

    def get_secret(
        self, secret_name: str, version_stage: str = "AWSCURRENT"
    ) -> Optional[bytes]:

        try:
            response = self.client.get_secret_value(
                SecretId=secret_name, VersionStage=version_stage
            )

            if "SecretString" in response:
                return response["SecretString"].encode()
            if "SecretBinary" in response:
                return response["SecretBinary"]

        except self.client.exceptions.ResourceNotFoundException:
            logger.warning(
                "Secret '%s' (stage: %s) not found.", secret_name, version_stage
            )
            return None

        except Exception as e:
            logger.exception(
                "Error retrieving secret '%s' (stage: %s): %s",
                secret_name,
                version_stage,
                e,
            )
            raise

        return None

    def set_secret(self, secret_name: str, secret_value: str) -> None:
        try:
            # Try update first
            self.client.put_secret_value(
                SecretId=secret_name, SecretString=secret_value
            )
            logger.info("Updated existing secret '%s'", secret_name)

        except self.client.exceptions.ResourceNotFoundException:
            # If not exists, create it
            self.client.create_secret(Name=secret_name, SecretString=secret_value)
            logger.info("Created new secret '%s'", secret_name)

    def delete_secret(self, secret_name: str, recovery_days: int = 7) -> None:

        try:
            self.client.delete_secret(
                SecretId=secret_name, RecoveryWindowInDays=recovery_days
            )
            logger.info(
                "Deleted secret '%s' (recoverable for %d days)",
                secret_name,
                recovery_days,
            )

        except Exception as e:
            logger.exception("Failed to delete secret '%s': %s", secret_name, e)
            raise

    def get_secret_key_versions(self, secret_name: str) -> dict[str, Optional[bytes]]:

        stages = ["AWSCURRENT", "AWSPREVIOUS"]
        results: dict[str, Optional[bytes]] = {
            "AWSCURRENT": None,
            "AWSPREVIOUS": None,
        }

        for stage in stages:
            try:
                response = self.client.get_secret_value(
                    SecretId=secret_name, VersionStage=stage
                )

                if "SecretString" in response and response["SecretString"]:
                    results[stage] = response["SecretString"].encode()
                elif "SecretBinary" in response and response["SecretBinary"]:
                    results[stage] = response["SecretBinary"]
                else:
                    logger.warning(
                        "Secret '%s' (%s) has no usable value", secret_name, stage
                    )

            except self.client.exceptions.ResourceNotFoundException:
                logger.warning(
                    "Secret '%s' with stage '%s' not found", secret_name, stage
                )

            except Exception as e:
                logger.exception(
                    "Error retrieving '%s' (%s): %s",
                    secret_name,
                    stage,
                    e,
                )

        # Fatal error if both missing
        if not results["AWSCURRENT"] and not results["AWSPREVIOUS"]:
            logger.critical(
                "Fatal: Neither AWSCURRENT nor AWSPREVIOUS exists for '%s'", secret_name
            )
            raise RuntimeError(
                f"No AWSCURRENT or AWSPREVIOUS secret exists for '{secret_name}'"
            )

        return results
