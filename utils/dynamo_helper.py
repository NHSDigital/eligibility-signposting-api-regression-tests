import json
import logging
import os

import boto3
from utils.common_utils import save_to_file, load_from_file
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()
DYNAMO_TEMP_LOCATION = "data/dynamoDB/temp/"


class DynamoDBHelper:
    def __init__(self, table_name, environment):
        # Create DynamoDB resource using credentials from env
        self.environment = environment
        self.table_name = table_name
        self.dynamodb_client = boto3.client("dynamodb", "eu-west-2")
        self.dynamodb_resource = boto3.resource("dynamodb", "eu-west-2")
        self.table = self.dynamodb_resource.Table(table_name)
        self.table_arn = None
        self.attribute_definitions = None
        self.key_schema = None
        self.tags = None

    def insert_item(self, item: dict):
        """
        Insert a single item into the table.
        """
        try:
            response = self.table.put_item(Item=item)
        except ClientError as e:
            logger.exception(
                "Failed to insert item: %s", e.response["Error"]["Message"]
            )
            raise
        else:
            return response

    def insert_items(self, items: list):
        """
        Insert multiple items using batch_writer.
        """
        try:
            with self.table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
        except ClientError as e:
            logger.exception("Batch insert failed: %s", e.response["Error"]["Message"])
            raise
        else:
            logger.info("Batch insert complete.")

    def get_item(self, key: dict):
        """
        Retrieve a single item by primary key.
        """
        try:
            response = self.table.get_item(Key=key)
        except ClientError as e:
            logger.exception("Failed to get item: %s", e.response["Error"]["Message"])
            raise
        else:
            return response.get("Item")

    def delete_item(self, key: dict):
        """
        delete a single item by primary key.
        """
        try:
            response = self.table.delete(Key=key)
        except ClientError as e:
            logger.exception(
                "Failed to delete item: %s", e.response["Error"]["Message"]
            )
            raise e
        else:
            return response

    def describe_table(self):
        logger.debug(f"Describe table: {self.table_name}")
        try:
            table_description = self.dynamodb_client.describe_table(
                TableName=self.table_name
            )["Table"]
        except ClientError as e:
            logger.exception(
                f"Failed to get table information: {e.response["Error"]["Message"]}"
            )
            raise
        self.table_arn = table_description["TableArn"]
        save_to_file(
            f"table_arn-{self.environment}.json",
            self.table_arn,
            directory=DYNAMO_TEMP_LOCATION,
        )
        self.attribute_definitions = table_description["AttributeDefinitions"]
        save_to_file(
            f"attribute_definitions-{self.environment}.json",
            json.dumps(self.attribute_definitions),
            directory=DYNAMO_TEMP_LOCATION,
        )
        self.key_schema = table_description["KeySchema"]
        save_to_file(
            f"key_schema-{self.environment}.json",
            json.dumps(self.key_schema),
            directory=DYNAMO_TEMP_LOCATION,
        )

        return self.table_arn, self.attribute_definitions, self.key_schema

    def get_table_tags(self):
        self.tags = self.dynamodb_client.list_tags_of_resource(
            ResourceArn=self.table_arn
        )["Tags"]
        logger.debug(f"tags: {self.tags}")
        save_to_file(
            f"tags-{self.environment}.json",
            json.dumps(self.tags),
            directory=DYNAMO_TEMP_LOCATION,
        )
        return self.tags

    def set_table_tags(self):
        self.dynamodb_client.tag_resource(ResourceArn=self.table_arn, Tags=self.tags)

    def create_table(self):
        logger.info(f"Creating table '{self.table_name}'...")
        try:
            self.dynamodb_client.create_table(
                TableName=self.table_name,
                KeySchema=self.key_schema,
                AttributeDefinitions=self.attribute_definitions,
                BillingMode="PAY_PER_REQUEST",
            )

            # Wait for the new table to become active
            logger.debug(
                f"Waiting for table '{self.table_name}' to be created and become active..."
            )
            waiter = self.dynamodb_client.get_waiter("table_exists")
            waiter.wait(TableName=self.table_name)
            logger.info(
                f"Table '{self.table_name}' successfully created and is now active."
            )
        except ClientError as e:
            logger.exception(f"Error creating table '{self.table_name}': {e}")

    def delete_table(self, table_name):
        logger.info(f"Deleting table '{table_name}'...")
        self.dynamodb_client.delete_table(TableName=table_name)

        # Wait for the table to be completely deleted
        logger.debug(f"Waiting for table '{self.table_name}' to be deleted...")
        waiter = self.dynamodb_client.get_waiter("table_not_exists")
        waiter.wait(TableName=self.table_name)
        logger.info(f"Table '{self.table_name}' successfully deleted.")


def restore_tags_to_table(dynamo_db_table: DynamoDBHelper):
    if dynamo_db_table.table_arn is None or dynamo_db_table.tags is None:
        logger.warning(
            "Unable to find TableArn or Tags, attempting to load from backup files..."
        )
        try:
            load_information_from_backup_files(dynamo_db_table)
        except FileNotFoundError:
            logger.error("Failed to restore tags and no backup file was found.")
    dynamo_db_table.set_table_tags()


def file_backup_exists(dynamo_db_table: DynamoDBHelper):
    try:
        json.loads(
            load_from_file(
                f"{DYNAMO_TEMP_LOCATION}tags-{dynamo_db_table.environment}.json"
            )
        )
        json.loads(
            load_from_file(
                f"{DYNAMO_TEMP_LOCATION}attribute_definitions-{dynamo_db_table.environment}.json"
            )
        )
        json.loads(
            load_from_file(
                f"{DYNAMO_TEMP_LOCATION}key_schema-{dynamo_db_table.environment}.json"
            )
        )
        load_from_file(
            f"{DYNAMO_TEMP_LOCATION}table_arn-{dynamo_db_table.environment}.json"
        )
        return True
    except FileNotFoundError:
        return False


def reset_dynamo_tables():
    environment = os.getenv("ENVIRONMENT")
    table_name = os.getenv("DYNAMODB_TABLE_NAME")

    logger.info("Resetting DynamoDB. This may take a few moments, please be patient.")
    if environment not in ["dev", "test"]:
        logger.warning(
            f"{environment} is not supported. Resetting DynamoDB is only supported in dev or test."
        )
        return
    dynamo_db_table = DynamoDBHelper(table_name, environment)

    # --- Step 1: Fetch table information ---
    try:
        table_arn, attribute_definitions, key_schema = dynamo_db_table.describe_table()
        logger.debug(f"TableArn: {table_arn}")
        logger.debug(f"Attribute Definitions: {attribute_definitions}")
        logger.debug(f"Key Schema: {key_schema}")

        dynamo_db_table.tags = dynamo_db_table.get_table_tags()
    except ClientError as e:
        logger.warning(f"Error describing table: {e}")
        if not file_backup_exists(dynamo_db_table):
            logger.error(
                f"FATAL! Unable to get table information and no backup present: {e}"
            )
            raise e
        else:
            load_information_from_backup_files(dynamo_db_table)

    # --- Step 2: Delete the table ---
    try:
        dynamo_db_table.delete_table(table_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.warning(
                f"Table '{dynamo_db_table.table_name}' does not exist. Skipping deletion."
            )
        else:
            logger.exception(
                f"Error deleting table '{dynamo_db_table.table_name}': {e}"
            )
            raise e

    # --- Step 3: Recreate the table ---
    dynamo_db_table.create_table()

    # --- Step 4: Restore the tags ---
    logger.debug(f"Adding tags to table '{dynamo_db_table.table_name}'...")
    restore_tags_to_table(dynamo_db_table)


def load_information_from_backup_files(dynamo_db_table: DynamoDBHelper):
    logger.warning("Table information taken from backup files")
    dynamo_db_table.tags = json.loads(
        load_from_file(f"{DYNAMO_TEMP_LOCATION}tags-{dynamo_db_table.environment}.json")
    )
    dynamo_db_table.attribute_definitions = json.loads(
        load_from_file(
            f"{DYNAMO_TEMP_LOCATION}attribute_definitions-{dynamo_db_table.environment}.json"
        )
    )
    dynamo_db_table.key_schema = json.loads(
        load_from_file(
            f"{DYNAMO_TEMP_LOCATION}key_schema-{dynamo_db_table.environment}.json"
        )
    )
    dynamo_db_table.table_arn = load_from_file(
        f"{DYNAMO_TEMP_LOCATION}table_arn-{dynamo_db_table.environment}.json"
    )


_cached_dynamo_helper: "DynamoDBHelper | None" = None


def insert_into_dynamo(data):
    global _cached_dynamo_helper
    logger.debug("Inserting into Dynamo: %s", data)
    environment = os.getenv("ENVIRONMENT")
    dynamodb_table_name = os.getenv("DYNAMODB_TABLE_NAME")

    if (
        _cached_dynamo_helper is None
        or _cached_dynamo_helper.table_name != dynamodb_table_name
    ):
        _cached_dynamo_helper = DynamoDBHelper(dynamodb_table_name, environment)

    table = _cached_dynamo_helper
    for item in data:
        try:
            table.insert_item(item)
            logger.debug("✅ Inserted: %s", item)
        except ClientError as e:
            logger.exception(
                "❌ Failed to insert %s: %s", item, e.response["Error"]["Message"]
            )
