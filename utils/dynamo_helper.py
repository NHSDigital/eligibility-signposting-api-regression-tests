import logging
import os

import boto3
from utils.common_utils import save_to_file, load_from_file
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

ENVIRONMENT = os.getenv("ENVIRONMENT")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")


class DynamoDBHelper:
    def __init__(self, table_name):
        # Create DynamoDB resource using credentials from env
        self.table_name = table_name
        self.dynamodb_client = boto3.client("dynamodb", "eu-west-2")
        self.dynamodb_resource = boto3.resource("dynamodb", "eu-west-2")
        self.table = self.dynamodb_resource.Table(table_name)

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
            )
        except ClientError as e:
            logger.exception(
                f"Failed to get table information: {e.response["Error"]["Message"]}"
            )
            raise
        save_to_file(
            "description.json", table_description, directory="data/dynamoDB/temp"
        )
        return table_description

    def get_table_tags(self, table_arn):
        tags = self.dynamodb_client.list_tags_of_resource(ResourceArn=table_arn)
        save_to_file("tags.json", tags, directory="data/dynamoDB/temp")
        return tags

    def set_table_tags(self, table_arn, tags):
        self.dynamodb_client.tag_resource(ResourceArn=table_arn, Tags=tags["Tags"])

    def create_table(self, attribute_definitions, key_schema):
        logger.info(f"Creating table '{self.table_name}'...")
        try:
            self.dynamodb_client.create_table(
                TableName=self.table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
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


def restore_tags_to_table(dynamo_db_table: DynamoDBHelper, table_arn, tags):
    if table_arn is None:
        logger.warning(
            "Unable to find TableArn, attempting to load from backup file..."
        )
        table_description = load_from_file("data/dynamoDB/temp/description.json")
        table_arn = table_description["Table"]["TableArn"]
        logger.warning(f"TableArn loaded from backup file: : {table_arn}")
    if tags is None:
        logger.warning("Unable to add tags, attempting to load from backup file...")
        try:
            tags = load_from_file("data/dynamoDB/temp/tags.json")
            logger.warning(f"tags loaded from backup file: {tags}")

            logger.debug(f"TableArn: {table_arn}")
            dynamo_db_table.set_table_tags(table_arn, tags)
        except FileNotFoundError:
            logger.error("Failed to restore tags and no backup file was found.")


def get_attribute_definitions_and_key_schema_from_file(
    attribute_definitions: str | int | bytes, key_schema: str | int | bytes
) -> tuple[str | int | bytes, str | int | bytes]:
    table_description = load_from_file("data/dynamoDB/temp/description.json")
    key_schema = table_description["Table"]["KeySchema"]
    logger.warning(f"KeySchema loaded from backup file: {key_schema}")

    attribute_definitions = table_description["Table"]["AttributeDefinitions"]
    logger.warning(
        f"attribute_definitions loaded from backup file: {attribute_definitions}"
    )
    return attribute_definitions, key_schema


def reset_dynamo_tables():
    logger.info("Resetting DynamoDB. This may take a few moments, please be patient.")
    if ENVIRONMENT not in ["dev", "test"]:
        logger.warning(
            f"{ENVIRONMENT} is not supported. Resetting DynamoDB is only supported in dev or test."
        )
        return
    dynamo_db_table = DynamoDBHelper(DYNAMODB_TABLE_NAME)
    table_name = DYNAMODB_TABLE_NAME

    # --- Step 1: Fetch table information ---
    table_description = dynamo_db_table.describe_table()
    table_arn = table_description["Table"]["TableArn"]
    logger.debug(f"TableArn: {table_arn}")

    key_schema = table_description["Table"]["KeySchema"]
    logger.debug(f"KeySchema: {key_schema}")

    attribute_definitions = table_description["Table"]["AttributeDefinitions"]
    logger.debug(f"attribute_definitions: {attribute_definitions}")

    tags = dynamo_db_table.get_table_tags(table_arn)
    logger.debug(f"tags: {tags}")
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
    if attribute_definitions is None or key_schema is None:
        logger.warning(
            "Unable to create table because AttributeDefinitions or KeySchema are not defined. "
            "Attempting to load from backup file..."
        )
        try:
            attribute_definitions, key_schema = (
                get_attribute_definitions_and_key_schema_from_file(
                    attribute_definitions, key_schema
                )
            )
        except FileNotFoundError as e:
            logger.exception(
                "Failed to load table information and no backup file was found."
            )
            raise e
    dynamo_db_table.create_table(attribute_definitions, key_schema)

    # --- Step 4: Restore the tags ---
    logger.debug(f"Adding tags to table '{dynamo_db_table.table_name}'...")
    restore_tags_to_table(dynamo_db_table, table_arn, tags)


def insert_into_dynamo(data):
    logger.debug("Inserting into Dynamo: %s", data)
    table = DynamoDBHelper(DYNAMODB_TABLE_NAME)
    for item in data:
        try:
            table.insert_item(item)
            logger.debug("✅ Inserted: %s", item)
        except ClientError as e:
            logger.exception(
                "❌ Failed to insert %s: %s", item, e.response["Error"]["Message"]
            )
