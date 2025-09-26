import logging
import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


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
            raise
        else:
            return response

    def get_table_information(self):
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
        table_arn = table_description["Table"]["TableArn"]
        return table_description, table_arn


def reset_dynamo_tables():
    environment = os.getenv("ENVIRONMENT")
    logger.info("Resetting DynamoDB. This may take a few moments, please be patient.")
    if environment not in ["dev", "test"]:
        logger.warning(
            f"{environment} is not supported. Resetting DynamoDB is only supported in dev or test."
        )
        return
    dynamo_db_table = DynamoDBHelper(os.getenv("DYNAMODB_TABLE_NAME"))
    table_name = dynamo_db_table.table_name

    # --- Step 1: Fetch table information ---
    table_description, table_arn = dynamo_db_table.get_table_information()
    key_schema = table_description["Table"]["KeySchema"]
    logger.debug(f"KeySchema: {key_schema}")
    attribute_definitions = table_description["Table"]["AttributeDefinitions"]
    logger.debug(f"attribute_definitions: {attribute_definitions}")
    tags = dynamo_db_table.dynamodb_client.list_tags_of_resource(ResourceArn=table_arn)
    logger.debug(f"tags: {tags}")
    # --- Step 2: Delete the table ---
    try:
        logger.info(f"Deleting table '{table_name}'...")
        dynamo_db_table.dynamodb_client.delete_table(TableName=table_name)

        # Wait for the table to be completely deleted
        logger.debug(
            f"Waiting for table '{dynamo_db_table.table_name}' to be deleted..."
        )
        waiter = dynamo_db_table.dynamodb_client.get_waiter("table_not_exists")
        waiter.wait(TableName=dynamo_db_table.table_name)
        logger.info(f"Table '{dynamo_db_table.table_name}' successfully deleted.")

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.warning(
                f"Table '{dynamo_db_table.table_name}' does not exist. Skipping deletion."
            )
        else:
            logger.exception(
                f"Error deleting table '{dynamo_db_table.table_name}': {e}"
            )
            return

    # --- Step 3: Recreate the table ---
    logger.info(f"Creating table '{dynamo_db_table.table_name}'...")
    try:
        dynamo_db_table.dynamodb_client.create_table(
            TableName=dynamo_db_table.table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            BillingMode="PAY_PER_REQUEST",
        )

        # Wait for the new table to become active
        logger.debug(
            f"Waiting for table '{dynamo_db_table.table_name}' to be created and become active..."
        )
        waiter = dynamo_db_table.dynamodb_client.get_waiter("table_exists")
        waiter.wait(TableName=dynamo_db_table.table_name)
        logger.info(
            f"Table '{dynamo_db_table.table_name}' successfully created and is now active."
        )
    except ClientError as e:
        logger.exception(f"Error creating table '{dynamo_db_table.table_name}': {e}")

    # --- Step 4: Restore the tags ---
    logger.debug(f"restoring tags to table '{dynamo_db_table.table_name}'...")
    dynamo_db_table.dynamodb_client.tag_resource(
        ResourceArn=table_arn, Tags=tags["Tags"]
    )


def insert_into_dynamo(data):
    logger.debug("Inserting into Dynamo: %s", data)
    table = DynamoDBHelper(os.getenv("DYNAMODB_TABLE_NAME"))
    for item in data:
        try:
            table.insert_item(item)
            logger.debug("✅ Inserted: %s", item)
        except ClientError as e:
            logger.exception(
                "❌ Failed to insert %s: %s", item, e.response["Error"]["Message"]
            )
