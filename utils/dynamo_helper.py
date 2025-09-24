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
        self.table_description = self.dynamodb_client.describe_table(
            TableName=self.table_name
        )
        self.key_schema = self.table_description["Table"]["KeySchema"]
        self.attribute_definitions = self.table_description["Table"][
            "AttributeDefinitions"
        ]
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


def reset_dynamo_tables():
    logger.info("Resetting DynamoDB. This may take a few moments, please be patient.")
    dynamo_table_name = os.getenv("DYNAMODB_TABLE_NAME")
    assert dynamo_table_name is not None, "DynamoDB table name not specified"
    table = DynamoDBHelper(dynamo_table_name)

    # --- Step 1: Delete the table if it exists ---
    try:
        logger.info(f"Deleting table '{table.table_name}'...")
        table.dynamodb_client.delete_table(TableName=table.table_name)

        # Wait for the table to be completely deleted
        logger.debug(f"Waiting for table '{table.table_name}' to be deleted...")
        waiter = table.dynamodb_client.get_waiter("table_not_exists")
        waiter.wait(TableName=table.table_name)
        logger.info(f"Table '{table.table_name}' successfully deleted.")

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.warning(
                f"Table '{table.table_name}' does not exist. Skipping deletion."
            )
        else:
            logger.exception(f"Error deleting table '{table.table_name}': {e}")
            return

    # --- Step 2: Recreate the table ---
    logger.info(f"Creating table '{table.table_name}'...")
    try:
        table.dynamodb_client.create_table(
            TableName=table.table_name,
            KeySchema=table.key_schema,
            AttributeDefinitions=table.attribute_definitions,
            BillingMode="PAY_PER_REQUEST",
        )

        # Wait for the new table to become active
        logger.debug(
            f"Waiting for table '{table.table_name}' to be created and become active..."
        )
        waiter = table.dynamodb_client.get_waiter("table_exists")
        waiter.wait(TableName=table.table_name)
        logger.info(
            f"Table '{table.table_name}' successfully created and is now active."
        )

    except ClientError as e:
        logger.exception(f"Error creating table '{table.table_name}': {e}")


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
