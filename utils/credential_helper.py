import logging

import boto3

logger = logging.getLogger(__name__)


class AWSSession:
    def __init__(self, environment):
        # Create DynamoDB resource using credentials from env
        self.session = boto3.session.Session(profile_name=environment)
        self.credentials = self.session.get_credentials()
        self.access_key = self.credentials.access_key
        self.secret_key = self.credentials.secret_key
        self.token = self.credentials.token

    def get_session(self):
        return self.session
