import boto3
from datetime import datetime, timedelta
import time

query = (
    "stats avg(integrationLatency) as avgIntegrationLatency,"
    " max(integrationLatency) as maxIntegrationLatency,"
    " min(integrationLatency) as minIntegrationLatency,"
    " avg(responseLatency) as avgResponseLatency,"
    " max(responseLatency) as maxResponseLatency,"
    " min(responseLatency) as minResponseLatency,"
    " count(*) as recordCount"
)


client = boto3.client("logs", region_name="eu-west-2")

response = client.start_query(
    logGroupName="/aws/apigateway/default-eligibility-signposting-api",
    startTime=int((datetime.now() - timedelta(minutes=1000)).timestamp()),
    endTime=int(datetime.now().timestamp()),
    queryString=query,
)

query_id = response["queryId"]

# Poll for query completion
while True:
    result = client.get_query_results(queryId=query_id)
    if result["status"] == "Complete":
        break
    time.sleep(1)

# Print the actual log results
for row in result["results"]:
    print({field["field"]: field["value"] for field in row})
