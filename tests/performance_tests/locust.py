import os
from pathlib import Path

import urllib3
import random
import csv

# Use pip and venv to get access to the Locust library
from locust import HttpUser, task, constant_throughput, events

from utils.eligibility_api_client import EligibilityApiClient


# Function to get CLI arguments for environment, which will be used for the
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--env", choices=["dev", "test", "pre-prod"], default="dev", help="Environment")

with open("temp/nhs_numbers.csv", newline='') as csvFile:
    reader = csv.reader(csvFile)
    csvData = list(reader)

#Class for API execution
class GetPatientId(HttpUser):

    # Required to prevent warnings on certain exceptions with TLS
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # This will set the pacing to 1 TPS per user, i.e. 1 user = 1 TPS, 50 = 50 TPS etc
    wait_time = constant_throughput(1)

    # This can be set in the CLI settings if required to be changed
    PROJECT_ROOT = Path(__file__).resolve().parents[2]  # adjust depth if needed
    client = EligibilityApiClient(cert_dir= f"{PROJECT_ROOT}/certs")

    @task
    def get_patient_data(self):

        # Gets a new random NHS Number from provided CSV
        csv_row = random.choice(csvData)
        patient_id = csv_row[0]

        # The request is getting sent is here
        with self.client.make_request(
            patient_id,
            headers={"Accept" : "application/json", "nhs-login-nhs-number": f"{patient_id}", "NHSE-Product-ID": "P.WTJ-FJT"},
            raise_on_error=False,
        ) as response:
            # This is checking asserting the response has the word cohortText, which shows on a valid request
            if 'processedSuggestions' not in response.text:
                response.failure(f"Response didn't contain processedSuggestions (expected), nhsNumber was {patient_id}. Response was {response.text}")

