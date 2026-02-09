import os
from pathlib import Path

import urllib3
import random
import csv

# Use pip and venv to get access to the Locust library
from locust import HttpUser, task, constant_throughput, events

# Function to get CLI arguments for environment, which will be used for the
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--env", choices=["dev", "test", "pre-prod"], default="dev", help="Environment")

with open("nhs_numbers.csv", newline='') as csvFile:
    reader = csv.reader(csvFile)
    csvData = list(reader)

#Class for API execution
class GetPatientId(HttpUser):

    # Required to prevent warnings on certain exceptions with TLS
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # This will set the pacing to 1 TPS per user, i.e. 1 user = 1 TPS, 50 = 50 TPS etc
    wait_time = constant_throughput(1)

    # This can be set in the CLI settings if required to be changed
    host = "https://dev.eligibility-signposting-api.nhs.uk/"

    @task
    def getPatientData(self):

        # Gets a new random NHS Number
        csvRow = random.choice(csvData)
        PatientId = csvRow[0]

        # Gets this value from the CLI options (if required, set to pre-prod by default). This is required to change the certs for each environment.
        env = self.environment.parsed_options.env
        out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), f"out/{env}"))

        PROJECT_ROOT = Path(__file__).resolve().parents[2]  # adjust depth if needed

        private_key_path = PROJECT_ROOT / "certs/api_private_key_cert.pem"
        client_cert_path = PROJECT_ROOT / "certs/api_client_cert.pem"

        # The request is getting sent is here
        with self.client.get(
            name="patient-check/{PatientId}",
            url=f"patient-check/{PatientId}",
            headers={"Accept" : "application/json", "nhs-login-nhs-number": f"{PatientId}", "NHSE-Product-ID": "P.WTJ-FJT"},
            cert=(client_cert_path, private_key_path),
            verify=False,
            catch_response=True
        ) as response:
            # This is checking asserting the response has the word cohortText, which shows on a valid request
            if 'processedSuggestions' not in response.text:
                response.failure(f"Response didn't contain processedSuggestions (expected), nhsNumber was {PatientId}. Response was {response.text}")

