[![Regression Tests](https://github.com/NHSDigital/eligibility-signposting-api-regression-tests/actions/workflows/regression_tests.yml/badge.svg?branch=main)](https://github.com/NHSDigital/eligibility-signposting-api-regression-tests/actions/workflows/regression_tests.yml)
[![AI Code Assurance](https://sonarcloud.io/api/project_badges/ai_code_assurance?project=NHSDigital_eligibility-signposting-api-regression-tests)](https://sonarcloud.io/summary/new_code?id=NHSDigital_eligibility-signposting-api-regression-tests)
# Regression Tests
These tests will automate End-to-End regression testing for:
* [Eligibility Signposting API](https://github.com/NHSDigital/eligibility-signposting-api)

## General usage
These tests are run automatically during deployment and shouldn't need to be touched unless performing debugging or
adding/removing/changing test cases <br />
If there are any test failures, this will report a failed build

When developing new features that need to be regression tested, you'll need to create a new PR for them on this repository. When you are happy with the tests and the feature, merge the regression tests first. This will create a new tagged release, which you should then reference in the counterpart feature pull request before merging the code.

## Setup

### Environment Variables
Environment Variable for this are used, however are not necessary to be set by the user.

### Preparing your development environment
You will need the following;
* Ubuntu (WSL)
* [ASDF](https://asdf-vm.com/guide/getting-started.html)
* You can now run the `make install-full` command

Once this is completed, everything you need to get going should now be installed. </br>
You can now activate your virtual environment `source .venv/bin/activate`

## Developing/Debugging Tests

## Running the tests:
Before running the tests, authentication to AWS is necessary.

### Method 1 (Recommended):
Run the `make run-tests` command
You need to specify the following when executing this command:
*  env= (options: dev test preprod)
*  log_level= (options: INFO DEBUG)
Example: ` make run-tests env=dev log_level=INFO`

### Method 2:
Run the tests by calling the pytest command directly.
This allows for further customisation suitable for debugging purposes

For example:
`poetry run pytest --env=${env} --log-cli-level=${log_level} -s tests/test_story_tests.py`

**Note that we with the `poetry run` command before calling pytest**

### Commit to Git
Pre commit hooks run checks on your code to ensure quality before being allowed to commit.
You can perform this process by running: <br /> `make pre-commit`

You may need to run this multiple times to ensure everything is ok before committing.
