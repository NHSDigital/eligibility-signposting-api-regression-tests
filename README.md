[![Regression Tests](https://github.com/NHSDigital/eligibility-signposting-api-regression-tests/actions/workflows/regression_tests.yml/badge.svg?branch=main)](https://github.com/NHSDigital/eligibility-signposting-api-regression-tests/actions/workflows/regression_tests.yml)

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
It is necessary to set some Environment variables in order to run any tests in your local environment. The tests will look for environment variables in the following order
(For security, the values will not be displayed here):
1. `.env` file
2. OS environment variable

The following environment variables may need to be set for the correct environment you wish to test against:
* BASE_URL
* ABORT_ON_AWS_FAILURE
* AWS_DEFAULT_REGION
* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY
* AWS_SESSION_TOKEN
* DYNAMODB_TABLE_NAME
* S3_BUCKET_NAME
* S3_PREFIX
* S3_JSON_SOURCE_DIR
* IGNORE_KEYS
* KEEP_SEED

To make this easier, a `template.env` file is located on the root. Fill in the values and rename this to `.env`

Any file that begins with `.env` is automatically ignored by Git

### Preparing your development environment
This test pack utilises the power of Docker to quickly and easily spin up a dev environment for you to work in
the Dockerfile is located in `{project_root}/.devcontainer/Dockerfile`

### Setup without docker development environment
If you'd like to use your own machine without containerisation. You will need the following;
* Ubuntu (WSL)
* [ASDF](https://asdf-vm.com/guide/getting-started.html)
* You can now run the `make install-full` command

Once this is completed, everything you need to get going should now be installed. </br>
You can now activate your virtual environment `source .venv/bin/activate`

## Developing/Debugging Tests

## Running the tests:

### Method 1 (Recommended):
Run the `runner.py` file located in the root of the project <br />
This is the preferred method and allows you to include/exclude tags <br />
a `~` before the tag name excludes it. <br />
This is how the tests are run on the CI
<h4> You MUST specify the environment and product <br />

#### Example: `python runner.py --env=INT --tags smoke --tags ~slow`
This will run all tests with the tag `@smoke` but skip any tests tagged with `@slow`

### Method 2:
Run the tests by calling the Make command `make run-tests`. This requires the parameter `env=` is passed in.
Optionally, you can pass in tags to be run, for example `tags=regression` will run all tests tagged as `regression`.

For example:
```
env=internal-dev PULL_REQUEST_ID=pr-300 tags=regression make run-tests
```

Change the `env` variable accordingly to either `INT` or `INTERNAL-DEV`.

### Commit to Git
Pre commit hooks run checks on your code to ensure quality before being allowed to commit. You can perform this process by running: <br /> `make pre-commit`

You may need to run this multiple times to ensure everything is ok before committing.
