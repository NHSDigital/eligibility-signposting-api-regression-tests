project_name = eligibility-signposting-api-regression-tests

.PHONY: test

guard-%:
	@ if [ "${${*}}" = "" ]; then \
		echo "Parameter $* needed!"; \
		exit 1; \
	fi

install: install-python install-hooks install-node

install-full: install-asdf-plugins install

uninstall-full: clear-virtualenv asdf-uninstall

update: update-poetry update-node install

update-node:
	npm update

update-poetry:
	poetry update

install-asdf-plugins:
	@cut -d' ' -f1 .tool-versions | while read plugin; do \
	  if ! asdf plugin list | grep -qx "$$plugin"; then \
	    echo "➕ Adding asdf plugin: $$plugin"; \
	    asdf plugin add "$$plugin"; \
	  else \
	    echo "✔ Plugin already installed: $$plugin"; \
	  fi; \
	done
	@asdf install

install-node:
	npx npm@11.6.3 ci

install-python:
	poetry config virtualenvs.in-project true --local
	poetry install

install-hooks: install-python
	poetry run pre-commit install --install-hooks --overwrite

lint-black:
	poetry run black .

lint-pyright:
	export PYRIGHT_PYTHON_GLOBAL_NODE=0; poetry run pyright .

lint-flake8:
	poetry run flake8 .

lint: lint-black lint-pyright lint-flake8

check-licenses:
	scripts/check_python_licenses.sh

clear-virtualenv:
	poetry env remove
	rm -f -d -r node_modules

asdf-uninstall:
	asdf plugin list | xargs -n 1 asdf plugin remove

deep-clean-install:
	make clear-virtualenv
	make asdf-uninstall
	make install-full

pre-commit:
	poetry run pre-commit run --all-files

clear-db: guard-env guard-log_level
	poetry run pytest --env=${env} --log-cli-level=${log_level} tests/test_reset_db.py

preload-data: guard-env guard-log_level
	poetry run pytest --env=${env} --log-cli-level=${log_level} tests/test_preload_data.py

run-tests: guard-env guard-log_level clear-db preload-data
	DYNAMO_PRELOADED=true poetry run pytest --env=${env} --log-cli-level=${log_level} tests/test_story_tests.py
	DYNAMO_PRELOADED=true poetry run pytest --env=${env} --log-cli-level=${log_level} tests/test_error_scenario_tests.py
	DYNAMO_PRELOADED=true poetry run pytest --env=${env} --log-cli-level=${log_level} tests/test_vita_integration_tests.py
	DYNAMO_PRELOADED=true poetry run pytest --env=${env} --log-cli-level=${log_level} tests/test_nbs_integration_tests.py

ifeq ($(filter $(env),test dev),$(env))
	DYNAMO_PRELOADED=true poetry run pytest --env=${env} --log-cli-level=${log_level} tests/test_hashing_tests.py
endif

ifeq ($(filter $(env),preprod),$(env))
	DYNAMO_PRELOADED=true poetry run pytest --env=${env} --log-cli-level=${log_level} tests/test_upload_consumer_configs.py
endif

run-vita-preprod-tests:
	poetry run pytest --env=preprod --log-cli-level=info tests/test_vita_integration_tests.py tests/test_upload_consumer_configs.py
