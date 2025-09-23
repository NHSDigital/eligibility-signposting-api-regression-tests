project_name = eligibility-signposting-api-regression-tests

.PHONY: test

guard-%:
	@ if [ "${${*}}" = "" ]; then \
		echo "Environment variable $* not set"; \
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
	npm ci

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

run-tests-old: guard-env
	echo "Running Regression Tests"
	poetry run python ./runner.py --env=$(env) --tags=$(tags)

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

run-tests: guard-env
	poetry run pytest --env=${env} --capture=tee-sys --show-capture=all tests/test_story_tests.py
