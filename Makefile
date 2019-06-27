help:
	@echo "Type: make [target]"
	@echo "    all    Run several tasks (clean/dev/lint/test)"
	@echo "    clean  Cleanup artifacts and temporary files"
	@echo "    dev    Install packages for dev virtualenv"
	@echo "    help   Show this help message"
	@echo "    lint   Check for PEP-8 standards"
	@echo "    test   Run tests"

all: clean dev lint test

clean:
	@rm -rf ./.DS_Store
	@rm -rf ./.pytest_cache
	@rm -rf ./nhlscrapy/.DS_Store
	@pip uninstall .
	@pip uninstall -r requirements.txt
	@pip uninstall -r requirements-dev.txt

dev:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pip install .

lint:
	@flake8 ./nhlscrapy/nhlscrapy.py

test:
	@pytest -s ./nhlscrapy/tests/

update-github:
	@git fetch origin
	@git checkout -b $(CI_COMMIT_REF_NAME) origin/$(CI_COMMIT_REF_NAME)
	@git remote add github https://$(USERNAME):$(PASSWORD)@github.com/xaaaaav/nhlscrapy.git
	@git push --all github
	@git push --tags github
	
