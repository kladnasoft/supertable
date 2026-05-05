.PHONY: help build clean upload test test-fast test-cov

PACKAGE_NAME = supertable

help:
	@echo "Makefile commands:"
	@echo "  make build   - Build the package (dist/)"
	@echo "  make clean   - Remove build artifacts"
	@echo "  make upload  - Upload to PyPI using twine"
	@echo "  make test      - Run all tests (every tests/ folder under supertable/)"
	@echo "  make test-fast - Stop at first failure, quiet output"
	@echo "  make test-cov  - Run tests with coverage report"

build:
	python -m build

clean:
	rm -rf build/ dist/ *.egg-info

upload:
	twine upload dist/*

test:
	pytest supertable/

test-fast:
	pytest supertable/ -x -q --no-header

test-cov:
	pytest supertable/ --cov=supertable --cov-report=term-missing
