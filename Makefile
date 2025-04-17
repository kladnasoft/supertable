.PHONY: help build clean upload test

PACKAGE_NAME = supertable

help:
	@echo "Makefile commands:"
	@echo "  make build   - Build the package (dist/)"
	@echo "  make clean   - Remove build artifacts"
	@echo "  make upload  - Upload to PyPI using twine"
	@echo "  make test    - Run tests (if any)"

build:
	python -m build

clean:
	rm -rf build/ dist/ *.egg-info

upload:
	twine upload dist/*

test:
	pytest tests/
