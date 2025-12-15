.PHONY: lint test docker-build

# Lint: static checks (fast, no execution)
lint:
	@echo "Running lint checks..."
	python -m compileall .
	ruff check .

# Test: for now just sanity checks (add pytest later)
test:
	@echo "Running tests / sanity checks..."
	python -m compileall .

# Docker build (optional for CI)
docker-build:
	@echo "Building Docker image..."
	docker build -t tech-product-expert:ci .

install:
	@echo "installing packages...."
	python -m pip install --upgrade pip
	pip install -r requirements.txt
