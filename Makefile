.PHONY: lint test docker-build

lint:
	@echo "Running lint checks..."
	python -m compileall .
	ruff check .

# Test: for now just sanity checks
test:
	@echo "Running tests / sanity checks..."
	python -m compileall .

# Docker build 
docker-build:
	@echo "Building Docker image..."
	docker build -t tech-product-expert:ci .

docker-build-airflow:
	@echo "Building Airflow image..."
	docker build -f Dockerfile.airflow -t tech-product-expert-airflow:ci .

install:
	@echo "installing packages...."
	python -m pip install --upgrade pip
	pip install -r requirements.txt
