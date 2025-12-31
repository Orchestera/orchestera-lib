.PHONY: install install-dev sync lint format pre-commit clean

# Install production dependencies
install:
	uv sync

# Install with dev dependencies
install-dev:
	uv sync --extra dev

# Sync dependencies (alias for install)
sync: install

# Run pre-commit on all files
pre-commit:
	uv run pre-commit run --all-files

# Run linters
lint:
	uv run black --check .
	uv run isort --check-only .

# Format code
format:
	uv run black .
	uv run isort .

# Clean up
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
