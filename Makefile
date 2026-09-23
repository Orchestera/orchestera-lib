.PHONY: install install-dev sync sync-dev test lint format pre-commit clean release

# Install production dependencies
install:
	uv sync

# Install with dev dependencies
install-dev:
	uv sync --extra dev

# Sync dependencies (alias for install)
sync: install

# Sync with dev dependencies (alias for install-dev)
sync-dev: install-dev

# Run pre-commit on all files
pre-commit:
	uv run pre-commit run --all-files

# Run unit tests
test:
	uv run python -m unittest discover -s tests -v

# Run linters
lint:
	uv run black --check .
	uv run isort --check-only .

# Format code
format:
	uv run black .
	uv run isort .

# Publish the version in pyproject.toml after its PR has merged to main.
# The GitHub release event triggers .github/workflows/publish.yml (PyPI).
release:
	@set -eu; \
	if [ "$$(git branch --show-current)" != main ]; then \
		echo 'Release from main after merging the version-bump PR.' >&2; exit 1; \
	fi; \
	if git status --porcelain | grep -q .; then \
		echo 'Release requires a clean working tree.' >&2; exit 1; \
	fi; \
	git fetch --quiet origin main; \
	if [ "$$(git rev-parse HEAD)" != "$$(git rev-parse origin/main)" ]; then \
		echo 'Local main must match origin/main; pull first.' >&2; exit 1; \
	fi; \
	version="$$(uv version --short)"; \
	tag="v$$version"; \
	set --; \
	case "$$version" in *a[0-9]*|*b[0-9]*|*rc[0-9]*) set -- --prerelease ;; esac; \
	if git ls-remote --exit-code --tags origin "refs/tags/$$tag" >/dev/null; then \
		echo "Tag $$tag already exists; refusing to publish twice." >&2; exit 1; \
	fi; \
	uv lock --check; \
	uv run --frozen python -m unittest discover -s tests -v; \
	uv build; \
	if git status --porcelain | grep -q .; then \
		echo 'Build or tests changed tracked files; aborting release.' >&2; exit 1; \
	fi; \
	gh release create "$$tag" --repo Orchestera/orchestera-lib \
		--target "$$(git rev-parse HEAD)" --title "$$tag" \
		--notes "orchestera-lib $$version" "$$@"; \
	echo "Created $$tag; check the Publish to PyPI Actions run."

# Clean up
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
