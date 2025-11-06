.PHONY: clean install dev test

clean:
	find . -type d -name "build" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "dist" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .venv

install: clean
	uv venv
	. .venv/bin/activate && cd packages/core && uv pip install -e .
	. .venv/bin/activate && cd packages/cli && uv pip install -e .

dev: install
	. .venv/bin/activate && uv pip install pytest pytest-cov black isort flake8 mypy

test:
	. .venv/bin/activate && pytest
