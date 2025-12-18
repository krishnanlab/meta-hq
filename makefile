.PHONY: clean install uv_install dev uv_dev test

clean:
	find . -type d -name "build" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "dist" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .venv

uv_install: clean
	uv venv
	uv sync
	. .venv/bin/activate 

uv_dev: uv_install
	. .venv/bin/activate && uv sync --dev

install: clean
	cd packages/core && pip install -e .
	cd packages/cli && pip install -e .

dev: clean
	cd packages/core && pip install -e ".[dev]"
	cd packages/cli && pip install -e ".[dev]"

test:
	. .venv/bin/activate && pytest
