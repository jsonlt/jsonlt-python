set shell := ['uv', 'run', '--frozen', 'bash', '-euxo', 'pipefail', '-c']
set unstable
set positional-arguments

project := "jsonlt"
package := "jsonlt"
pnpm := "pnpm exec"

# List available recipes
default:
  @just --list

# Run benchmarks
benchmark *args:
  pytest -m benchmark --codspeed "$@"

# Build distribution packages
build: clean-python
  uv build --no-sources

# Build distribution packages with SBOM
build-release: build
  #!/usr/bin/env bash
  uv run --frozen --isolated --group release cyclonedx-py environment --of json -o dist/sbom.cdx.json

# Clean build artifacts
clean: clean-python

# Clean Python build artifacts
clean-python:
  #!/usr/bin/env bash
  rm -rf dist
  find . -type d -name __pycache__ -exec rm -rf {} +
  find . -type d -name .pytest_cache -exec rm -rf {} +
  find . -type d -name .ruff_cache -exec rm -rf {} +

# Format code
format:
  codespell -w
  ruff format .
  {{pnpm}} biome format --write .

# Fix code issues
fix:
  ruff format .
  ruff check --fix .
  biome format --write .
  biome check --write .

# Fix code issues including unsafe fixes
fix-unsafe:
  ruff format .
  ruff check --fix --unsafe-fixes .
  biome check --write --unsafe

# Run all linters
lint:
  ruff check .
  basedpyright
  codespell
  yamllint --strict .
  {{pnpm}} biome check .
  {{pnpm}} markdownlint-cli2 "**/*.md"

# Lint Markdown files
lint-markdown:
  {{pnpm}} markdownlint-cli2 "**/*.md"

# Lint Python code
lint-python:
  ruff check .
  ruff format --check .
  basedpyright

# Lint prose in Markdown files
lint-prose:
  vale CODE_OF_CONDUCT.md CONTRIBUTING.md README.md SECURITY.md

# Check spelling
lint-spelling:
  codespell

# Check types
lint-types:
  basedpyright

# Lint web files (CSS, HTML, JS, JSON)
lint-web:
  {{pnpm}} biome check .

# Install all dependencies (Python + Node.js)
install: install-node install-python

# Install only Node.js dependencies
install-node:
  #!/usr/bin/env bash
  pnpm install --frozen-lockfile

# Install pre-commit hooks
install-prek:
  prek install

# Install only Python dependencies
install-python:
  #!/usr/bin/env bash
  uv sync --frozen

# Run pre-commit hooks on changed files
prek:
  prek

# Run pre-commit hooks on all files
prek-all:
  prek run --all-files

# Publish to TestPyPI (requires OIDC token in CI or UV_PUBLISH_TOKEN)
publish-testpypi: build-release
  uv publish --publish-url https://test.pypi.org/legacy/

# Publish to PyPI (requires OIDC token in CI or UV_PUBLISH_TOKEN)
publish-pypi: build-release
  uv publish

# Run command
run *args:
  "$@"

# Run Node.js
run-node *args:
  {{pnpm}} "$@"

# Run Python
run-python *args:
  python "$@"

# Generate SBOM for current environment
sbom output="sbom.cdx.json":
  uv run --isolated --group release cyclonedx-py environment --of json -o {{output}}

# Run tests (excludes benchmarks and slow tests by default)
test *args:
  pytest "$@"

# Run all tests
test-all *args:
  pytest -m "" "$@"

# Run tests with coverage
test-coverage *args:
  pytest -m "not benchmark" --cov={{package}} --cov-branch --cov-report=term-missing:skip-covered --cov-report=xml --cov-report=json "$@"

# RUn documentation tests
test-examples *args:
  pytest -m example "$@"

# Run only failed tests from last run
test-failed *args: (test args "--lf")

# Run slow tests
test-slow *args:
  pytest -m "slow" "$@"

# Update documentation examples (refresh output blocks)
update-examples *args:
  pytest -m example --update-examples "$@"

# Sync Vale styles and dictionaries
vale-sync:
  vale sync
