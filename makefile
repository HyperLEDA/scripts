check:
	uvx ruff format --config=pyproject.toml --check
	uvx ruff check --config=pyproject.toml

fix:
	uvx ruff format --config=pyproject.toml
	uvx ruff check --config=pyproject.toml --fix
