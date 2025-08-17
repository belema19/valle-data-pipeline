pypi-ingest:
	uv run ./ingest/pipeline.py

format:
	uv run ruff format

test:
	uv run pytest tests