pypi-ingest:
	uv run ./ingest/pipeline.py

pypi-analize:
	uv run ./analize/pipeline.py

reset-data:
	rm -r ./data

format:
	uv run ruff format

test:
	uv run pytest tests