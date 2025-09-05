pypi-ingest:
	uv run ./ingest/pipeline.py

pypi-transform:
	uv run ./transform/pipeline.py

pypi-visualize:
	uv run streamlit run ./visualize/main.py

reset-data:
	rm -r ./data

format:
	uv run ruff format

test:
	uv run pytest tests