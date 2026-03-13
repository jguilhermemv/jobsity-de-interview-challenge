FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --only main

COPY src ./src

ENV PYTHONPATH=/app/src

CMD ["uvicorn", "de_challenge.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
