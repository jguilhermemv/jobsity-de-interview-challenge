FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir poetry setuptools \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --only main --no-root \
    && pip install --no-cache-dir "setuptools<81"

COPY src ./src

ENV PYTHONPATH=/app/src

CMD ["uvicorn", "de_challenge.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
