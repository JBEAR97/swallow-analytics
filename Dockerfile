FROM python:3.12-slim


WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Installa psql client per railway shell
RUN apt-get update && apt-get install -y postgresql-client \
    && rm -rf /var/lib/apt/lists/*


COPY main.py dashboard.py ./


# Default to FastAPI (will be overridden by startCommand in railway.toml)
CMD uvicorn main:app --host 0.0.0.0 --port $PORT