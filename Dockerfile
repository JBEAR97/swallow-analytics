FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Installa psql client per railway shell
RUN apt-get update && apt-get install -y postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY dashboard.py .

CMD streamlit run dashboard.py --server.address=0.0.0.0 --server.port=$PORT --server.headless=true
