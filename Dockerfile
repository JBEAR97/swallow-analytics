FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard.py .
COPY .railwayignore .env Procfile railway.json .  # opzionali

EXPOSE $PORT
CMD ["streamlit", "run", "dashboard.py", "--server.port=$PORT", "--server.address=0.0.0.0", "--server.headless=true", "--server.enableCORS=false"]
