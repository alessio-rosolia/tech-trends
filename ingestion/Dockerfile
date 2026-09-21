FROM python:3.10-slim

# Creiamo un utente non privilegiato
RUN useradd -m -u 1000 ingestion_user

WORKDIR /app

# Cache delle dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia del codice e del file di configurazione
COPY main.py .
COPY bluesky.env .

# Passaggio all'utente non-root
USER ingestion_user

CMD ["python", "-u", "main.py"]