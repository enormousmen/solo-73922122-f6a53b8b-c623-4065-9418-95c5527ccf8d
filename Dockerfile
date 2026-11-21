FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DB_PATH=/data/volumes.sqlite3 \
    POOL_BLOCKS=1024 \
    WEB_CONCURRENCY=4

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# The SQLite database lives on a named volume so volumes survive restarts.
RUN mkdir -p /data \
    && useradd --uid 10001 --user-group --home-dir /app --no-create-home thinvol \
    && chown -R thinvol:thinvol /app /data
USER thinvol

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).status == 200 else 1)"

# Multiple workers share the SQLite file; BEGIN IMMEDIATE serializes writers
# across processes.  Adjust WEB_CONCURRENCY to match available cores.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers ${WEB_CONCURRENCY} --no-access-log"]
