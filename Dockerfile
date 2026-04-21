FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY README.md setup.py ./
COPY polymarket/ polymarket/
RUN pip install --no-cache-dir -e .

RUN mkdir -p /app/data/dataset /app/data/data_clean /app/logs

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=30s \
    CMD curl -f http://localhost:8000/ || exit 1

ENTRYPOINT ["python", "-m", "polymarket.service"]
CMD ["--host", "0.0.0.0", "--port", "8000"]
