FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY services/data_pipeline/requirements.txt /app/services/data_pipeline/requirements.txt
RUN pip install --no-cache-dir -r services/data_pipeline/requirements.txt
RUN pip install --no-cache-dir aiohttp asyncpg redis

# Copy shared code and service code
COPY shared/ /app/shared/
COPY migrations/ /app/migrations/
COPY services/__init__.py /app/services/__init__.py
COPY services/data_pipeline/ /app/services/data_pipeline/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8080

CMD ["python", "-m", "services.data_pipeline.main"]
