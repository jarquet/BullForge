FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY services/feature_engine/requirements.txt /app/services/feature_engine/requirements.txt
RUN pip install --no-cache-dir -r services/feature_engine/requirements.txt
RUN pip install --no-cache-dir asyncpg redis

# Copy shared code and service code
COPY shared/ /app/shared/
COPY migrations/ /app/migrations/
COPY services/__init__.py /app/services/__init__.py
COPY services/feature_engine/ /app/services/feature_engine/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "services.feature_engine.main"]
