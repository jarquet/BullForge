FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY services/model_service/requirements.txt /app/services/model_service/requirements.txt
RUN pip install --no-cache-dir -r services/model_service/requirements.txt
RUN pip install --no-cache-dir asyncpg redis

# Copy shared code and service code
COPY shared/ /app/shared/
COPY migrations/ /app/migrations/
COPY services/__init__.py /app/services/__init__.py
COPY services/model_service/ /app/services/model_service/

# Create default artifacts directory
RUN mkdir -p /app/models/artifacts

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "services.model_service.main"]
