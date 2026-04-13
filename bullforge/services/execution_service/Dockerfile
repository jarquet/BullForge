FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY services/execution_service/requirements.txt /app/services/execution_service/requirements.txt
RUN pip install --no-cache-dir -r services/execution_service/requirements.txt

# Copy shared code and service code
COPY shared/ /app/shared/
COPY migrations/ /app/migrations/
COPY services/__init__.py /app/services/__init__.py
COPY services/execution_service/ /app/services/execution_service/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "services.execution_service.main"]
