FROM python:3.12-slim

WORKDIR /app

COPY services/monitoring_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
COPY services/execution_service/alpaca_broker.py /app/services/execution_service/alpaca_broker.py
COPY services/execution_service/__init__.py /app/services/execution_service/__init__.py
COPY services/monitoring_service/ /app/services/monitoring_service/
COPY migrations/ /app/migrations/

CMD ["python", "-m", "services.monitoring_service.main"]
