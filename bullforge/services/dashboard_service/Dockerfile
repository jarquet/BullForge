FROM python:3.12-slim

WORKDIR /app

COPY services/dashboard_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
COPY services/dashboard_service/ /app/services/dashboard_service/
COPY migrations/ /app/migrations/

ENV PYTHONPATH=/app
CMD ["python", "-m", "services.dashboard_service.main"]
