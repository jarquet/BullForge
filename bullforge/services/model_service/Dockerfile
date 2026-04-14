FROM nvcr.io/nvidia/pytorch:25.11-py3

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/model_service/requirements.txt /app/services/model_service/requirements.txt
RUN pip install --no-cache-dir -r services/model_service/requirements.txt

COPY shared/ /app/shared/
COPY migrations/ /app/migrations/
COPY services/__init__.py /app/services/__init__.py
COPY services/model_service/ /app/services/model_service/

RUN mkdir -p /app/models/artifacts

ENV PYTHONPATH=/app

CMD ["python", "-m", "services.model_service.main"]
