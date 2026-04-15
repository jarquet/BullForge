FROM nvcr.io/nvidia/pytorch:25.11-py3

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

COPY services/model_service/requirements.txt /tmp/model.txt
COPY services/execution_service/requirements.txt /tmp/exec.txt
RUN pip install --no-cache-dir -r /tmp/model.txt -r /tmp/exec.txt

COPY shared/ /app/shared/
COPY migrations/ /app/migrations/
COPY services/__init__.py /app/services/__init__.py
COPY services/model_service/ /app/services/model_service/
COPY services/execution_service/ /app/services/execution_service/
COPY services/backtest_service/ /app/services/backtest_service/
COPY scripts/ /app/scripts/

ENTRYPOINT ["python", "scripts/backtest.py"]
