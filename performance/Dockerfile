FROM python:3.12-slim

ARG PIP_INDEX_URL=https://artifactor.cera.com/api/pypi/pypi/simple
ARG PIP_TRUSTED_HOST=artifactor.cera.com

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_INDEX_URL=${PIP_INDEX_URL} \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} \
    PERFORMANCE_APP_PORT=8787 \
    PERFORMANCE_APP_BASE_PATH=

WORKDIR /app/performance

RUN cat > /tmp/requirements.txt <<'REQ'
quart>=0.20,<1
requests>=2.31,<3
python-docx>=1.1,<2
matplotlib>=3.8,<4
REQ

COPY ragflow_benchmark_app.py /app/performance/ragflow_benchmark_app.py

RUN python -m pip install --upgrade pip \
    && python -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

RUN mkdir -p /app/performance/data/runs /app/performance/data/uploads

EXPOSE 8787

CMD ["python", "/app/performance/ragflow_benchmark_app.py"]
