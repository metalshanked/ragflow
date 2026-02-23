# ============================================================
# Dockerfile for RAGFlow Assessment API
#
# Build:
#   docker build -t ragflow-assessment ./assessment
#
# Run:
#   docker run -p 8000:8000 \
#     -e ASSESSMENT_RAGFLOW_BASE_URL=http://ragflow-host:9380 \
#     -e ASSESSMENT_RAGFLOW_API_KEY=ragflow-your-key \
#     ragflow-assessment
# ============================================================

FROM python:3.12-slim

LABEL maintainer="RAGFlow Assessment API"
LABEL description="FastAPI wrapper for RAGFlow assessment processing"

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt /app/assessment/requirements.txt
RUN pip install --no-cache-dir -r /app/assessment/requirements.txt

# Copy application source into a proper Python package directory
COPY . /app/assessment/

# Create a directory for the default SQLite database (mountable as a volume)
RUN mkdir -p /app/data
VOLUME ["/app/data"]

# Default environment variables (override at runtime)
ENV ASSESSMENT_HOST=0.0.0.0 \
    ASSESSMENT_PORT=8000 \
    ASSESSMENT_DATABASE_URL=sqlite+aiosqlite:////app/data/assessment.db

EXPOSE 8000

# Health check against the unauthenticated /health endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"]

# Run as a proper Python package so that relative imports work correctly
CMD ["uvicorn", "assessment.main:app", "--host", "0.0.0.0", "--port", "8000"]
