FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /opt/app
COPY requirements/core.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# Source code is deliberately NOT copied: docker-compose bind-mounts ./src.
CMD ["python", "-m", "src.selftest.main"]
