FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py ./
RUN useradd -u 10001 appuser && chown -R appuser /app
USER appuser
CMD ["python", "producer.py"]