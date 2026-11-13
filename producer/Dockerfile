FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sse.py producer.py ./
RUN useradd -m app && mkdir /state && chown app /state
USER app
ENTRYPOINT ["python", "-u", "producer.py"]
