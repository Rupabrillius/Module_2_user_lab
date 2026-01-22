FROM python:3.11-slim
WORKDIR /app
COPY services/dq-gate/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY common /app/common
COPY services/dq-gate/app.py /app/app.py
CMD ["python", "/app/app.py"]
