# CPU inference image — ultralytics base already ships torch + opencv
FROM ultralytics/ultralytics:latest-cpu

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

CMD ["python", "producer.py"]
