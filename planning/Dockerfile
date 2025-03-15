FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
COPY integrationproject-453814-33379dc2ad9c.json /app/service-account.json

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["python", "app.py"]
