FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY service_account.json /app/service_account.json

COPY . /usr/src/app



EXPOSE 5001

CMD ["python", "app.py"]
