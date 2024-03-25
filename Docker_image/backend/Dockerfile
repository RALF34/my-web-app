FROM python:3.11-slim

WORKDIR /backend

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 8080

CMD uvicorn --host 0.0.0.0 --port 8080 backend.main:app
