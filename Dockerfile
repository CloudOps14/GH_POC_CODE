FROM python:3.11

WORKDIR /app

RUN pip install flask requests prometheus_client python-dotenv

COPY exporter.py .

COPY .env .

CMD ["python", "exporter.py"]
