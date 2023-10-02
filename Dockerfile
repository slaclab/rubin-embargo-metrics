FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9-slim

RUN mkdir -p /app
WORKDIR /app
COPY requirements.txt /app

RUN pip3 install -r /app/requirements.txt

COPY . /app

ENTRYPOINT [ "uvicorn", "main:app", "--host", "0.0.0.0", "--reload" ]
