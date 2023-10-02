from fastapi import FastAPI, Request
from prometheus_client import make_asgi_app, Counter

app = FastAPI()

index_counter = Counter('index_counter', 'Description of counter')

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

@app.get("/")
def index():
    index_counter.inc()
    return "Hello, world!"


