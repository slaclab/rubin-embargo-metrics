from fastapi import FastAPI, Request
from prometheus_client import make_asgi_app, Counter
from redis import asyncio as aioredis

from os import environ
import logging


REDIS = None
app = FastAPI()

index_counter = Counter('index_counter', 'Description of counter')

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

@app.on_event('startup')
async def startup_event():
  REDIS = await aioredis.from_url( environ.get('REDIS_URL', 'redis://localhost/1') ) 

@app.get("/keys")
async def list():
  # list all keys from redis store
  async with REDIS.client() as redis:
    keys = await redis.keys()
  return f"KEYS: {keys}"

@app.get("/")
def index():
  index_counter.inc()
  return f"REDIS STATE: {REDIS}"


