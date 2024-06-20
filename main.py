from fastapi import FastAPI
from prometheus_client import make_asgi_app, Counter, Gauge, Info, Summary, Histogram
from redis import asyncio as aioredis
from os import environ
from pythonjsonlogger import jsonlogger
from collections import defaultdict
from datetime import datetime, timedelta
import logging
import re
import time

# Create FastAPI app
app = FastAPI()
REDIS = None
# Logging to output as JSON
#General application
logger = logging.getLogger("app")
log_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt='%(asctime)s %(levelname)s %(name)s'
)
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)
#database
db_logger = logging.getLogger("db")
db_log_handler = logging.StreamHandler()
db_formatter = jsonlogger.JsonFormatter(
    fmt='%(asctime)s %(levelname)s %(name)s'
)
db_log_handler.setFormatter(db_formatter)
db_logger.addHandler(db_log_handler)
db_logger.setLevel(logging.INFO)
#prometheus
prometheus_logger = logging.getLogger("prometheus")
prometheus_log_handler = logging.StreamHandler()
prometheus_formatter = jsonlogger.JsonFormatter(
    fmt='%(asctime)s %(levelname)s %(name)s'
)
prometheus_log_handler.setFormatter(prometheus_formatter)
prometheus_logger.addHandler(prometheus_log_handler)
prometheus_logger.setLevel(logging.INFO)
# Prometheus metric app
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
#Default dict that will handle all the metrics
metrics = defaultdict(lambda: None)
def get_create_metric(metric_base_name, description, metric_type, labels=None):
    if metric_base_name not in metrics:
        if metric_type == 'gauge':
            prometheus_logger.info({"event": f"Creating {metric_type} metric. Name {metric_base_name}"})
            metrics[metric_base_name] = Gauge(metric_base_name, description, labels) if labels is not None else Gauge(metric_base_name, description)
            prometheus_logger.info({"event": f"{metric_base_name} created"})
        elif metric_type == 'histogram':
            prometheus_logger.info({"event": f"Creating {metric_type} metric. Name {metric_base_name}"})
            metrics[metric_base_name] = Histogram(metric_base_name, description, labels) if labels is not None else Histogram(metric_base_name, description)
            prometheus_logger.info({"event": f"{metric_base_name} created"})
        elif metric_type == 'summary':
            prometheus_logger.info({"event": f"Creating {metric_type} metric. Name {metric_base_name}"})
            metrics[metric_base_name] = Summary(metric_base_name, description, labels) if labels is not None else Summary(metric_base_name, description)
            prometheus_logger.info({"event": f"{metric_base_name} created"})
        elif metric_type == 'info':
            prometheus_logger.info({"event": f"Creating {metric_type} metric. Name {metric_base_name}"})
            metrics[metric_base_name] = Info(metric_base_name, description, labels) if labels is not None else Info(metric_base_name, description)
            prometheus_logger.info({"event": f"{metric_base_name} created"})
    return metrics[metric_base_name]

#Function to create gauge metrics from INGEST,FAIL,RECINSTR
def update_gauge_metric(prefix, key, hash_fields_values):
    base_metric_name = f"redis_summit_embargo"
    labels = ["bucket", "instrument", "obsday"]
    metric_key_name, bucket, instrument = key.split(":")
    map_obs_day={"today": datetime.now().strftime("%Y%m%d"),
    "today-1": (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
    "today-2": (datetime.now() - timedelta(days=2)).strftime("%Y%m%d"),
    "today-3": (datetime.now() - timedelta(days=3)).strftime("%Y%m%d"),
    "today-4": (datetime.now() - timedelta(days=4)).strftime("%Y%m%d"),
    "today-5": (datetime.now() - timedelta(days=5)).strftime("%Y%m%d"),
    "today-6": (datetime.now() - timedelta(days=6)).strftime("%Y%m%d")}
    #yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d") # this is for test should work with file_date == datetime.now().strftime("%Y%m%d")
    for obsday, value in hash_fields_values.items():
        mapped_obsday = next((k for k, v in map_obs_day.items() if v == obsday), "")
        metric_labels = {'metric_key_name':metric_key_name, 'bucket': bucket, 'instrument': instrument, 'obsday':obsday, 'map_obs_day': mapped_obsday}

        metric_name = f"{base_metric_name}"  # Unique metric name for each obsday
        # Create or update the metric
        gauge_metric = get_create_metric(metric_name,"Values per observation day for INGEST,FAIL,RECINSTR", "gauge", labels=list(metric_labels.keys()))
        gauge_metric.labels(**metric_labels).set(int(value))


def update_latency_metrics(bucket, instrument, obsday, latencies):
    base_metric_name = "redis_summit_embargo_latency"
    labels = ["bucket", "instrument", "obsday", "latency_type", "map_obs_day"]

    map_obs_day = {
        "today": datetime.now().strftime("%Y%m%d"),
        "today-1": (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
        "today-2": (datetime.now() - timedelta(days=2)).strftime("%Y%m%d"),
        "today-3": (datetime.now() - timedelta(days=3)).strftime("%Y%m%d"),
        "today-4": (datetime.now() - timedelta(days=4)).strftime("%Y%m%d"),
        "today-5": (datetime.now() - timedelta(days=5)).strftime("%Y%m%d"),
        "today-6": (datetime.now() - timedelta(days=6)).strftime("%Y%m%d")
    }
    mapped_obsday = next((k for k, v in map_obs_day.items() if v == obsday), "")

    if latencies:
        min_latency = min(latencies)
        max_latency = max(latencies)
        avg_latency = sum(latencies) / len(latencies)

        for latency_type, value in [("min", min_latency), ("max", max_latency), ("avg", avg_latency)]:
            metric_labels = {
                'bucket': bucket,
                'instrument': instrument,
                'obsday': obsday,
                'latency_type': latency_type,
                'map_obs_day': mapped_obsday
            }
            metric_name = f"{base_metric_name}"
            gauge_metric = get_create_metric(metric_name, "Latency metrics", "gauge", labels=list(metric_labels.keys()))
            gauge_metric.labels(**metric_labels).set(value)

        # Update histogram
        hist_metric = get_create_metric(f"{base_metric_name}_histogram", "Latency histogram", "histogram", labels=["bucket", "instrument", "obsday", "map_obs_day"])
        for latency in latencies:
            hist_metric.labels(bucket=bucket, instrument=instrument, obsday=obsday, map_obs_day=mapped_obsday).observe(latency)


#Startup in charge of connect to Redis
@app.on_event('startup')
async def startup_event():
    global REDIS
    try:
        redis_url = environ.get('REDIS_URL', 'redis://redis:6379')
        if 'REDIS_URL' in environ:
            logger.info({"event": "Using REDIS_URL from environment","redis_url": redis_url})
        else:
            logger.info({"event": "REDIS_URL not found in environment. Using default value","redis_url": redis_url})

        raw_password = environ.get('REDIS_PASSWORD')
        try:
            encoded_password = base64.b64decode(raw_password).decode('utf-8')
            password_is_encoded = True
        except Exception:
            encoded_password = raw_password
            password_is_encoded = False

        if password_is_encoded:
            logger.info({"event": "Using decoded REDIS_PASSWORD from environment"})
        elif raw_password:
            logger.info({"event": "Using plain REDIS_PASSWORD from environment"})
        else:
            logger.error({"event": "REDIS_PASSWORD not found in environment","password_usage": "Proceeding without password"})

        try:
            REDIS = await aioredis.from_url(redis_url, password=encoded_password)
        except AuthenticationError:
            logger.error({"event": "Authentication failed", "exception": "Check URL or password"})
            raise Exception("Authentication failed: Check URL or password")
    except Exception as e:
        logger.error({"event": "Error trying to connect to Redis","exception": str(e),"traceback": "See below for traceback"},exc_info=True)

#Redis-Info endpoint. Gets the values from the redis db
@app.get("/redis-info")
async def get_info():
    if REDIS is not None:
        try:
            async with REDIS.client() as redis:
                info = await redis.info()
                db_logger.info({"event": "Getting Redis db information"})
                for key, value in info.items():
                    metric_base_name = f"redis_db_info_{key}"
                    if isinstance(value, int):
                        gauge_metric = get_create_metric(metric_base_name, 'redis database info', 'gauge')
                        gauge_metric.set(value)
                        logger.debug({"event": f"redis_db_info_{key}", "key": key, "value": value})
                    elif isinstance(value, str) and "count=" in value:
                        match = re.search(r"count=(\d+)", value)
                        if match:
                            count_value = int(match.group(1))
                            gauge_metric = get_create_metric(metric_base_name, 'redis database info', 'gauge')
                            gauge_metric.set(count_value)
                            logger.debug({"event": f"redis_db_info_{key}", "key": key, "value": count_value})
                        else:
                            logger.warning({"event": "No count= value found", "key": key})
                    elif isinstance(value, str):
                        info_metric = get_create_metric(metric_base_name, 'redis database info', 'info')
                        info_metric.info({key: value})
                        logger.debug({"event": f"redis_db_info_{key}", "key": key, "value": value})
                return info
        except Exception as e:
            logger.error({
                "event": "Error in /redis-info endpoint",
                "exception": str(e),
                "traceback": "See below for traceback"
            }, exc_info=True)
            return {"error": "Error retrieving Redis info"}
    else:
        logger.info({"event": "Redis not connected", "action": "Attempting to access /redis-info"})
        return {"error": "Redis not connected"}

#Get all the keys and values for hashes and strings
@app.get("/get-redis-keys")
async def get_redis_keys():
    if REDIS is not None:
        try:
            async with REDIS.client() as redis:
                keys = await redis.keys('*')  # Fetch all keys
                hash_key_values = []
                for key in keys:
                    key_type = await redis.type(key)  # Check the type of each key
                    if key_type == b'hash':
                        # Fetch all fields and values for hash keys
                        hash_fields_values = await redis.hgetall(key)
                        # Convert field and values to a more readable format
                        hash_fields_values_readable = {field.decode("utf-8"): value.decode("utf-8") for field, value in hash_fields_values.items()}
                        hash_key_values.append({"key": key.decode("utf-8"), "values": hash_fields_values_readable})
                        db_logger.debug({
                            "event": "Hash key found",
                            "key": key.decode("utf-8"),
                            "values": hash_fields_values_readable,
                            "type":"hash"
                        })
                    elif key_type == b'string':
                        # Fetch the value for string keys
                        value = await redis.get(key)
                        value_readable = value.decode("utf-8") if isinstance(value, bytes) else value
                        db_logger.debug({
                            "event": "String key found",
                            "key": key.decode("utf-8"),
                            "value": value_readable,
                            "type": "string"
                        })
                    else:
                        # Log the event of not hash/string key
                        db_logger.debug({
                            "event": "Skipping key",
                            "key": key.decode("utf-8"),
                            "type": key_type.decode()
                        })
                return {"hashes": hash_key_values}
        except Exception as e:
            logger.error({
                "event": "Error in /redis-hash-keys endpoint",
                "exception": str(e)
            })
            return {"error": "Error retrieving hash keys from Redis"}
    else:
        return {"error": "Redis not connected"}

#Get values in function of the type of hash
async def fetch_hash_keys_by_prefix(prefix: str):
    if REDIS is not None:
        try:
            async with REDIS.client() as redis:
                db_logger.info({"event": f"Getting {prefix} hash information"})
                cursor = '0'
                hash_key_values = []
                latencies = []
                while cursor != 0:
                    cursor, keys = await redis.scan(cursor, match=f"{prefix}:*", count=100)
                    for key in keys:
                        key_type = await redis.type(key)
                        if key_type == b'hash' and prefix != "FILE":
                            hash_fields_values = await redis.hgetall(key)
                            hash_fields_values_readable = {field.decode("utf-8"): value.decode("utf-8") for field, value in hash_fields_values.items()}
                            db_logger.info({"event": f"{prefix} hash key found","key": key.decode("utf-8")})
                            hash_key_values.append({"key": key.decode("utf-8"), "values": hash_fields_values_readable})
                            db_logger.debug({
                                "event": f"{prefix} hash key found",
                                "key": key.decode("utf-8"),
                                "values": hash_fields_values_readable
                            })
                            update_gauge_metric(prefix, key.decode("utf-8"), hash_fields_values_readable)
                        if key_type == b'hash' and prefix == "FILE":
                            hash_fields_values = await redis.hgetall(key)
                            hash_fields_values_readable = {field.decode("utf-8"): value.decode("utf-8") for field, value in hash_fields_values.items()}
                            key_str = key.decode("utf-8")
                            file_date = re.search(r'\d{8}', key_str)
                            file_date = file_date.group()
                            #yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d") # this is for test should work with file_date == datetime.now().strftime("%Y%m%d")
                            if set(hash_fields_values_readable.keys()) == {"recv_time", "ingest_time"} and file_date == datetime.now().strftime("%Y%m%d"):
                                db_logger.debug({"event": f"Computing latency for {key_str}"})
                                recv_time = float(hash_fields_values_readable["recv_time"])
                                ingest_time = float(hash_fields_values_readable["ingest_time"])
                                time_diff = ingest_time - recv_time
                                latencies.append(time_diff)
                                hash_fields_values_readable["latency"]=time_diff
                                hash_key_values.append({"key": key.decode("utf-8"), "values": hash_fields_values_readable})
                                db_logger.debug({
                                    "event": f"{prefix} hash key found",
                                    "key": key.decode("utf-8"),
                                    "values": hash_fields_values_readable
                                })

                            else:
                                db_logger.debug({"event": f"Skipping latency for {key_str}"})
                    if cursor == '0':
                        break
                if latencies:
                    update_latency_metrics('rubin-summit', 'LATISS', file_date, latencies)
                return hash_key_values
        except Exception as e:
            logger.error({
                "event": f"Error in fetch_hash_keys_by_prefix_{prefix}",
                "exception": str(e)
            })
            return {"error": "Error retrieving hash keys from Redis"}
    else:
        return {"error": "Redis not connected"}

# Final endpoint
@app.get("/redisdb-file-info")
async def redisdb_file_info():
    # Measure and update metrics for get_info

    start_time = time.time()
    redis_db_info_data = await get_info()
    duration = time.time() - start_time
    hist_get_info = get_create_metric("redis_db_info_duration_seconds", "Time taken for get_info operation", "histogram")
    gauge_get_info = get_create_metric("redis_db_info_last_duration_seconds", "Last duration for get_info operation", "gauge")
    hist_get_info.observe(duration)
    gauge_get_info.set(duration)

    # Measure and update metrics for INGEST
    start_time = time.time()
    redis_db_INGEST_data =  await fetch_hash_keys_by_prefix('INGEST')
    duration = time.time() - start_time
    hist_ingest = get_create_metric("redis_db_INGEST_duration_seconds", "Time taken for fetch_hash_keys_by_prefix operation with INGEST prefix", "histogram")
    gauge_ingest = get_create_metric("redis_db_INGEST_last_duration_seconds", "Last duration for fetch_hash_keys_by_prefix operation with INGEST prefix", "gauge")
    hist_ingest.observe(duration)
    gauge_ingest.set(duration)

    # Measure and update metrics for FAIL
    start_time = time.time()
    redis_db_INGEST_data =  await fetch_hash_keys_by_prefix('FAIL')
    duration = time.time() - start_time
    hist_fail = get_create_metric("redis_db_FAIL_duration_seconds", "Time taken for fetch_hash_keys_by_prefix operation with FAIL prefix", "histogram")
    gauge_fail = get_create_metric("redis_db_FAIL_last_duration_seconds", "Last duration for fetch_hash_keys_by_prefix operation with FAIL prefix", "gauge")
    hist_fail.observe(duration)
    gauge_fail.set(duration)

    # Measure and update metrics for FAIL
    start_time = time.time()
    redis_db_INGEST_data =  await fetch_hash_keys_by_prefix('RECINSTR')
    duration = time.time() - start_time
    hist_recinstr = get_create_metric("redis_db_RECINSTR_duration_seconds", "Time taken for fetch_hash_keys_by_prefix operation with RECINSTR prefix", "histogram")
    gauge_recinstr = get_create_metric("redis_db_RECINSTR_last_duration_seconds", "Last duration for fetch_hash_keys_by_prefix operation with RECINSTR prefix", "gauge")
    hist_recinstr.observe(duration)
    gauge_recinstr.set(duration)

    # Measure and update metrics for FILE
    start_time = time.time()
    redis_db_FILE_data = await fetch_hash_keys_by_prefix('FILE')
    duration = time.time() - start_time
    hist_file = get_create_metric("redis_db_FILE_duration_seconds", "Time taken for fetch_hash_keys_by_prefix operation with FILE prefix", "histogram")
    gauge_file = get_create_metric("redis_db_FILE_last_duration_seconds", "Last duration for fetch_hash_keys_by_prefix operation with FILE prefix", "gauge")
    hist_file.observe(duration)
    gauge_file.set(duration)


@app.get("/")
def index():
  index_counter.inc()
  return f"REDIS STATE: {REDIS}"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
