from collections import defaultdict
from fastapi import FastAPI, Request
from prometheus_client import make_asgi_app, Counter, Gauge, Info, Summary, Histogram
from redis import asyncio as aioredis
from scipy import stats
from datetime import datetime
from os import environ
import numpy as np
import logging
import urllib.parse
import traceback
import time

app = FastAPI()
REDIS = None
index_counter = Counter('index_counter', 'Description of counter')

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

metrics = defaultdict(lambda: None)

@app.on_event('startup')
async def startup_event():
  REDIS = await aioredis.from_url( environ.get('REDIS_URL', 'redis://localhost/1') )

def get_create_metric(metric_base_name, description, metric_type, labels=None):
    if metric_base_name not in metrics:
        if metric_type == 'gauge':
            metrics[metric_base_name] = Gauge(metric_base_name, description, labels) if labels is not None else Gauge(metric_base_name, description)
        elif metric_type == 'histogram':
            metrics[metric_base_name] = Histogram(metric_base_name, description, labels) if labels is not None else Histogram(metric_base_name, description)
        elif metric_type == 'summary':
            metrics[metric_base_name] = Summary(metric_base_name, description, labels) if labels is not None else Summary(metric_base_name, description)
        elif metric_type == 'info':
            metrics[metric_base_name] = Info(metric_base_name, description, labels) if labels is not None else Info(metric_base_name, description)
    return metrics[metric_base_name]

def export_metrics_ingest_info(dictionary,name):
    for bucket, instruments in dictionary.items():
        for instrument, days in instruments.items():
            for obs_day, data in days.items():
                day_label = data.get("day_label", obs_day).replace("-", "_").replace(" ", "_")
                if data.get("day_label")=="more_than_7_days_ago":
                    continue
                labels = ['type', 'day_window', 'obs_day', 'instrument']
                if "latencies" in data:
                    latencies = data["latencies"]
                    metric_base_name = f"redis_latencies_{name}"
                    histogram = get_create_metric(metric_base_name, 'Latency distribution', 'histogram', labels)
                    summary = get_create_metric(metric_base_name, 'Latency summary', 'summary', labels)

                    for latency in latencies:
                        histogram.labels(type='latency', day_window=day_label, obs_day=obs_day, instrument=instrument).observe(latency)
                        summary.labels(type='latency', day_window=day_label, obs_day=obs_day, instrument=instrument).observe(latency)

                for stat in ["min_latency", "max_latency", "mean_latency", "std_deviation", "range", "min_failure_ingest", "max_failure_ingest", "mean_failure_ingest", "count","rate_of_change","skewness","kurtosis"]:
                    if stat in data:
                        metric_base_name = f"redis_metric_{name}"
                        gauge = get_create_metric(metric_base_name, 'Latency and failure metrics for instruments', 'gauge', labels)
                        gauge.labels(type=stat, day_window=day_label, obs_day=obs_day, instrument=instrument).set(data[stat])

                if "percentiles" in data:
                    for percentile, value in data["percentiles"].items():
                        formatted_percentile = f"percentile_{name}_{percentile}".replace(".", "_")
                        metric_base_name = f"redis_percentile_{name}"
                        gauge = get_create_metric(metric_base_name, 'Latency percentile metrics for instruments', 'gauge', labels)
                        gauge.labels(type=formatted_percentile, day_window=day_label, obs_day=obs_day, instrument=instrument).set(value)

@app.get("/ingest-info")
async def ingest_info():
    current_date = datetime.now().strftime('%Y%m%d')
    if REDIS is not None:
        try:
            async with REDIS.client() as redis:
                file_keys = await redis.keys("FILE:*")
                latency_by_arg = {}
                not_ingested_data = {}
                fail_info_by_arg = {}
                entries_onekey = {}
                entries_nokeys = {}
                for file_key in file_keys:
                    file_data = await redis.hgetall(file_key)
                    file_data_decoded = {key.decode('utf-8'): value.decode('utf-8') for key, value in file_data.items()}

                    parts = file_key.decode('utf-8').split('/')
                    if len(parts) > 3:
                        bucket = parts[0].split(':')[1]
                        instrument = parts[1]
                        obs_day = parts[2]
                        full_file_name = '/'.join(parts[3:])
                        num_keys = len(file_data_decoded)
                        # Populating latency_data
                        if num_keys == 2 and ('recv_time' in file_data_decoded and 'ingest_time' in file_data_decoded):
                            try:
                                recv_time = float(file_data_decoded['recv_time'])
                                ingest_time = float(file_data_decoded['ingest_time'])
                                latency_value = ingest_time - recv_time
                                day_data = latency_by_arg.setdefault(bucket, {}).setdefault(f"{instrument}", {}).setdefault(obs_day, {"latencies": [], "count": 0})
                                day_data["latencies"].append(latency_value)
                                day_data["count"] += 1
                                if len(day_data["latencies"]) > 1:
                                    latencies = np.array(day_data["latencies"])
                                    day_data["min_latency"] = latencies.min()
                                    day_data["max_latency"] = latencies.max()
                                    day_data["mean_latency"] = latencies.mean()
                                    day_data["std_deviation"] = latencies.std()
                                    day_data["range"] = day_data["max_latency"] - day_data["min_latency"]
                                    day_data["percentiles"] = {p: np.percentile(latencies, p) for p in [25, 50, 75, 90, 95, 99]}
                                    day_data["rate_of_change"] = np.diff(latencies).mean()
                                    day_data["skewness"] = stats.skew(latencies)
                                    day_data["kurtosis"] = stats.kurtosis(latencies)
                                else:
                                    day_data["std_deviation"] = 0
                                    day_data["range"] = 0
                                    day_data["percentiles"] = {}
                                    day_data["rate_of_change"] = 0
                                    day_data["skewness"] = 0
                                    day_data["kurtosis"] = 0

                                obs_date = datetime.strptime(obs_day, '%Y%m%d')
                                current_date_obj = datetime.strptime(current_date, '%Y%m%d')
                                days_diff = (current_date_obj - obs_date).days
                                if days_diff == 0:
                                    day_label = "today"
                                elif days_diff <= 7:
                                    day_label = f"{days_diff}_days_ago"
                                else:
                                    day_label = f"more_than_7_days_ago"  # or any other label you prefer for dates beyond 7 days
                                day_data["day_label"] = day_label
                            except ValueError:
                                logging.error(f"Non-float values for keys in {file_key.decode('utf-8')}: {file_data_decoded}")
                        #Populating not ingested data
                        if num_keys == 1 and 'recv_time' in file_data_decoded:
                            try:
                                recv_time = float(file_data_decoded['recv_time'])
                                current_time_ns = time.time_ns()
                                latency_value = current_time_ns - recv_time
                                not_ingested = not_ingested_data.setdefault(bucket, {}).setdefault(f"{instrument}", {}).setdefault(obs_day, {"current_latencies": [], "count": 0})
                                not_ingested["current_latencies"].append(latency_value)
                                not_ingested["count"] += 1

                                if len(not_ingested["current_latencies"]) > 1:
                                    current_latencies = np.array(not_ingested["current_latencies"])
                                    not_ingested["min_latency"] = current_latencies.min()
                                    not_ingested["max_latency"] = current_latencies.max()
                                    not_ingested["mean_latency"] = current_latencies.mean()
                                    not_ingested["std_deviation"] = current_latencies.std()
                                    not_ingested["range"] = not_ingested["max_latency"] - not_ingested["min_latency"]
                                    not_ingested["percentiles"] = {p: np.percentile(current_latencies, p) for p in [25, 50, 75, 90, 95, 99]}
                                    not_ingested["rate_of_change"] = np.diff(current_latencies).mean()
                                    not_ingested["skewness"] = stats.skew(current_latencies)
                                    not_ingested["kurtosis"] = stats.kurtosis(current_latencies)
                                else:
                                    not_ingested["std_deviation"] = 0
                                    not_ingested["range"] = 0
                                    not_ingested["percentiles"] = {}
                                    not_ingested["rate_of_change"] = 0
                                    not_ingested["skewness"] = 0
                                    not_ingested["kurtosis"] = 0
                                obs_date = datetime.strptime(obs_day, '%Y%m%d')
                                current_date_obj = datetime.strptime(current_date, '%Y%m%d')
                                days_diff = (current_date_obj - obs_date).days
                                if days_diff == 0:
                                    day_label = "today"
                                elif days_diff <= 7:
                                    day_label = f"{days_diff}_days_ago"
                                else:
                                    day_label = f"more_than_7_days_ago"  # or any other label you prefer for dates beyond 7 days
                                not_ingested["day_label"] = day_label
                            except ValueError:
                                logging.error(f"Non-float values for keys in {file_key.decode('utf-8')}: {file_data_decoded}")
                        #Populating ingest failures (not necesary keep failing)
                        elif 'ing_fail_count' in file_data_decoded and 'ing_fail_exc' in file_data_decoded:
                            try:
                                obs_date = datetime.strptime(obs_day, '%Y%m%d')
                                current_date_obj = datetime.strptime(current_date, '%Y%m%d')
                                days_diff = (current_date_obj - obs_date).days
                                if days_diff == 0:
                                    day_label = "today"
                                elif days_diff <= 7:
                                    day_label = f"{days_diff}_days_ago"
                                else:
                                    day_label = f"more_than_7_days_ago"
                                failure_ingest = fail_info_by_arg.setdefault(bucket, {}).setdefault(f"{instrument}", {}).setdefault(obs_day, {"failures": []})
                                failure_count = int(file_data_decoded['ing_fail_count'])
                                failure_ingest['failures'].append(failure_count)
                                failure_ingest['min_failure_ingest']=min(failure_ingest['failures'])
                                failure_ingest['max_failure_ingest']=max(failure_ingest['failures'])
                                failure_ingest['mean_failure_ingest']=sum(failure_ingest['failures'])/len(failure_ingest['failures'])
                                failure_ingest['count']=len(failure_ingest['failures'])
                                failure_ingest['day_label']= day_label
                            except ValueError:
                                logging.error(f"Non-float values for keys in {file_key.decode('utf-8')}: {file_data_decoded}")
                        #Populating FILE with no keys.
                    elif num_keys == 1:
                        try:
                                obs_date = datetime.strptime(obs_day, '%Y%m%d')
                                current_date_obj = datetime.strptime(current_date, '%Y%m%d')
                                days_diff = (current_date_obj - obs_date).days
                                if days_diff == 0:
                                    day_label = "today"
                                elif days_diff <= 7:
                                    day_label = f"{days_diff}_days_ago"
                                else:
                                    day_label = f"more_than_7_days_ago"
                                entries_nokeys.setdefault(bucket, {}).setdefault(f"{instrument}", {}).setdefault(obs_day, []).append({
                                'num_keys': 0,
                                'day_label': day_label})
                        except ValueError:
                            logging.error(f"Non-float values for keys in {file_key.decode('utf-8')}: {file_data_decoded}")
                    else:
                        logging.warning(f"Unexpected file key format: {file_key.decode('utf-8')}")

                export_metrics_ingest_info(latency_by_arg,"ingested_data")
                export_metrics_ingest_info(fail_info_by_arg,"failure_data")
                export_metrics_ingest_info(not_ingested_data,"not_ingested")
                export_metrics_ingest_info(entries_nokeys,"entries_with_no_keys")

                return {
                    "ingested_data": latency_by_arg,
                    "failure_data": fail_info_by_arg,
                    "not_ingested" : not_ingested_data,
                    "entries_with_no_keys": entries_nokeys
                }

        except Exception as e:
            logging.error(f"Error in /latency endpoint: {e}")
            logging.error("Traceback:", exc_info=True)
            return {"error": "Error calculating data"}
    else:
        return {"error": "Redis not connected"}

@app.get("/redis-info")
async def get_info():
    if REDIS is not None:
        try:
            async with REDIS.client() as redis:
                info = await redis.info()
                for key, value in info.items():
                    metric_base_name = f"redis_db_{key}"
                    if isinstance(value, int):
                        gauge_metric = get_create_metric(metric_base_name, 'redis databse info', 'gauge')
                        #metric = get_create_metric(key, 'gauge')
                        gauge_metric.set(value)
                    elif isinstance(value, str):
                        info_metric = get_create_metric(metric_base_name, 'redis databse info', 'info')
                        #metric = get_create_metric(key, 'info')
                        info_metric.info({key: value})
                return info
        except Exception as e:
            logging.error(f"Error in /redis-info endpoint: {e}")
            logging.error("Traceback:", exc_info=True)
            return {"error": "Error retrieving Redis info"}
    else:
        return {"error": "Redis not connected"}

@app.get("/")
def index():
  index_counter.inc()
  return f"REDIS STATE: {REDIS}"
