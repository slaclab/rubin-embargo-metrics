# rubin-embargo-metrics
Simple FastAPI app to gather metrics from a Redis store and publish as prometheus /metrics

metrics from Redis has the following convention:

## RECINSTR:
```
RECINSTR:{bucket}:{instrument}
```
has a key for each `day_obs` giving the number of files received for ingest

## INGEST:
```
INGEST:{bucket}:{instrument}}
```
has a key for each `day_obs` giving the number of succesfull files ingested. 

## FAIL:
```
FAIL:{bucket}:{instrument}
```
has a key for each `day_obs` giving the number of ingest or metadata translation failures (including retries).

## MAXSEQ:
```
MAXSEQ:{bucket}:{instrument}:{day_obs
```
contains the largest `seq_num` seen for that instrument on that day_obs.

## FILE:
```
FILE:{bucket}/{instrument}/{day_obs}/{path}
```
contains a `recv_time` and an `ingest_time` key (if ingest was successful). `FILE:` keys are only retained for 7 days.
# making dev environment

```
make devenv
```

Will setup a virtual environment and configure all dependencies for the app

# testing

```
make start
```

will start the fastapi server against main.py

# building

```
make containers
```

will do a docker build and push as per the Makefile
