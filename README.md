# rubin-embargo-metrics
Simple FastAPI app to gather metrics from a Redis store and publish as prometheus /metrics

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
