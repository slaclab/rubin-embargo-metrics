CONTAINER_RT?=sudo docker
REPO?=slaclab
POD?=rubin-embargo-metrics
TAG?=latest

PYTHON_BIN?=./bin/python3
PIP_BIN?=./bin/pip3
VENV_BIN?=./bin/activate
UVICORN?=./bin/uvicorn

build:
	$(CONTAINER_RT) build -t $(REPO)/$(POD):$(TAG) .

push:
	$(CONTAINER_RT) push $(REPO)/$(POD):$(TAG)

containers: build push

virtualenv:
	python3 -m venv .

pip:
	$(PYTHON_BIN) -m pip install --upgrade pip
	source $(VENV_BIN) && $(PIP_BIN) install -r requirements.txt

devenv: virtualenv pip


start:
	source $(VENV_BIN) && $(UVICORN) main:app --host 0.0.0.0 --reload 
