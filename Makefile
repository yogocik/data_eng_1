IMAGE_NAME?=python-tables
IMAGE_TAG?=latest
CONTAINER_NAME?=python-tables-container

requirements:
	poetry export -f requirements.txt --without-hashes --output requirements.txt
local-run:
	python ./solution/main.py
docker-build:
	sudo docker rmi -f $(IMAGE_NAME):$(IMAGE_TAG) && sudo docker build -t $(IMAGE_NAME):$(IMAGE_TAG) . || echo "Failed build image $(IMAGE_NAME):$(IMAGE_TAG)"
docker-remove:
	sudo docker container stop $(CONTAINER_NAME) && sudo docker rm $(CONTAINER_NAME) || echo "Container $(CONTAINER_NAME) is not existed or deleted"
docker-run:
	make docker-remove & sudo docker run -it --name $(CONTAINER_NAME) $(IMAGE_NAME):$(IMAGE_TAG) || echo "Failed run image $(IMAGE_NAME):$(IMAGE_TAG)"