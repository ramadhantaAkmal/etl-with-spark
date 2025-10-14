#!/bin/bash

docker-compose exec spark-master service ssh restart
docker-compose exec spark-worker service ssh restart