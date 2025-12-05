#!/bin/bash
set -e  # stop on any error

echo "==== Building Docker images ===="
docker build -t hzz-worker:latest ./worker
docker build -t hzz-producer:latest ./producer
docker build -t hzz-aggregator:latest ./aggregator