#!/bin/bash
# wait-for-rabbitmq.sh

# Define the RabbitMQ credentials
RABBITMQ_USER="attendify"
RABBITMQ_PASS="uXe5u1oWkh32JyLA"

# Wait for RabbitMQ to become ready by checking the alarms endpoint with authentication
until curl -u "$RABBITMQ_USER:$RABBITMQ_PASS" -s http://rabbitmq:15672/api/health/checks/alarms | grep -q '"status":"ok"'; do
  echo "Waiting for RabbitMQ to be ready..."
  sleep 5
done

# Once RabbitMQ is ready, run the Python script
echo "RabbitMQ is ready, running configure.py"
