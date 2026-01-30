#!/bin/bash
source "$(dirname "$0")/common.sh"

print_header "Starting Kafka Producer..."
print_info "Type your messages and press Enter. Press Ctrl+C to exit."
echo ""

docker exec -it ${KAFKA_CONTAINER} /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --topic ${INPUT_TOPIC}
