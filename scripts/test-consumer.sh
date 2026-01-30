#!/bin/bash
source "$(dirname "$0")/common.sh"

print_header "Starting Kafka Consumer..."
print_info "Listening for messages... Press Ctrl+C to exit."
echo ""

docker exec -it ${KAFKA_CONTAINER} /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --topic ${OUTPUT_TOPIC} \
    --from-beginning
