#!/bin/bash
source "$(dirname "$0")/common.sh"

print_info "Starting services with Docker Compose..."
docker-compose up -d

echo ""
print_info "Waiting for services to be ready..."
sleep 3

echo ""
print_header "Service Status:"
docker-compose ps

echo ""
print_success "Services started successfully!"
echo ""
print_header "Available commands:"
echo "  make logs         - View all logs"
echo "  make logs-app     - View app logs"
echo "  make logs-kafka   - View Kafka logs"
echo "  make stop         - Stop services"
echo ""
print_header "Kafka Connection Info:"
echo "  Bootstrap Server: ${KAFKA_BOOTSTRAP}"
echo "  Input Topic:      ${INPUT_TOPIC}"
echo "  Output Topic:     ${OUTPUT_TOPIC}"
echo ""
print_header "Quick Test:"
echo "  make test-producer  # Send messages to Kafka"
echo "  make test-consumer  # Receive messages from Kafka"
