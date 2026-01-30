#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/common.sh"

# デフォルト設定
MESSAGE_COUNT=${1:-100}
TIMEOUT=${2:-60}

print_header "========================================"
print_header "     Performance Test Suite"
print_header "========================================"
echo ""
print_info "Configuration:"
echo "  Messages: ${MESSAGE_COUNT}"
echo "  Timeout:  ${TIMEOUT}s"
echo ""

# Step 1: サービス確認・アプリ停止
print_step "Step 1/5" "Preparing environment..."
if ! docker ps | grep -q ${KAFKA_CONTAINER}; then
    print_error "Kafka container is not running. Run 'make start' first."
    exit 1
fi

# アプリコンテナを停止（Kafkaは起動したまま）
docker-compose -f "${PROJECT_DIR}/docker-compose.yml" stop app 2>/dev/null || true
sleep 2
print_success "App stopped, Kafka running"
echo ""

# 新しいコンシューマーグループ
CONSUMER_GROUP="perf-test-$(date +%s)"

# Step 2: メッセージを事前送信
print_step "Step 2/5" "Pre-loading ${MESSAGE_COUNT} messages to Kafka..."
for i in $(seq 1 ${MESSAGE_COUNT}); do
    hour=$((i % 24))
    message="{\"time\":\"2024-01-01T$(printf '%02d' $hour):00:00\"}"
    echo "$message"
done | docker exec -i ${KAFKA_CONTAINER} /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --topic ${INPUT_TOPIC} 2>/dev/null

print_success "All messages loaded to input topic"
echo ""

# Step 3: アプリ起動＆計測開始
print_step "Step 3/5" "Starting app and measuring processing time..."
PROCESS_START=$(date +%s.%N)

docker-compose -f "${PROJECT_DIR}/docker-compose.yml" start app 2>/dev/null
print_info "App started, waiting for processing..."

# Step 4: 出力トピックをポーリングして全件受信を待つ
print_step "Step 4/5" "Waiting for all messages to be processed..."
RECEIVED=0
POLL_INTERVAL=1
ELAPSED=0

while [ ${RECEIVED} -lt ${MESSAGE_COUNT} ] && [ ${ELAPSED} -lt ${TIMEOUT} ]; do
    sleep ${POLL_INTERVAL}
    ELAPSED=$((ELAPSED + POLL_INTERVAL))

    RECEIVED=$(docker exec ${KAFKA_CONTAINER} /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server ${KAFKA_BOOTSTRAP} \
        --topic ${OUTPUT_TOPIC} \
        --group ${CONSUMER_GROUP} \
        --from-beginning \
        --timeout-ms 1000 2>/dev/null | wc -l || echo "0")

    echo -ne "\r  Received: ${RECEIVED}/${MESSAGE_COUNT} (${ELAPSED}s elapsed)"
done

PROCESS_END=$(date +%s.%N)
PROCESS_TIME=$(echo "$PROCESS_END - $PROCESS_START" | bc)
echo ""
echo ""

# Step 5: 結果表示
print_step "Step 5/5" "Results"
echo ""
print_header "========================================"
print_header "     Performance Results"
print_header "========================================"
echo ""
echo "  Messages sent:     ${MESSAGE_COUNT}"
echo "  Messages received: ${RECEIVED}"
echo "  Processing time:   ${PROCESS_TIME}s"

if [ "${RECEIVED}" -gt 0 ]; then
    THROUGHPUT=$(echo "scale=2; ${RECEIVED} / ${PROCESS_TIME}" | bc)
    echo "  Throughput:        ${THROUGHPUT} msg/s"
fi

echo ""
if [ "${RECEIVED}" -ge "${MESSAGE_COUNT}" ]; then
    print_success "All messages processed successfully!"
else
    print_error "Timeout or messages lost (${RECEIVED}/${MESSAGE_COUNT})"
fi
