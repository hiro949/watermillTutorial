#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/common.sh"

print_header "========================================"
print_header "     Integration Test Suite"
print_header "========================================"
echo ""

# テスト結果を保持
TEST_EXIT_CODE=0

# クリーンアップ関数
cleanup() {
    echo ""
    print_info "[Cleanup] Stopping services..."
    make -C "${PROJECT_DIR}" stop >/dev/null 2>&1
    print_success "[Cleanup] Done"
    exit $TEST_EXIT_CODE
}

trap cleanup EXIT

# Step 1: 環境の初期化
print_step "Step 1/5" "Cleaning up environment..."
FORCE=1 make -C "${PROJECT_DIR}" clean 2>/dev/null || true
echo ""

# Step 2: ビルド
print_step "Step 2/5" "Building Docker images..."
make -C "${PROJECT_DIR}" build
if [ $? -ne 0 ]; then
    print_error "Build failed!"
    TEST_EXIT_CODE=1
    exit 1
fi
echo ""

# Step 3: サービス起動
print_step "Step 3/5" "Starting services..."
make -C "${PROJECT_DIR}" start
echo "Waiting for services to be ready..."
sleep 5
print_success "Services started"
echo ""

# Step 4: テスト実行
print_step "Step 4/5" "Running tests..."
echo ""

# テストデータ
declare -A TEST_CASES=(
    ["09:00:00"]="Good morning!"
    ["14:00:00"]="Good afternoon!"
    ["19:00:00"]="Good evening!"
    ["02:00:00"]="Good night!"
)

# テストメッセージを送信
print_info "Sending test messages..."
for time in "${!TEST_CASES[@]}"; do
    message="{\"time\":\"2024-01-01T${time}\"}"
    echo "  Sending: $message"
    echo "$message" | docker exec -i ${KAFKA_CONTAINER} /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server ${KAFKA_BOOTSTRAP} \
        --topic ${INPUT_TOPIC} 2>/dev/null
    sleep 0.5
done

# 処理待ち
echo ""
print_info "Waiting for processing..."
sleep 3

# 結果を取得
echo ""
print_info "Checking results..."
RESULTS=$(docker exec ${KAFKA_CONTAINER} /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --topic ${OUTPUT_TOPIC} \
    --from-beginning \
    --timeout-ms 3000 2>/dev/null || true)

echo ""
print_info "Results received:"
echo "$RESULTS" | tail -8

# 結果を検証
echo ""
PASSED=0
FAILED=0

for time in "${!TEST_CASES[@]}"; do
    expected="${TEST_CASES[$time]}"
    if echo "$RESULTS" | grep -q "$expected"; then
        echo -e "  ${GREEN}PASS${NC}: $time -> $expected"
        ((PASSED++)) || true
    else
        echo -e "  ${RED}FAIL${NC}: $time -> expected '$expected'"
        ((FAILED++)) || true
    fi
done

# Step 5: サマリー
echo ""
print_step "Step 5/5" "Test Summary"
echo "========================================"
if [ $FAILED -eq 0 ]; then
    print_success "All tests passed! ($PASSED/$PASSED)"
    TEST_EXIT_CODE=0
else
    print_error "Some tests failed! (Passed: $PASSED, Failed: $FAILED)"
    TEST_EXIT_CODE=1
fi
