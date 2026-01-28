#!/bin/bash

# 共通設定とユーティリティ関数

set -e

# カラー定義
export GREEN='\033[0;32m'
export YELLOW='\033[1;33m'
export BLUE='\033[0;34m'
export RED='\033[0;31m'
export NC='\033[0m'

# 共通変数
export KAFKA_CONTAINER="watermill-kafka"
export KAFKA_BOOTSTRAP="localhost:9092"
export INPUT_TOPIC="greeting-input"
export OUTPUT_TOPIC="greeting-output"

# ユーティリティ関数
print_info() {
    echo -e "${YELLOW}$1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_header() {
    echo -e "${BLUE}$1${NC}"
}

print_step() {
    echo -e "${YELLOW}[$1] $2${NC}"
}
