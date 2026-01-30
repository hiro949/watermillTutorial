#!/bin/bash
source "$(dirname "$0")/common.sh"

# -y/--yes オプションまたは FORCE=1 環境変数で確認をスキップ
SKIP_CONFIRM=${FORCE:-false}
for arg in "$@"; do
    case $arg in
        -y|--yes)
            SKIP_CONFIRM=true
            ;;
    esac
done

if [ "$SKIP_CONFIRM" != "true" ] && [ "$SKIP_CONFIRM" != "1" ]; then
    print_error "WARNING: This will remove all containers, networks, and volumes!"
    print_info "Are you sure you want to continue? [y/N] "
    read -r response

    if [[ ! "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        echo "Cleanup cancelled."
        exit 0
    fi
fi

echo ""
print_info "Stopping and removing containers..."
docker-compose down

echo ""
print_info "Removing volumes..."
docker-compose down -v

echo ""
print_info "Removing unused Docker resources..."
docker system prune -f

echo ""
print_success "Cleanup completed successfully!"
echo ""
echo "All containers, networks, and volumes have been removed."
echo "  make build  - To rebuild images"
echo "  make start  - To start services"
