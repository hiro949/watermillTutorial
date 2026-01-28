#!/bin/bash
source "$(dirname "$0")/common.sh"

print_info "Building Docker images..."
docker-compose build --no-cache

echo ""
print_success "Build completed successfully!"
echo ""
echo "Next steps:"
echo "  make start  - Start all services"
echo "  make help   - Show all available commands"
