#!/bin/bash
source "$(dirname "$0")/common.sh"

print_info "Stopping services..."
docker-compose stop

echo ""
print_success "Services stopped successfully!"
echo ""
echo "Note: Containers and volumes are preserved."
echo "  make start  - To restart services"
echo "  make clean  - To remove everything"
