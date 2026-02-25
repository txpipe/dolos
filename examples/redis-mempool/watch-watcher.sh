#!/bin/bash

# Monitor which Dolos node currently holds the watcher lock
# This script polls Redis every 2 seconds and displays the current watcher

REDIS_CONTAINER="redis-mempool-redis"
LOCK_KEY="dolos:mempool:watcher:lock"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if redis container is running
check_redis() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${REDIS_CONTAINER}$"; then
        echo -e "${RED}Error: Redis container '${REDIS_CONTAINER}' is not running${NC}"
        echo "Start the example with: docker-compose up -d"
        exit 1
    fi
}

# Get watcher info from Redis
get_watcher_info() {
    local watcher_id
    local ttl
    
    watcher_id=$(docker exec "${REDIS_CONTAINER}" redis-cli GET "${LOCK_KEY}" 2>/dev/null)
    ttl=$(docker exec "${REDIS_CONTAINER}" redis-cli TTL "${LOCK_KEY}" 2>/dev/null)
    
    if [ -z "$watcher_id" ] || [ "$watcher_id" = "(nil)" ]; then
        echo -e "${YELLOW}No active watcher (lock expired or not set)${NC}"
    else
        # Shorten UUID for display
        local short_id="${watcher_id:0:8}"
        
        if [ "$ttl" -lt 0 ]; then
            echo -e "${YELLOW}Current watcher: ${short_id}... (no TTL - may have just been set)${NC}"
        else
            echo -e "${GREEN}Current watcher: ${short_id}... (TTL: ${ttl}s)${NC}"
        fi
    fi
}

# Main loop
echo "=========================================="
echo "Redis Mempool Watcher Monitor"
echo "=========================================="
echo "Press Ctrl+C to exit"
echo ""

check_redis

# Clear screen and start monitoring
clear
while true; do
    # Move cursor to top
    tput cup 0 0
    
    echo "=========================================="
    echo "Redis Mempool Watcher Monitor"
    echo "=========================================="
    echo ""
    
    get_watcher_info
    
    echo ""
    echo "Last update: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Press Ctrl+C to exit"
    
    sleep 2
done
