package main

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

// Lua script for rate limiting
const rateLimitScript = `
local tenant = ARGV[1]
local action = ARGV[2]
local now = tonumber(ARGV[3])
local limit = action == "write" and 1000 or 10000
local key = "quota:" .. tenant .. ":" .. action
local count = redis.call("GET", key) or 0
count = tonumber(count)
if count >= limit then
    return 0
end
redis.call("INCR", key)
redis.call("EXPIRE", key, 60)
return 1
`

func initRedis(addr string) *redis.Client {
    client := redis.NewClient(&redis.Options{Addr: addr})
    _, err := client.Ping(context.Background()).Result()
    if err != nil {
        log.Fatalf("Failed to connect to Redis: %v", err)
    }
    return client
}

func checkRateLimit(ctx context.Context, client *redis.Client, tenant, action string) bool {
    result, err := client.Eval(ctx, rateLimitScript, nil, tenant, action, time.Now().Unix()).Int()
    if err != nil {
        log.Printf("Rate limit check failed: %v", err)
        return false
    }
    return result == 1
}