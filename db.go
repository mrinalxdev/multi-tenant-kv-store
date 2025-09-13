package main

import (
    "context"
    "database/sql"
    "log"
    "time"

    "github.com/go-redis/redis/v8"
)

func flushToPostgres(ctx context.Context, redisClient *redis.Client, db *sql.DB) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            tenants, err := redisClient.Keys(ctx, "tenant:*").Result()
            if err != nil {
                log.Printf("Error fetching tenants: %v", err)
                continue
            }

            for _, tenantKey := range tenants {
                tenant := tenantKey[len("tenant:"):]
                keys, err := redisClient.HKeys(ctx, tenantKey).Result()
                if err != nil {
                    log.Printf("Error fetching keys for tenant %s: %v", tenant, err)
                    continue
                }

                for _, key := range keys {
                    value, err := redisClient.HGet(ctx, tenantKey, key).Result()
                    if err != nil {
                        continue
                    }

                    _, err = db.ExecContext(ctx, `
                        INSERT INTO kv (tenant, key, value, updated)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (tenant, key) DO UPDATE
                        SET value = $3, updated = $4`,
                        tenant, key, value, time.Now())
                    if err != nil {
                        log.Printf("Error flushing to PostgreSQL: %v", err)
                    }

                    var totalSize int64
                    err = db.QueryRowContext(ctx, `SELECT SUM(LENGTH(value)) FROM kv WHERE tenant = $1`, tenant).Scan(&totalSize)
                    if err != nil {
                        log.Printf("Error checking storage size: %v", err)
                        continue
                    }
                    if totalSize > 100*1024*1024 { // 100 MB
                        var oldestKey string
                        var oldestTime time.Time
                        row := db.QueryRowContext(ctx, `SELECT key, updated FROM kv WHERE tenant = $1 ORDER BY updated ASC LIMIT 1`, tenant)
                        if err := row.Scan(&oldestKey, &oldestTime); err == nil {
                            _, err = db.ExecContext(ctx, `DELETE FROM kv WHERE tenant = $1 AND key = $2`, tenant, oldestKey)
                            if err != nil {
                                log.Printf("Error deleting oldest key: %v", err)
                            }
                            redisClient.HDel(ctx, tenantKey, oldestKey)
                        }
                    }
                }
            }
        }
    }
}