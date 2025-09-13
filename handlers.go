package main

import (
    "database/sql"
    "encoding/json"
    "io"
    "net/http"

    "github.com/go-chi/chi/v5"
    "github.com/go-redis/redis/v8"
)

func handlePut(redisClient *redis.Client, db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        tenant := chi.URLParam(r, "tenant")
        key := chi.URLParam(r, "key")

        if !checkRateLimit(r.Context(), redisClient, tenant, "write") {
            http.Error(w, "Write quota exceeded", http.StatusTooManyRequests)
            return
        }

        body, err := io.ReadAll(r.Body)
        if err != nil || len(body) > 64*1024 {
            http.Error(w, "Invalid body or size exceeds 64 KiB", http.StatusBadRequest)
            return
        }

        var jsonCheck interface{}
        if err := json.Unmarshal(body, &jsonCheck); err != nil {
            http.Error(w, "Invalid JSON", http.StatusBadRequest)
            return
        }

        _, err = redisClient.HSet(r.Context(), "tenant:"+tenant, key, body).Result()
        if err != nil {
            http.Error(w, "Failed to store value", http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusOK)
    }
}

func handleGet(redisClient *redis.Client) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        tenant := chi.URLParam(r, "tenant")
        key := chi.URLParam(r, "key")

        if !checkRateLimit(r.Context(), redisClient, tenant, "read") {
            http.Error(w, "Read quota exceeded", http.StatusTooManyRequests)
            return
        }

        value, err := redisClient.HGet(r.Context(), "tenant:"+tenant, key).Result()
        if err == redis.Nil {
            http.Error(w, "Key not found", http.StatusNotFound)
            return
        } else if err != nil {
            http.Error(w, "Failed to retrieve value", http.StatusInternalServerError)
            return
        }

        w.Header().Set("Content-Type", "application/json")
        w.Write([]byte(value))
    }
}

func handleDelete(redisClient *redis.Client, db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        tenant := chi.URLParam(r, "tenant")
        key := chi.URLParam(r, "key")

        if !checkRateLimit(r.Context(), redisClient, tenant, "write") {
            http.Error(w, "Write quota exceeded", http.StatusTooManyRequests)
            return
        }

        _, err := redisClient.HDel(r.Context(), "tenant:"+tenant, key).Result()
        if err != nil {
            http.Error(w, "Failed to delete key", http.StatusInternalServerError)
            return
        }

        _, err = db.ExecContext(r.Context(), `DELETE FROM kv WHERE tenant = $1 AND key = $2`, tenant, key)
        if err != nil {
            http.Error(w, "Failed to delete from database", http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusOK)
    }
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
    http.ServeFile(w, r, "static/index.html")
}