package main

import (
    "context"
    "database/sql"
    "log"
    "net/http"
    "os"
    "os/signal"
    "time"

    "github.com/go-chi/chi/v5"
    "github.com/go-chi/chi/v5/middleware"
    _ "github.com/lib/pq"
)

func main() {
    redisClient := initRedis(os.Getenv("REDIS_ADDR"))
    db := initPostgres(os.Getenv("POSTGRES_DSN"))
    defer redisClient.Close()
    defer db.Close()

    ctx, cancel := context.WithCancel(context.Background())
    go flushToPostgres(ctx, redisClient, db)

    r := chi.NewRouter()
    r.Use(middleware.Logger)
    r.Use(middleware.Recoverer)

    r.Get("/", serveIndex)
    r.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

    // API routes
    r.Route("/v1/{tenant}/{key}", func(r chi.Router) {
        r.Put("/", handlePut(redisClient, db))
        r.Get("/", handleGet(redisClient))
        r.Delete("/", handleDelete(redisClient, db))
    })

    srv := &http.Server{Addr: ":8080", Handler: r}
    go func() {
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("Server failed: %v", err)
        }
    }()

    stop := make(chan os.Signal, 1)
    signal.Notify(stop, os.Interrupt)
    <-stop
    log.Println("Shutting down...")
    cancel()
    ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    srv.Shutdown(ctx)
    log.Println("Server stopped")
}

func initPostgres(dsn string) *sql.DB {
    db, err := sql.Open("postgres", dsn)
    if err != nil {
        log.Fatalf("Failed to connect to PostgreSQL: %v", err)
    }
    if err := db.Ping(); err != nil {
        log.Fatalf("Failed to ping PostgreSQL: %v", err)
    }
    return db
}