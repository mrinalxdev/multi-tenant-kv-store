CREATE TABLE kv (
    tenant TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (tenant, key)
);