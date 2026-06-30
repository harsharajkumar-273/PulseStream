-- PostgreSQL Schema Definition for PulseStream

-- Enable UUID extension if not enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Clients Table (For API Key Authentication)
CREATE TABLE IF NOT EXISTS clients (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    api_key VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Events Table (For Ingested Metrics)
-- The primary key 'id' represents the unique Idempotency-Key sent by the client.
-- This enforces database-level idempotency natively.
CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,
    device_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    timestamp BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for querying metrics by device and type
CREATE INDEX IF NOT EXISTS idx_events_device_type ON events (device_id, event_type);
-- Index for querying metrics by timestamp (useful for timeseries aggregation)
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp DESC);

-- Seed a default test client (we can use this key for development)
INSERT INTO clients (api_key, name, active)
VALUES ('ps_live_test_key_abc123xyz', 'Developer Test IoT Device', true)
ON CONFLICT (api_key) DO NOTHING;
