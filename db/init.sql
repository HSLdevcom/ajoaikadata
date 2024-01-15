CREATE TABLE IF NOT EXISTS messages (
    timestamp TIMESTAMPTZ NOT NULL,
    msg_type INT NOT NULL,
    vehicle_id INT NOT NULL,
    message JSONB NOT NULL,
    PRIMARY KEY (timestamp, msg_type, vehicle_id)
);

CREATE TABLE IF NOT EXISTS events (
    timestamp TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    vehicle_id INT NOT NULL,
    state JSONB NOT NULL,
    PRIMARY KEY (timestamp, event_type, vehicle_id)
);

-- Data will be copied to staging, and then inserted into main tables.
CREATE SCHEMA staging;

SELECT
    create_hypertable('messages', 'timestamp');

SELECT
    create_hypertable('events', 'timestamp');