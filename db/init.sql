CREATE TABLE IF NOT EXISTS messages (
    ntp_timestamp TIMESTAMPTZ NOT NULL,
    mqtt_timestamp TIMESTAMPTZ NOT NULL,
    msg_type INT NOT NULL,
    vehicle_id INT NOT NULL,
    message JSONB NOT NULL,
    PRIMARY KEY (ntp_timestamp, msg_type, vehicle_id)
);

CREATE TABLE IF NOT EXISTS events (
    ntp_timestamp TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    vehicle_id INT NOT NULL,
    data JSONB NOT NULL,
    PRIMARY KEY (ntp_timestamp, event_type, vehicle_id)
);

-- Data will be copied to staging, and then inserted into main tables.
CREATE SCHEMA staging;

SELECT
    create_hypertable('messages', 'ntp_timestamp');

SELECT
    create_hypertable('events', 'ntp_timestamp');