CREATE TABLE IF NOT EXISTS messages (
    tst TIMESTAMPTZ NOT NULL,
    ntp_timestamp TIMESTAMPTZ NOT NULL,
    eke_timestamp TIMESTAMP NOT NULL,
    mqtt_timestamp TIMESTAMPTZ NOT NULL,
    tst_source TEXT NOT NULL,
    msg_type INT NOT NULL,
    vehicle_id INT NOT NULL,
    message JSONB NOT NULL,
    PRIMARY KEY (tst, msg_type, vehicle_id)
);

CREATE TABLE IF NOT EXISTS events (
    tst TIMESTAMPTZ NOT NULL,
    ntp_timestamp TIMESTAMPTZ NOT NULL,
    eke_timestamp TIMESTAMP NOT NULL,
    mqtt_timestamp TIMESTAMPTZ NOT NULL,
    tst_source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    vehicle_id INT NOT NULL,
    data JSONB NOT NULL,
    PRIMARY KEY (tst, event_type, vehicle_id)
);

CREATE TABLE IF NOT EXISTS stationevents (
    tst TIMESTAMPTZ NOT NULL,
    ntp_timestamp TIMESTAMPTZ NOT NULL,
    eke_timestamp TIMESTAMP NOT NULL,
    tst_source TEXT NOT NULL,
    vehicle_id INT NOT NULL,
    station TEXT NOT NULL,
    track TEXT NOT NULL,
    direction TEXT NOT NULL,
    data JSONB NOT NULL,
    PRIMARY KEY (tst, vehicle_id)
);

-- Data will be copied to staging, and then inserted into main tables.
CREATE SCHEMA staging;

SELECT
    create_hypertable('messages', 'tst');

SELECT
    create_hypertable('events', 'tst');

SELECT
    create_hypertable('stationevents', 'tst');