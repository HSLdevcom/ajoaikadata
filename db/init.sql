CREATE TABLE IF NOT EXISTS messages (
    timestamp TIMESTAMPTZ NOT NULL,
    msg_type int NOT NULL,
    vehicle_id int NOT NULL,
    message JSONB NOT NULL,
    PRIMARY KEY (timestamp, msg_type, vehicle_id)
);

SELECT
    create_hypertable('messages', 'timestamp');