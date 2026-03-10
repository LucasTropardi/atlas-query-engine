CREATE TABLE IF NOT EXISTS sample_records (
    id BIGINT PRIMARY KEY,
    label VARCHAR(100) NOT NULL
);

INSERT INTO sample_records (id, label) VALUES
    (1, 'atlas mysql lab')
ON DUPLICATE KEY UPDATE label = VALUES(label);
