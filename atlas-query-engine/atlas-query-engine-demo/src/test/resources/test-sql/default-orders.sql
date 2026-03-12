CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    country VARCHAR(10) NOT NULL
);

INSERT INTO orders (id, country) VALUES (1, 'BR');
