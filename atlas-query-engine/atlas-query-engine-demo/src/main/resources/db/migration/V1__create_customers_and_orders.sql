CREATE TABLE public.customers (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE public.orders (
    id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    country VARCHAR(2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at DATE NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    CONSTRAINT fk_orders_customers
        FOREIGN KEY (customer_id) REFERENCES public.customers (id)
);

INSERT INTO public.customers (id, name) VALUES
    (1, 'Lucas Costa'),
    (2, 'Ana Silva'),
    (3, 'John Miller'),
    (4, 'Maria Souza')
ON CONFLICT (id) DO NOTHING;

INSERT INTO public.orders (id, customer_id, country, status, created_at, amount) VALUES
    (1, 1, 'BR', 'PAID', '2026-01-02', 120.50),
    (2, 1, 'BR', 'PAID', '2026-01-08', 250.00),
    (3, 2, 'BR', 'PENDING', '2026-01-10', 90.00),
    (4, 3, 'US', 'PAID', '2026-01-03', 310.75),
    (5, 3, 'US', 'PAID', '2026-01-12', 450.25),
    (6, 3, 'US', 'CANCELLED', '2026-01-15', 75.00),
    (7, 4, 'AR', 'PAID', '2026-01-05', 180.00),
    (8, 4, 'AR', 'PENDING', '2026-01-17', 60.00),
    (9, 2, 'CL', 'PAID', '2026-01-18', 220.10),
    (10, 1, 'CL', 'PAID', '2026-01-20', 330.40)
ON CONFLICT (id) DO NOTHING;
