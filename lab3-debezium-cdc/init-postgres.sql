CREATE TABLE IF NOT EXISTS public.orders (
    order_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    order_status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- เปิด REPLICA IDENTITY FULL เพื่อให้ Debezium ส่งค่าทั้ง before และ after ตอนเกิด UPDATE / DELETE
ALTER TABLE public.orders REPLICA IDENTITY FULL;

-- DDIA Chapter 7 & 11: Transactional Outbox Table (สำหรับบันทึก Domain Events ใน Local Transaction เดียวกับตารางธุรกิจ)
CREATE TABLE IF NOT EXISTS public.outbox_events (
    event_id SERIAL PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL DEFAULT 'ORDER',
    aggregate_id INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE public.outbox_events REPLICA IDENTITY FULL;

INSERT INTO public.orders (order_id, customer_name, product_name, category, amount, order_status) VALUES
(101, 'Somchai K.', 'MacBook Pro M3', 'Electronics', 59900.00, 'PAID'),
(102, 'Suda W.', 'Mechanical Keyboard', 'Accessories', 3490.00, 'PENDING'),
(103, 'Ananda P.', '4K Monitor 27"', 'Electronics', 12500.00, 'SHIPPED'),
(104, 'Nattapong T.', 'Ergonomic Chair', 'Furniture', 8900.00, 'PAID'),
(105, 'Kanya S.', 'USB-C Hub Pro', 'Accessories', 1590.00, 'PENDING');

SELECT setval('public.orders_order_id_seq', 105, true);

