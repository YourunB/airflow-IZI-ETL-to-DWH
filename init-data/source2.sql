CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  customer_id INT,
  phone NUMERIC
);

INSERT INTO customers (customer_id, phone)
VALUES (1, 37529699), (2, 37529333);
