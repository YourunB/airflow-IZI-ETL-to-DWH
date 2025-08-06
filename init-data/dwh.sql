CREATE TABLE customers_with_payments (
  id SERIAL PRIMARY KEY,
  customer_id INT,
  phone NUMERIC,
  amount NUMERIC
);
