CREATE TABLE IF NOT EXISTS customers_with_payments (
  -- billing.v_payment
  payment_id INT,
  id_payment_type INT,
  payment_type_name TEXT,
  grp_payment_type TEXT,
  id_club INT,
  id_user INT,
  id_type_balance INT,
  name_balance TEXT,
  amount NUMERIC,
  id_reservation_terminal INT,
  id_subscription_user INT,
  id_reservation_shop INT,
  id_user_init INT,
  id_parent INT,
  payment_date TIMESTAMPTZ,
  payment_comment TEXT,

  -- public.sales
  sale_id INT,
  sale_user_id INT,
  sale_user_master_id INT,
  sale_user_slave_id INT,
  sale_date TIMESTAMPTZ,
  sale_comment TEXT,
  sale_club_id INT,
  sale_transmitted BOOLEAN,
  sale_state_id INT,
  sale_payment_id INT
);
