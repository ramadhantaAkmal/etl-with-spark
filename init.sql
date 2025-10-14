CREATE TABLE fact_sales (
    transaction_id   BIGINT PRIMARY KEY,
    product_id       INT NOT NULL,
    user_id          INT NOT NULL,
    quantity         INT NOT NULL,
    transaction_date DATE NOT NULL,
    email            VARCHAR(100),
    join_date        DATE,
    name             VARCHAR(50),
    product_name     VARCHAR(50),
    category         VARCHAR(30),
    price            BIGINT NOT NULL,
    currency         VARCHAR(5)
);
