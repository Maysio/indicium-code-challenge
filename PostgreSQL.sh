CREATE TABLE order_details (
    order_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL
);

CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    order_date DATE
);
