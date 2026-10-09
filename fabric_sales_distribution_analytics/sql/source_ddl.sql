CREATE SCHEMA sales;
GO
CREATE TABLE sales.orders (
  order_id varchar(25) NOT NULL PRIMARY KEY, order_date date, ship_mode varchar(20),
  customer_id varchar(20), country varchar(50), city varchar(60), state varchar(50),
  postal_code varchar(10), region varchar(20), modified_at datetime2 DEFAULT sysutcdatetime());
CREATE TABLE sales.order_lines (
  row_id int NOT NULL PRIMARY KEY, order_id varchar(25), product_id varchar(25),
  quantity int, sales decimal(12,4), discount decimal(5,2), profit decimal(12,4),
  modified_at datetime2 DEFAULT sysutcdatetime());
CREATE TABLE sales.shipments (
  shipment_id varchar(20) NOT NULL PRIMARY KEY, order_line_id int, order_id varchar(25),
  product_id varchar(25), qty_ordered int, ship_date date, ship_mode varchar(20),
  qty_shipped int, modified_at datetime2 DEFAULT sysutcdatetime());