CREATE VIEW dbo.vw_monthly_sales AS
SELECT d.year_month, r.region, p.category,
       SUM(f.sales) AS sales, SUM(f.profit) AS profit, SUM(f.qty_ordered) AS units
FROM dbo.fact_sales f
JOIN dbo.dim_date d    ON f.order_date_key = d.date_key
JOIN dbo.dim_region r  ON f.region_key = r.region_key
JOIN dbo.dim_product p ON f.product_key = p.product_key
GROUP BY d.year_month, r.region, p.category;