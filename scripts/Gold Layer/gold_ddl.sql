



/*
===================================
xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

BUILDING GOLD LAYER

xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
===================================
*/
-- ====================================================================================
-- Creating (gold.dim_customers) View (Master information = CRM)
-- (Found Mis_matching information in ci.cst_gndr and ca.GEN) (Applying CRM information)
-- ====================================================================================
If Object_id('gold.dim_customers', 'V') is NOT NULL
Drop View gold.dim_customers;
GO
Create or alter VIEW gold.dim_customers AS
	Select
		ROW_NUMBER() over(Order By cst_id) as customer_key, -- This is a surrogate key that is being used as a unique identifier (Surrogate Key)
		ci.cst_id 				as customer_id,
		ci.cst_key 				as customer_number,
		ci.cst_firstname 		as first_name,
		ci.cst_lastname 		as last_name,
		la.CNTRY 				as country,
		ci.cst_marital_status 	as marital_status,
		Case
			When ci.cst_gndr != 'n/a' Then ci.cst_gndr
			Else ca.GEN
		End 					as gender,
		ca.BDATE 				as birthdate,
		ci.cst_create_date 		as create_date
			From silver.crm_cust_info as ci
				Left Join silver.erp_cust_az12 as ca
					ON ci.cst_key = ca.CID
				Left Join silver.erp_loc_a101 as la
					ON ci.cst_key = la.CID;
GO
/*
============================================================
Creating (gold.dim_products) View (Master information = CRM)
============================================================
*/
If OBJECT_ID('gold.dim_products', 'V') is NOT NULL
	Drop view gold.dim_products;
GO
Create or alter View gold.dim_products AS
SELECT
	ROW_NUMBER() Over(Order By pi.prd_start_dt, pi.prd_key) as Product_key, -- This is a surrogate key that is being used as a unique identifier (Surrogate Key)
	pi.prd_id 		as product_id,
	pi.prd_key 		as product_number,
	pi.prd_nm 		as product_name,
	pi.cat_id 		as category_id,
	pcg.CAT 		as category,
	pcg.SUBCAT 		as sub_category,
	pcg.MAINTENANCE as maintenance,
	pi.prd_line 	as product_line,
	pi.prd_cost 	as product_cost,
	pi.prd_start_dt as product_start_date
		From silver.crm_prd_info as pi
			Left Join Silver.erp_px_cat_g1v2 as pcg 
		ON pi.cat_id = pcg.ID
			Where pi.prd_end_dt is null; -- (Task: Filtering out historical data) (Historical data = Entries NOT NULL in prd_end_dt)
GO

/*
============================================================
Creating (gold.fact_sales) View (Master information = CRM)
============================================================
*/
-- Found null values in product_key
If Object_id('gold.fact_sales', 'V') is NOT NULL
	Drop VIEW gold.fact_sales;
GO
Create or alter View gold.fact_sales as
Select
	sd.sls_ord_num 	as order_number,
	dp.Product_key 	as product_key,
	dc.customer_key as customer_key,
	sd.sls_cust_id 	as customer_id,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt 	as shipping_date,
	sd.sls_due_dt 	as due_date,
	sd.sls_sales 	as Sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price 	as price
		From silver.crm_sales_details as sd
			Left Join gold.dim_products as dp
		ON sd.sls_prd_key = dp.product_number
			Left Join gold.dim_customers as dc
		ON sd.sls_cust_id = dc.customer_id -- if we use till here for creating the VIEW gold.fact_sales (There will be NULL entries in gold.dim_customers table, Customer_id Column) Meaning multiple customers_id's that are present in sales table is not present in customer table which should be invalid, since customer table holds accurate customer information. 
			Where dc.customer_id is not null; -- Use till here to fix those errors when joining crm_sales with dim_customers table, By filtering out NULLs when fact_sales customer_key = dim_customers customer_key (We can also fix it when creating the gold.dim_customers view by joining using 
											 -- RIGHT JOIN for getting all the entries of customer_key from silver.crm_sales_deatils with the silver.crm_cust Info and then filter out Null entries of customer Id's from customers_table)
GO	
