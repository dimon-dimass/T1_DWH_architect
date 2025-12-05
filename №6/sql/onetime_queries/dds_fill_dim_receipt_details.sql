insert into schema20.dds_receipt_details
select sales_tran_id 
	,item_id 
	,avg(unit_selling_price_amt) unit_selling_price_amt
	,avg(unit_cost_amt) unit_cost_amt
	,sum(item_qty) item_qty
from schema20.stg_sales_transaction_line
where tran_line_date < '2023-12-01'
group by sales_tran_id, item_id;
