insert into schema20.dds_receipts(receipt_id, customer_id, employee_id, department_id, tran_type_cd, tran_status_cd, purchase_cost, purchase_qty
								,uniq_prods_qty, purchase_rev, tran_start_dttm, tran_end_dttm, purchase_date)
select sales_tran_id 
	,individual_party_id 
	,associate_party_id 
	,location_id 
	,tran_type_cd 
	,tran_status_cd 
	,mkb_cost_amt
	,mkb_item_qty
	,mkb_number_unique_items_qty
	,mkb_rev_amt
	,tran_start_dttm_dd 
	,tran_end_dttm_dd
	,tran_date 
from schema20.stg_sales_transaction
where tran_date < '2023-12-01';
