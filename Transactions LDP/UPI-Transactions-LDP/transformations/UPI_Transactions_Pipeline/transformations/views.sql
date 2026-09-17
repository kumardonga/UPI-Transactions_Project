create or replace view upi_stream_silver_view as
select * from UPI_transactions_silver where status ='PENDING' or status ='SUCCESS'