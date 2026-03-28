--append logic
--“Insert new data into the target table without modifying existing data.”
create or alter procedure append_customers

as 
begin 

    insert into staging.customers_clean(customer_id,name,city,updated)
    select customer_id,name,city,updated
    from landing.customers;

end;