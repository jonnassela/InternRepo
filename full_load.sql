-- full load logic// Delete everything → Insert everything again
-- Full load replaces all data in the target table with fresh data from the source to ensure consistency.

create procedure full_load_customers
as 
begin

    --remove old data
    delete from staging.customers_clean;

    --load fresh data
    insert into staging.customers_clean
    select *
    from landing.customers;

end;