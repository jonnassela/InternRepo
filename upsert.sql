--upser logic

create or alter procedure upsert_customers
as
begin
    --update existing rows
    -- if rows exist in both and id matches, update them in staging
    update s
    set 
        s.name=l.name,
        s.city=l.city,
        s.updated=l.updated
    from staging.customers_clean s
    join landing.customers l on s.customer_id=l.customer_id;


    -- inserting new rows
    -- if rows in staging dont exist, insert them from landing
    insert into staging.customers_clean(customer_id,name,city,updated)
    select 
        l.customer_id,
        l.name,
        l.city,
        l.updated
    from landing.customers l left join staging.customers_clean s on l.customer_id=s.customer_id
    where s.customer_id is null;

end;