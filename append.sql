--append logic
--“Insert new data into the target table without modifying existing data.”
-- create or alter procedure append_customers

-- as 
-- begin 

--     insert into staging.customers_clean(customer_id,name,city,updated)
--     select customer_id,name,city,updated
--     from landing.customers;

-- end;

--version 2 for commit 2
--fixing the problem with duplicates 

create or alter procedure append_customers
as 
begin 
    begin try 

        --insert only new records
        insert into staging.customers_clean (customer_id,name,city,updated)
        select 
            l.customer_id,
            l.name,
            l.city,
            l.updated
        from landing.customers l 
        left join staging.customers_clean s
            on l.customer_id = s.customer_id
        where s.customer_id is null; -- only new rows inserted so no duplicated 

        --log success 
        insert into config.audit_log(procedure_name, status,message)
        values ('append_customers','success','append completed');

    END TRY;
    begin catch
        --log error
        insert into config.audit_log(procedure_name,status,message)
        values('append_customers''FAIL', ERROR_MESSAGE());

    end catch;
END;