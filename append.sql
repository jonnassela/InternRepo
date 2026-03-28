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

-- create or alter procedure append_customers
-- as 
-- begin 
--     begin try 

--         --insert only new records
--         insert into staging.customers_clean (customer_id,name,city,updated)
--         select 
--             l.customer_id,
--             l.name,
--             l.city,
--             l.updated
--         from landing.customers l 
--         left join staging.customers_clean s
--             on l.customer_id = s.customer_id
--         where s.customer_id is null; -- only new rows inserted so no duplicated 

--         --log success 
--         insert into config.audit_log(procedure_name, status,message)
--         values ('append_customers','success','append completed');

--     END TRY;
--     begin catch
--         --log error
--         insert into config.audit_log(procedure_name,status,message)
--         values('append_customers''FAIL', ERROR_MESSAGE());

--     end catch;
-- END;


--commit 3

CREATE OR ALTER PROCEDURE append_customers
AS
BEGIN
    BEGIN TRANSACTION;

    BEGIN TRY

        -- Insert only new records
        INSERT INTO staging.customers_clean (customer_id, name, city, updated)
        SELECT 
            l.customer_id,
            l.name,
            l.city,
            l.updated
        FROM landing.customers l
        LEFT JOIN staging.customers_clean s
            ON l.customer_id = s.customer_id
        WHERE s.customer_id IS NULL;

        DECLARE @inserted_rows INT = @@ROWCOUNT;

        -- Log success
        INSERT INTO config.audit_log (procedure_name, status, message)
        VALUES (
            'append_customers',
            'SUCCESS',
            'Inserted rows: ' + CAST(@inserted_rows AS VARCHAR)
        );

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH

        ROLLBACK TRANSACTION;

        INSERT INTO config.audit_log (procedure_name, status, message)
        VALUES (
            'append_customers',
            'FAIL',
            ERROR_MESSAGE()
        );

    END CATCH
END;