-- full load logic// Delete everything → Insert everything again
-- Full load replaces all data in the target table with fresh data from the source to ensure consistency.


--hardcoded version


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

GO
CREATE PROCEDURE full_load_customers
AS
BEGIN

    BEGIN TRY

        DELETE FROM staging.customers_clean;

        INSERT INTO staging.customers_clean
        SELECT *
        FROM landing.customers;

        INSERT INTO config.audit_log
        (procedure_name, status, message)
        VALUES ('full_load_customers', 'SUCCESS', 'Full load completed');

    END TRY

    BEGIN CATCH

        INSERT INTO config.audit_log
        (procedure_name, status, message)
        VALUES ('full_load_customers', 'ERROR', ERROR_MESSAGE());

    END CATCH

END;

GO
CREATE OR ALTER PROCEDURE full_load_customers -- no need to drop procedure just overwrite it
AS
BEGIN
    BEGIN TRANSACTION; -- all or nothing logic, groups multiple operations in one unit

    BEGIN TRY -- error handling
        -- Step 1: Clear staging table
        DELETE FROM staging.customers_clean; -- ensures clean state

        -- Step 2: Load fresh data
        INSERT INTO staging.customers_clean (customer_id, name, city, updated)
        SELECT customer_id, name, city, updated
        FROM landing.customers;

        -- Step 3: Log success
        INSERT INTO config.audit_log (procedure_name, status, message) -- logs success
        VALUES ('full_load_customers', 'SUCCESS', 
                'Full load completed. Rows inserted: ' + CAST(@@ROWCOUNT AS VARCHAR)); -- nr of rows affected

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH -- if error happens jump to catch
        ROLLBACK TRANSACTION;

        INSERT INTO config.audit_log (procedure_name, status, message) -- logs failure 
        VALUES ('full_load_customers', 'FAIL', ERROR_MESSAGE());
    END CATCH
END;