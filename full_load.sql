-- full load logic// Delete everything → Insert everything again
-- Full load replaces all data in the target table with fresh data from the source to ensure consistency.

-- create procedure full_load_customers
-- as 
-- begin

--     --remove old data
--     delete from staging.customers_clean;

--     --load fresh data
--     insert into staging.customers_clean
--     select *
--     from landing.customers;

-- end;

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