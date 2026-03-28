--incremental load logic
--load the only the data that has changed since the last load

GO
CREATE OR ALTER PROCEDURE incremental_load_customers
AS
BEGIN
    BEGIN TRY

        DECLARE @last_load DATETIME;

        SELECT @last_load = last_load_time 
        FROM config.load_config;

        -- UPDATE changed rows
        UPDATE s
        SET 
            s.name = l.name,
            s.city = l.city,
            s.updated = l.updated
        FROM staging.customers_clean s
        JOIN landing.customers l
            ON s.customer_id = l.customer_id
        WHERE l.updated > @last_load;

        -- INSERT new rows
        INSERT INTO staging.customers_clean (customer_id, name, city, updated)
        SELECT 
            l.customer_id,
            l.name,
            l.city,
            l.updated
        FROM landing.customers l
        LEFT JOIN staging.customers_clean s
            ON l.customer_id = s.customer_id
        WHERE s.customer_id IS NULL
        AND l.updated > @last_load; -- this is the line that indicates that its incremental load without this it becomes full load

        -- update config
        UPDATE config.load_config
        SET last_load_time = GETDATE();

        -- audit
        INSERT INTO config.audit_log (procedure_name, status, message)
        VALUES ('incremental_load_customers', 'SUCCESS', 'Incremental load completed');

    END TRY
    BEGIN CATCH
        INSERT INTO config.audit_log (procedure_name, status, message)
        VALUES ('incremental_load_customers', 'FAIL', ERROR_MESSAGE());
    END CATCH
END;
GO