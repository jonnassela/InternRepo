--incremental load logic
--load the only the data that has changed since the last load


--hardcoded
GO
CREATE OR ALTER PROCEDURE incremental_load_customers
AS
BEGIN

    -- Step 1: Update only recently changed rows (HARDCODED date)
    UPDATE s
    SET 
        s.name = l.name,
        s.city = l.city,
        s.updated = l.updated
    FROM staging.customers_clean s
    JOIN landing.customers l
        ON s.customer_id = l.customer_id
    WHERE l.updated > '2024-01-01';  -- hardcoded timestamp

    -- Step 2: Insert only new + recent rows
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
    AND l.updated > '2024-01-01';  -- same hardcoded filter

END;
GO

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



--commit 2 
GO
CREATE OR ALTER PROCEDURE incremental_load_customers
AS
BEGIN
    BEGIN TRANSACTION;

    BEGIN TRY

        DECLARE @last_load DATETIME;
        DECLARE @updated_rows INT = 0;
        DECLARE @inserted_rows INT = 0;

        -- Step 1: Get last load time
        SELECT @last_load = last_load_time 
        FROM config.load_config;

        -- Step 2: Update changed rows
        UPDATE s
        SET 
            s.name = l.name,
            s.city = l.city,
            s.updated = l.updated
        FROM staging.customers_clean s
        JOIN landing.customers l
            ON s.customer_id = l.customer_id
        WHERE l.updated > @last_load;

        SET @updated_rows = @@ROWCOUNT;

        -- Step 3: Insert new rows
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
        AND l.updated > @last_load;

        SET @inserted_rows = @@ROWCOUNT;

        -- Step 4: Update config table
        UPDATE config.load_config
        SET last_load_time = GETDATE();

        -- Step 5: Audit log with details
        INSERT INTO config.audit_log (procedure_name, status, message)
        VALUES (
            'incremental_load_customers',
            'SUCCESS',
            'Updated: ' + CAST(@updated_rows AS VARCHAR) +
            ', Inserted: ' + CAST(@inserted_rows AS VARCHAR)
        );

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH

        ROLLBACK TRANSACTION;

        INSERT INTO config.audit_log (procedure_name, status, message)
        VALUES (
            'incremental_load_customers',
            'FAIL',
            ERROR_MESSAGE()
        );

    END CATCH
END;
GO