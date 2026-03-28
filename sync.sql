-- -- insert, update, delete logic

CREATE OR ALTER PROCEDURE sync_customers
AS
BEGIN

    -- Step 1: Update existing rows
    UPDATE s
    SET 
        s.name = l.name,
        s.city = l.city,
        s.updated = l.updated
    FROM staging.customers_clean s
    JOIN landing.customers l
        ON s.customer_id = l.customer_id;

    -- Step 2: Insert new rows
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

    -- Step 3: Delete rows not in landing
    DELETE s
    FROM staging.customers_clean s
    LEFT JOIN landing.customers l
        ON s.customer_id = l.customer_id
    WHERE l.customer_id IS NULL;

END;

--commit 2 error handling 
GO
CREATE OR ALTER PROCEDURE sync_customers
AS
BEGIN
    BEGIN TRY

        -- Step 1: Update existing rows
        UPDATE s
        SET 
            s.name = l.name,
            s.city = l.city,
            s.updated = l.updated
        FROM staging.customers_clean s
        JOIN landing.customers l
            ON s.customer_id = l.customer_id;

        -- Step 2: Insert new rows
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

        -- Step 3: Delete rows not in landing
        DELETE s
        FROM staging.customers_clean s
        LEFT JOIN landing.customers l
            ON s.customer_id = l.customer_id
        WHERE l.customer_id IS NULL;

        -- Step 4: Audit success
        INSERT INTO config.audit_log (procedure_name, status, message)
        VALUES ('sync_customers', 'SUCCESS', 'Sync completed (update + insert + delete)');

    END TRY
    BEGIN CATCH

        -- Log error
        INSERT INTO config.audit_log (procedure_name, status, message)
        VALUES ('sync_customers', 'FAIL', ERROR_MESSAGE());

    END CATCH
END;
GO


--commit3

GO
CREATE OR ALTER PROCEDURE sync_customers
AS
BEGIN
    BEGIN TRANSACTION;

    BEGIN TRY

        -- Step 1: Update
        UPDATE s
        SET 
            s.name = l.name,
            s.city = l.city,
            s.updated = l.updated
        FROM staging.customers_clean s
        JOIN landing.customers l
            ON s.customer_id = l.customer_id;

        DECLARE @updated_rows INT = @@ROWCOUNT;

        -- Step 2: Insert
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

        -- Step 3: Delete
        DELETE s
        FROM staging.customers_clean s
        LEFT JOIN landing.customers l
            ON s.customer_id = l.customer_id
        WHERE l.customer_id IS NULL;

        DECLARE @deleted_rows INT = @@ROWCOUNT;

        -- Step 4: Log success
        INSERT INTO config.audit_log (procedure_name, status, message)
        VALUES (
            'sync_customers',
            'SUCCESS',
            'Updated: ' + CAST(@updated_rows AS VARCHAR) +
            ', Inserted: ' + CAST(@inserted_rows AS VARCHAR) +
            ', Deleted: ' + CAST(@deleted_rows AS VARCHAR)
        );

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH

        ROLLBACK TRANSACTION;

        INSERT INTO config.audit_log (procedure_name, status, message)
        VALUES (
            'sync_customers',
            'FAIL',
            ERROR_MESSAGE()
        );

    END CATCH
END;
GO