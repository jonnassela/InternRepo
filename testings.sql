USE InternDB;
GO

-- SAMPLE DATA
INSERT INTO landing.customers VALUES (1, 'John', 'NY', GETDATE());
INSERT INTO landing.customers VALUES (2, 'Anna', 'LA', GETDATE());
INSERT INTO landing.customers VALUES (3, 'Mike', 'Chicago', GETDATE());

-- TEST FULL LOAD
EXEC full_load_customers;
SELECT * FROM staging.customers_clean;

-- TEST UPSERT
UPDATE landing.customers SET city = 'Strug' WHERE customer_id = 1;
INSERT INTO landing.customers VALUES (4, 'Jona', 'London', GETDATE());

EXEC upsert_customers;
SELECT * FROM staging.customers_clean;

-- TEST APPEND
TRUNCATE TABLE staging.customers_clean;
EXEC append_customers;

-- TEST SYNC
EXEC sync_customers;

-- TEST INCREMENTAL
EXEC incremental_load_customers;

-- CHECK LOGS
SELECT * FROM config.audit_log;