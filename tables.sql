USE InternDB;
GO

-- SCHEMAS
CREATE SCHEMA landing;
GO
CREATE SCHEMA staging;
GO
CREATE SCHEMA config;
GO


CREATE TABLE landing.customers (
    customer_id INT,
    name VARCHAR(100),
    city VARCHAR(100),
    updated DATETIME
);
GO

CREATE TABLE staging.customers_clean (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    city VARCHAR(100),
    updated DATETIME
);
GO

CREATE TABLE config.load_config (
    last_load_time DATETIME
);
GO

DROP TABLE IF EXISTS config.load_config;
GO

CREATE TABLE config.load_config (
    id              INT IDENTITY(1,1) PRIMARY KEY,  -- unique row per load
    last_load_time  DATETIME,                        -- previous load (for the WHERE filter)
    current_load_time DATETIME                       -- this load's timestamp
);
GO

--INSERT INTO config.load_config VALUES ('2000-01-01');
--GO

CREATE TABLE config.audit_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    procedure_name VARCHAR(100),
    status VARCHAR(20),
    message VARCHAR(255),
    log_time DATETIME DEFAULT GETDATE()
);
GO