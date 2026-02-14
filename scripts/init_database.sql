/*
    Creating the databases according to the medallion
    architecture.
    If the database already exists it will be dropped
    and recreated.
*/

IF EXISTS (SELECT * FROM sys.databases WHERE name = 'bronze')
BEGIN
    ALTER DATABASE bronze SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE bronze;
END;

CREATE DATABASE bronze;

IF EXISTS (SELECT * FROM sys.databases WHERE name = 'silver')
BEGIN
    ALTER DATABASE silver SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE silver;
END;

CREATE DATABASE silver;

IF EXISTS (SELECT * FROM sys.databases WHERE name = 'gold')
BEGIN
    ALTER DATABASE gold SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE gold;
END;

CREATE DATABASE gold;
