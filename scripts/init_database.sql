/*
    Creating the databases according to the medallion
    architecture.
    If the database already exists it will be dropped
    and recreated.
*/

DROP DATABASE IF EXISTS bronze;
CREATE DATABASE bronze;

DROP DATABASE IF EXISTS silver;
CREATE DATABASE silver;

DROP DATABASE IF EXISTS gold;
CREATE DATABASE gold;
