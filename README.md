spanner-cli
===

spanner-cli, the Cloud Spanner command line interface.

## Description

`spanner-cli` is a command line client for [Cloud Spanner](https://cloud.google.com/spanner/).  
You can control your Spanner databases with idiomatic SQL commands like `mysql(1)`.

## Disclaimer
This tool is still **ALPHA** quality.  
Do not use this tool for controlling production databases.

## Install

```
go get -u github.com/yfuruyama/spanner-cli
```

## Syntax

All syntax is case-insensitive.

| Usage | Syntax | Note |
| --- | --- | --- |
| List databases | `SHOW DATABASES;` | |
| Select database | `USE <database>;` | |
| Create database | `CREATE DATABSE <database>;` | |
| Drop database |  | Not implemented now |
| List tables | `SHOW TABLES;` | |
| Show table schema | `SHOW CREATE TABLE <table>;` | |
| Create table | `CREATE TABLE ...;` | |
| Change table schema | `ALTER TABLE ...;` | |
| Delete table | `DROP TABLE ...;` | |
| Create index | `CREATE INDEX ...;` | |
| Delete index | `DROP INDEX ...;` | |
| Query | `SELECT ...;` | |
| DML | `INSERT / UPDATE / DELETE ...;` | |
| Partitioned DML | | Not supported yet |
| Start Read-Write Transaction | `BEGIN (RW);` | |
| Commit Read-Write Transaction | `COMMIT;` | |
| Rollback Read-Write Transaction | `ROLLBACK;` | |
| Start Read-Only Transaction | `BEGIN RO;` | |
| End Read-Only Transaction | `CLOSE;` | |
| Show Query Execution Plan | | Not supported yet |

## Example

TODO

```
spanner-cli --project=<PROJECT> --instance=<INSTANCE> --database=<DATABASE>
```

## TODO

* STRUCT data type
* comment literal (#)
* prompt customize
* vertical format for query result (\G)
* EXPLAIN
* DESCRIBE
* evaluate SQL from stdin
