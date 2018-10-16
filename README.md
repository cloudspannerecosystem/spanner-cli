spanner-cli
===

[![CircleCI](https://circleci.com/gh/yfuruyama/spanner-cli.svg?style=svg)](https://circleci.com/gh/yfuruyama/spanner-cli)

spanner-cli, the Cloud Spanner command line interface.

![gif](https://github.com/yfuruyama/spanner-cli/blob/master/image/spanner_cli.gif)

## Description

`spanner-cli` is a command line client for [Cloud Spanner](https://cloud.google.com/spanner/).  
You can control your Spanner databases with idiomatic SQL commands like `mysql(1)`.

This tool is still **ALPHA** quality.  
Do not use this tool for operating production databases.

## Install

```
go get -u github.com/yfuruyama/spanner-cli
```

## Usage

```
Usage:
  spanner-cli [OPTIONS]

Application Options:
  -p, --project=  GCP Project ID. (default: gcloud config value of "core/project")
  -i, --instance= Cloud Spanner Instance ID. (default: gcloud config value of "spanner/instance")
  -d, --database= Cloud Spanner Database ID.
  -e, --execute=  Execute SQL statement and quit.
  -t, --table     Display output in table format for batch mode.

Help Options:
  -h, --help      Show this help message
```

This tool uses [Application Default Credentials](https://cloud.google.com/docs/authentication/production?hl=en#providing_credentials_to_your_application) as credential source to connect to Spanner databases.  
Please be sure to prepare your credential by `gcloud auth application-default login`.

## Example

### Interactive mode

```
$ spanner-cli -p myproject -i myinstance -d mydb
Connected.
spanner> CREATE TABLE users (
      ->   id INT64 NOT NULL,
      ->   name STRING(16) NOT NULL,
      ->   active BOOL NOT NULL
      -> ) PRIMARY KEY (id);
Query OK, 0 rows affected (30.60 sec)

spanner> SHOW TABLES;
+----------------+
| Tables_in_mydb |
+----------------+
| users          |
+----------------+
1 rows in set (18.66 msecs)

spanner> INSERT INTO users (id, name, active) VALUES (1, "foo", true), (2, "bar", false);
Query OK, 2 rows affected (5.08 sec)

spanner> SELECT * FROM users ORDER BY id ASC;
+----+------+--------+
| id | name | active |
+----+------+--------+
| 1  | foo  | true   |
| 2  | bar  | false  |
+----+------+--------+
2 rows in set (3.09 msecs)

spanner> BEGIN;
Query OK, 0 rows affected (0.02 sec)

spanner(rw txn)> DELETE FROM users WHERE active = false;
Query OK, 1 rows affected (0.61 sec)

spanner(rw txn)> COMMIT;
Query OK, 0 rows affected (0.20 sec)

spanner> SELECT * FROM users ORDER BY id ASC;
+----+------+--------+
| id | name | active |
+----+------+--------+
| 1  | foo  | true   |
+----+------+--------+
1 rows in set (2.58 msecs)

spanner> DROP TABLE users;
Query OK, 0 rows affected (25.20 sec)

spanner> SHOW TABLES;
Empty set (2.02 msecs)

spanner> EXIT;
Bye
```

### Batch mode

By passing SQL from standard input, `spanner-cli` runs in batch mode

```
$ echo 'SELECT * FROM users;' | spanner-cli -p myproject -i myinstance -d mydb
id      name    active
1       foo     true
2       bar     false
```

You can also pass SQL from command line option `-e`

```
$ spanner-cli -p myproject -i myinstance -d mydb -e 'SELECT * FROM users;'
id      name    active
1       foo     true
2       bar     false
```

With `-t` option, results are displayed in table format

```
$ spanner-cli -p myproject -i myinstance -d mydb -e 'SELECT * FROM users;' -t
+----+------+--------+
| id | name | active |
+----+------+--------+
| 1  | foo  | true   |
| 2  | bar  | false  |
+----+------+--------+
```

## Syntax

The syntax is case-insensitive.  
`\G` delimiter is also supported for dispalying results vertically.

| Usage | Syntax | Note |
| --- | --- | --- |
| List databases | `SHOW DATABASES;` | |
| Switch database | `USE <database>;` | |
| Create database | `CREATE DATABSE <database>;` | |
| Drop database |  | Not supported yet |
| List tables | `SHOW TABLES;` | |
| Show table schema | `SHOW CREATE TABLE <table>;` | |
| Create table | `CREATE TABLE ...;` | |
| Change table schema | `ALTER TABLE ...;` | |
| Delete table | `DROP TABLE ...;` | |
| Create index | `CREATE INDEX ...;` | |
| Delete index | `DROP INDEX ...;` | |
| Query | `SELECT ...;` | |
| Stale Read | | Not supported yet |
| DML | `INSERT / UPDATE / DELETE ...;` | |
| Partitioned DML | | Not supported yet |
| Start Read-Write Transaction | `BEGIN (RW);` | |
| Commit Read-Write Transaction | `COMMIT;` | |
| Rollback Read-Write Transaction | `ROLLBACK;` | |
| Start Read-Only Transaction | `BEGIN RO;` | |
| End Read-Only Transaction | `CLOSE;` | |
| Show Query Execution Plan | | Not supported yet |
| Exit | `EXIT;` | |

## How to develop

Run unit tests

```
$ make test
```

Run unit tests and integration tests, which connects to real Cloud Spanner database.

```
$ PROJECT=${PROJECT_ID} INSTANCE=${INSTANCE_ID} DATABASE=${DATABASE_ID} CREDENTIAL=${CREDENTIAL} make test
```

## TODO

* STRUCT data type
* prompt customize
* EXPLAIN
* DESCRIBE
* show secondary index by "SHOW CREATE TABLE"
