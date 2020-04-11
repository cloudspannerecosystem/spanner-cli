spanner-cli [![CircleCI](https://circleci.com/gh/cloudspannerecosystem/spanner-cli.svg?style=svg)](https://circleci.com/gh/cloudspannerecosystem/spanner-cli)
===

Interactive command line tool for Cloud Spanner.

![gif](https://github.com/cloudspannerecosystem/spanner-cli/blob/master/image/spanner_cli.gif)

## Description

`spanner-cli` is an interactive command line tool for [Google Cloud Spanner](https://cloud.google.com/spanner/).  
You can control your Spanner databases with idiomatic SQL commands.

## Install

If you have `go`, you can install using `go get`.

```
go get -u github.com/cloudspannerecosystem/spanner-cli
```

Otherwise, you can download the binary from the [releases page](https://github.com/cloudspannerecosystem/spanner-cli/releases).

## Usage

```
Usage:
  spanner-cli [OPTIONS]

spanner:
  -p, --project=    (required) GCP Project ID.
  -i, --instance=   (required) Cloud Spanner Instance ID
  -d, --database=   (required) Cloud Spanner Database ID.
  -e, --execute=    Execute SQL statement and quit.
  -f, --file=       Execute SQL statement from file and quit.
  -t, --table       Display output in table format for batch mode.
      --credential= Use the specific credential file
      --prompt=     Set the prompt to the specified format

Help Options:
  -h, --help        Show this help message
```

Unless you specify a credential file with `--credential`, this tool uses [Application Default Credentials](https://cloud.google.com/docs/authentication/production?hl=en#providing_credentials_to_your_application) as credential source to connect to Spanner databases.  
Please make sure to prepare your credential by `gcloud auth application-default login`.

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

By passing SQL from standard input, `spanner-cli` runs in batch mode.

```
$ echo 'SELECT * FROM users;' | spanner-cli -p myproject -i myinstance -d mydb
id      name    active
1       foo     true
2       bar     false
```

You can also pass SQL with command line option `-e`.

```
$ spanner-cli -p myproject -i myinstance -d mydb -e 'SELECT * FROM users;'
id      name    active
1       foo     true
2       bar     false
```

With `-t` option, results are displayed in table format.

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
| Drop database | `DROP DATABASE <database>;` | |
| List tables | `SHOW TABLES;` | |
| Show table schema | `SHOW CREATE TABLE <table>;` | |
| Show columns | `SHOW COLUMNS FROM <table>;` | |
| Show indexes | `SHOW INDEX FROM <table>;` | |
| Create table | `CREATE TABLE ...;` | |
| Change table schema | `ALTER TABLE ...;` | |
| Delete table | `DROP TABLE ...;` | |
| Create index | `CREATE INDEX ...;` | |
| Delete index | `DROP INDEX ...;` | |
| Query | `SELECT ...;` | |
| DML | `INSERT / UPDATE / DELETE ...;` | |
| Partitioned DML | | Not supported yet |
| Show Query Execution Plan | `EXPLAIN SELECT ...;` | |
| Start Read-Write Transaction | `BEGIN (RW);` | |
| Commit Read-Write Transaction | `COMMIT;` | |
| Rollback Read-Write Transaction | `ROLLBACK;` | |
| Start Read-Only Transaction | `BEGIN RO;` | |
| Start Read-Only Transaction (Exact Staleness) | `BEGIN RO <seconds>;` | |
| Start Read-Only Transaction (Read Timestamp) | `BEGIN RO <RFC3339-formatted time>;` | See [RFC3339](https://tools.ietf.org/html/rfc3339) (ISO 8601) |
| End Read-Only Transaction | `CLOSE;` | |
| Exit CLI | `EXIT;` | |

## Customize prompt

You can customize the prompt by `--prompt` option.  
There are some defined variables for being used in prompt.

Variables:

* `\p` : GCP Project ID
* `\i` : Cloud Spanner Instance ID
* `\d` : Cloud Spanner Database ID
* `\t` : In transaction

Example:

```
$ spanner-cli -p myproject -i myinstance -d mydb --prompt='[\p:\i:\d]\t> '
Connected.
[myproject:myinstance:mydb]> SELECT * FROM users ORDER BY id ASC;
+----+------+--------+
| id | name | active |
+----+------+--------+
| 1  | foo  | true   |
| 2  | bar  | false  |
+----+------+--------+
2 rows in set (3.09 msecs)

[myproject:myinstance:mydb]> begin;
Query OK, 0 rows affected (0.08 sec)

[myproject:myinstance:mydb](rw txn)> ...
```

The default prompt is `spanner\t> `.

## Config file

This tool supports a configuration file called `spanner_cli.cnf`, similar to `my.cnf`.  
The config file path must be `~/.spanner_cli.cnf`.  
In the config file, you can set default option values for command line options.

Example:

```conf
[spanner]
project = myproject
instance = myinstance
prompt = "[\\p:\\i:\\d]\\t> "
```

## How to develop

Run unit tests.

```
$ make test
```

Run integration tests, which connects to real Cloud Spanner database.

```
$ PROJECT=${PROJECT_ID} INSTANCE=${INSTANCE_ID} DATABASE=${DATABASE_ID} CREDENTIAL=${CREDENTIAL} make test
```

## TODO

* Show secondary index by "SHOW CREATE TABLE"

## Disclaimer

Do not use this tool for production databases as the tool is still alpha quality.

This tool is not an official Google product.
