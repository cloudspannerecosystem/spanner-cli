spanner-cli [![CircleCI](https://circleci.com/gh/cloudspannerecosystem/spanner-cli.svg?style=svg)](https://circleci.com/gh/cloudspannerecosystem/spanner-cli)
===

Interactive command line tool for Cloud Spanner.

![gif](https://github.com/cloudspannerecosystem/spanner-cli/blob/master/image/spanner_cli.gif)

## Description

`spanner-cli` is an interactive command line tool for [Google Cloud Spanner](https://cloud.google.com/spanner/).  
You can control your Spanner databases with idiomatic SQL commands.

## Install

If you have `go`, you can install using `go install`.

```
go install github.com/cloudspannerecosystem/spanner-cli@latest
```

Otherwise, you can download the binary from the [releases page](https://github.com/cloudspannerecosystem/spanner-cli/releases).

## Usage

```
Usage:
  spanner-cli [OPTIONS]

spanner:
  -p, --project=    (required) GCP Project ID. [$SPANNER_PROJECT_ID]
  -i, --instance=   (required) Cloud Spanner Instance ID [$SPANNER_INSTANCE_ID]
  -d, --database=   (required) Cloud Spanner Database ID. [$SPANNER_DATABASE_ID]
  -e, --execute=    Execute SQL statement and quit.
  -f, --file=       Execute SQL statement from file and quit.
  -t, --table       Display output in table format for batch mode.
  -v, --verbose     Display verbose output.
      --credential= Use the specific credential file
      --prompt=     Set the prompt to the specified format
      --priority=   Set default request priority (HIGH|MEDIUM|LOW)

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

In the following syntax, we use `<>` for a placeholder, `[]` for an optional keyword,
and `{}` for a mutually exclusive keyword.

* The syntax is case-insensitive.
* `\G` delimiter is also supported for displaying results vertically.

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
| Truncate table | `TRUNCATE TABLE <table>;` | Only rows are deleted. Note: Non-atomically because executed as a [partitioned DML statement](https://cloud.google.com/spanner/docs/dml-partitioned?hl=en). |
| Create index | `CREATE INDEX ...;` | |
| Delete index | `DROP INDEX ...;` | |
| Query | `SELECT ...;` | |
| DML | `{INSERT\|UPDATE\|DELETE} ...;` | |
| Partitioned DML | `PARTITIONED {UPDATE\|DELETE} ...;` | |
| Show Query Execution Plan | `EXPLAIN SELECT ...;` | |
| Show DML Execution Plan | `EXPLAIN {INSERT\|UPDATE\|DELETE} ...;` | |
| Show Query Execution Plan with Stats | `EXPLAIN ANALYZE SELECT ...;` | |
| Show DML Execution Plan with Stats | `EXPLAIN ANALYZE {INSERT\|UPDATE\|DELETE} ...;` | |
| Start Read-Write Transaction | `BEGIN [RW] [PRIORITY {HIGH\|MEDIUM\|LOW}];` | See [Request Priority](#request-priority) for details on the priority. |
| Commit Read-Write Transaction | `COMMIT;` | |
| Rollback Read-Write Transaction | `ROLLBACK;` | |
| Start Read-Only Transaction | `BEGIN RO [{<seconds>\|<RFC3339-formatted time>}] [PRIORITY {HIGH\|MEDIUM\|LOW}];` | `<seconds>` and `<RFC3339-formatted time>` is used for stale read. See [Request Priority](#request-priority) for details on the priority. |
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

## Configuration Precedence

1. Command line flags(highest)
2. Environment variables
3. `.spanner_cli.cnf` in current directory
4. `.spanner_cli.cnf` in home directory(lowest)

## Request Priority

You can set [request priority](https://cloud.google.com/spanner/docs/reference/rest/v1/RequestOptions#Priority) for command level or transaction level.
By default `MEDIUM` priority is used for every request.

To set a priority for command line level, you can use `--priority={HIGH|MEDIUM|LOW}` command line option.

To set a priority for transaction level, you can use `PRIORITY {HIGH|MEDIUM|LOW}` keyword.

Here are some examples for transaction-level priority.

```
# Read-write transaction with low priority
BEGIN PRIORITY LOW;

# Read-only transaction with low priority
BEGIN RO PRIORITY LOW;

# Read-only transaction with 60s stale read and medium priority
BEGIN RO 60 PRIORITY MEDIUM;

# Read-only transaction with exact timestamp and medium priority
BEGIN RO 2021-04-01T23:47:44+00:00 PRIORITY MEDIUM;
```

Note that transaction-level priority takes precedence over command-level priority.

## How to develop

Run unit tests.

```
$ make test
```

Run integration tests, which connects to real Cloud Spanner database.

```
$ PROJECT=${PROJECT_ID} INSTANCE=${INSTANCE_ID} DATABASE=${DATABASE_ID} CREDENTIAL=${CREDENTIAL} make test
```

Run integration tests in [CircleCI Local CLI](https://circleci.com/docs/2.0/local-cli/), which connects to [Cloud Spanner Emulator](https://cloud.google.com/spanner/docs/emulator?hl=en).

```
$ circleci local execute
```

## TODO

* Show secondary index by "SHOW CREATE TABLE"

## Disclaimer

Do not use this tool for production databases as the tool is still alpha quality.

Please feel free to report issues and send pull requests, but note that this application is not officially supported as part of the Cloud Spanner product.
