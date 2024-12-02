spanner-cli [![run-tests](https://github.com/cloudspannerecosystem/spanner-cli/actions/workflows/run-tests.yaml/badge.svg)](https://github.com/cloudspannerecosystem/spanner-cli/actions/workflows/run-tests.yaml)
===

Interactive command line tool for Cloud Spanner.

![gif](https://github.com/cloudspannerecosystem/spanner-cli/blob/master/image/spanner_cli.gif)

## Description

`spanner-cli` is an interactive command line tool for [Google Cloud Spanner](https://cloud.google.com/spanner/).  
You can control your Spanner databases with idiomatic SQL commands.

## Install

[Install Go](https://go.dev/doc/install) and run the following command.

```
# For Go 1.16+
go install github.com/cloudspannerecosystem/spanner-cli@latest

# For Go <1.16
go get -u github.com/cloudspannerecosystem/spanner-cli
```

Or you can build a docker image.

```
git clone https://github.com/cloudspannerecosystem/spanner-cli.git
cd spanner-cli
docker build -t spanner-cli .
```

## Usage

```
Usage:
  spanner-cli [OPTIONS]

spanner:
  -p, --project=               (required) GCP Project ID. [$SPANNER_PROJECT_ID]
  -i, --instance=              (required) Cloud Spanner Instance ID [$SPANNER_INSTANCE_ID]
  -d, --database=              (required) Cloud Spanner Database ID. [$SPANNER_DATABASE_ID]
  -e, --execute=               Execute SQL statement and quit.
  -f, --file=                  Execute SQL statement from file and quit.
  -t, --table                  Display output in table format for batch mode.
  -v, --verbose                Display verbose output.
      --credential=            Use the specific credential file
      --prompt=                Set the prompt to the specified format
      --history=               Set the history file to the specified path
      --priority=              Set default request priority (HIGH|MEDIUM|LOW)
      --role=                  Use the specific database role
      --endpoint=              Set the Spanner API endpoint (host:port)
      --directed-read=         Directed read option (replica_location:replica_type). The replicat_type is optional and either READ_ONLY or READ_WRITE
      --skip-tls-verify        Insecurely skip TLS verify
      --proto-descriptor-file= Path of a file that contains a protobuf-serialized google.protobuf.FileDescriptorSet message to use in this invocation.
      --version                Show version of spanner-cli

Help Options:
  -h, --help                   Show this help message
```

### Authentication

Unless you specify a credential file with `--credential`, this tool uses [Application Default Credentials](https://cloud.google.com/docs/authentication/production?hl=en#providing_credentials_to_your_application) as credential source to connect to Spanner databases.  

Please make sure to prepare your credential by `gcloud auth application-default login`.

If you're running spanner-cli in docker container on your local machine, you have to pass local credentials to the container with the following command.

```
docker run -it \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/credentials.json \
  -v $HOME/.config/gcloud/application_default_credentials.json:/tmp/credentials.json:ro \
  spanner-cli --help
```

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

### Directed reads mode

spanner-cli now supports directed reads, a feature that allows you to read data from a specific replica of a Spanner database. 
To use directed reads with spanner-cli, you need to specify the `--directed-read` flag.
The `--directed-read` flag takes a single argument, which is the name of the replica that you want to read from.
The replica name can be specified in one of the following formats:

- `<replica_location>`
- `<replica_location>:<replica_type>`

The `<replica_location>` specifies the region where the replica is located such as `us-central1`, `asia-northeast2`.  
The `<replica_type>` specifies the type of the replica either `READ_WRITE` or `READ_ONLY`. 
 
```
$ spanner-cli -p myproject -i myinstance -d mydb --directed-read us-central1

$ spanner-cli -p myproject -i myinstance -d mydb --directed-read us-central1:READ_ONLY

$ spanner-cli -p myproject -i myinstance -d mydb --directed-read asia-northeast2:READ_WRITE
```

Directed reads are only effective for single queries or queries within a read-only transaction.
Please note that directed read options do not apply to queries within a read-write transaction.

> [!NOTE]
> If you specify an incorrect region or type for directed reads, directed reads will not be enabled and [your requsts won't be routed as expected](https://cloud.google.com/spanner/docs/directed-reads#parameters). For example, in a multi-region configuration `nam3`, if you mistype `us-east1` as `us-east-1`, the connection will succeed, but directed reads will not be enabled. 
>
> To perform directed reads to `asia-northeast2` in a multi-region configuration `asia1`, you need to specify `asia-northeast2` or `asia-northeast2:READ_WRITE`.
> Since the replicas placed in `asia-northeast2` are READ_WRITE replicas, directed reads will not be enabled if you specify `asia-northeast2:READ_ONLY`.
> 
> Please refer to [the Spanner documentation](https://cloud.google.com/spanner/docs/instance-configurations#available-configurations-multi-region) to verify the valid configurations.

## Syntax

In the following syntax, we use `<>` for a placeholder, `[]` for an optional keyword,
and `{}` for a mutually exclusive keyword.

* The syntax is case-insensitive.
* `\G` delimiter is also supported for displaying results vertically.

| Usage | Syntax | Note |
| --- | --- | --- |
| List databases | `SHOW DATABASES;` | |
| Switch database | `USE <database> [ROLE <role>];` | The role you set is used for accessing with [fine-grained access control](https://cloud.google.com/spanner/docs/fgac-about). |
| Create database | `CREATE DATABSE <database>;` | |
| Drop database | `DROP DATABASE <database>;` | |
| List tables | `SHOW TABLES [<schema>];` | If schema is not provided, default schema is used |
| Show table schema | `SHOW CREATE TABLE <table>;` | The table can be a FQN.|
| Show columns | `SHOW COLUMNS FROM <table>;` | The table can be a FQN.|
| Show indexes | `SHOW INDEX FROM <table>;` | The table can be a FQN.|
| Create table | `CREATE TABLE ...;` | |
| Change table schema | `ALTER TABLE ...;` | |
| Delete table | `DROP TABLE ...;` | |
| Truncate table | `TRUNCATE TABLE <table>;` | Only rows are deleted. Note: Non-atomically because executed as a [partitioned DML statement](https://cloud.google.com/spanner/docs/dml-partitioned?hl=en). |
| Create index | `CREATE INDEX ...;` | |
| Delete index | `DROP INDEX ...;` | |
| Create role | `CREATE ROLE ...;` | |
| Drop role | `DROP ROLE ...;` | |
| Grant | `GRANT ...;` | |
| Revoke | `REVOKE ...;` | |
| Query | `SELECT ...;` | |
| DML | `{INSERT\|UPDATE\|DELETE} ...;` | |
| Partitioned DML | `PARTITIONED {UPDATE\|DELETE} ...;` | |
| Show Query Execution Plan | `EXPLAIN SELECT ...;` | |
| Show DML Execution Plan | `EXPLAIN {INSERT\|UPDATE\|DELETE} ...;` | |
| Show Query Execution Plan with Stats | `EXPLAIN ANALYZE SELECT ...;` | |
| Show DML Execution Plan with Stats | `EXPLAIN ANALYZE {INSERT\|UPDATE\|DELETE} ...;` | |
| Show Query Result Shape | `DESCRIBE SELECT ...;` | |
| Show DML Result Shape | `DESCRIBE {INSERT\|UPDATE\|DELETE} ... THEN RETURN ...;` | |
| Start a new query optimizer statistics package construction | `ANALYZE;` | |
| Start Read-Write Transaction | `BEGIN [RW] [PRIORITY {HIGH\|MEDIUM\|LOW}] [TAG <tag>];` | See [Request Priority](#request-priority) for details on the priority. The tag you set is used as both transaction tag and request tag. See also [Transaction Tags and Request Tags](#transaction-tags-and-request-tags).|
| Commit Read-Write Transaction | `COMMIT;` | |
| Rollback Read-Write Transaction | `ROLLBACK;` | |
| Start Read-Only Transaction | `BEGIN RO [{<seconds>\|<RFC3339-formatted time>}] [PRIORITY {HIGH\|MEDIUM\|LOW}] [TAG <tag>];` | `<seconds>` and `<RFC3339-formatted time>` is used for stale read. See [Request Priority](#request-priority) for details on the priority. The tag you set is used as request tag. See also [Transaction Tags and Request Tags](#transaction-tags-and-request-tags).|
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

## Transaction Tags and Request Tags

In a read-write transaction, you can add a tag following `BEGIN RW TAG <tag>`.
spanner-cli adds the tag set in `BEGIN RW TAG` as a transaction tag.
The tag will also be used as request tags within the transaction.

```
# Read-write transaction
# transaction_tag = tx1
+--------------------+
| BEGIN RW TAG tx1;  |
|                    |
| SELECT val         |
| FROM tab1      +-----request_tag = tx1
| WHERE id = 1;      |
|                    |
| UPDATE tab1        |
| SET val = 10   +-----request_tag = tx1
| WHERE id = 1;      |
|                    |
| COMMIT;            |
+--------------------+
```

In a read-only transaction, you can add a tag following `BEGIN RO TAG <tag>`.
Since read-only transaction doesn't support transaction tag, spanner-cli adds the tag set in `BEGIN RO TAG` as request tags.
```
# Read-only transaction
# transaction_tag = N/A
+--------------------+
| BEGIN RO TAG tx2;  |
|                    |
| SELECT SUM(val)    |
| FROM tab1      +-----request_tag = tx2
| WHERE id = 1;      |
|                    |
| CLOSE;             |
+--------------------+
```

## Using with the Cloud Spanner Emulator

This tool supports the [Cloud Spanner Emulator](https://cloud.google.com/spanner/docs/emulator) via the [`SPANNER_EMULATOR_HOST` environment variable](https://cloud.google.com/spanner/docs/emulator#client-libraries).

```sh
$ export SPANNER_EMULATOR_HOST=localhost:9010
# Or with gcloud env-init:
$ $(gcloud emulators spanner env-init)

$ spanner-cli -p myproject -i myinstance -d mydb
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

Please feel free to report issues and send pull requests, but note that this application is not officially supported as part of the Cloud Spanner product.
