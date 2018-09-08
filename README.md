spanner-cli
===

```
spanner-cli -i $INSTANCE
```

```
-i: instance name
-p: project name (optional)
-d: database name (optional)
--service-account: service account (optional)
```

```
SHOW DATABASES;
USE test;

SHOW TABLES:
SHOW CREATE TABLE test;
```

TODO
---
* create table xxx
* show create table xxx (desc)
* show tables
* readline
* DML
* explain
* prompt customize
* progress mark

Memo
---
* show create table xxx => GetDatabaseDdl
* create table / drop table / alter table / create index => UpdateDatabaseDdl
* show tables => select * from information_schema?
* show databases => ListDatabases
