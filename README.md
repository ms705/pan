# pan
An interactive Soup shell.

## How to use

Pan accepts ad-hoc queries written using standard SQL syntax.

 * `CREATE TABLE` queries create new base tables;
 * `INSERT` queries are executed and insert records into base tables;
 * `SELECT` queries install new materialized views and return the records selected.
 * `SHOW GRAPH` displays the current Soup data-flow graph in GraphViz dot format.

Pan performs a Soup migration for each new `CREATE TABLE` and `SELECT` query, but
will often reuse existing queries if possible.

Each query must be terminated by a semicolon and a newline.

```
$ cargo run

Welcome to Pan, your interactive Soup shell!

Pan> create table test (a text, b int, c varchar(255), PRIMARY KEY (a));

Pan> insert into test values ("hello", 42, "world");

Pan> select * from test;

Pan> show graph;

[...]
```
