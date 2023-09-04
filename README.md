# jdbcutil

A util to connect and manage popular RDBMS

## 1. Help

```shell
$ java -jar jdbcutil-<version>-shaded.jar
Usage: jdbcutil [COMMAND]
A util to connect and manage popular RDBMS
Commands:
  sql        execute queries in different databases such as mysql, oracle,
               postgresql and etc.
             Query with resultSet output over stdout in CSV format.

  tableCopy  copy data between different databases
```

## sql subcommand

the `sql` subcommand allow you to execute queries in different databases such as mysql, oracle, postgresql and etc.
you can put all RDBMS jdbc driver jars in a directory, and use `-Djava.ext.dirs=<your driver path>` to specify the driver path.

```shell
$ java -Djava.ext.dirs=<your jdbc drivers path>  -jar jdbcutil-<version>-shaded.jar sql \
    -f PostgreSQLText \
    -U 'jdbc:postgresql://host:port/dbname' \
    -u postgres \
    -p secretkey \
    'select version()'
```

##  tableCopy subcommand

the `tableCopy` subcommand allow you to copy table from a database to another. 

First, create a json file `sample.json` , like the following:

```json
{
    "src": {
        "jdbc": "jdbc:oracle:thin:@127.0.0.1:1521/orcl",
        "user": "oracle",
        "password": "password",
        "sql": "select * from mytable"
    },
    "dest": {
        "jdbc": "jdbc:mysql://127.0.0.1:3306/test",
        "user": "mysql",
        "password": "password",
        "dbtable": "tbl",
        "mode": "overwrite",
        "preSql": "",
        "postSql": ""
    }
}
```

then, run the following command 

```shell script
java -Djava.ext.dirs=<your jdbc drivers path> -jar jdbcutil-<version>-shaded.jar tableCopy ./sample.json
```