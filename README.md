# jdbcutil.jar

## 1. Help

```shell
$ java -jar jdbcutil-1.0.0.jar
jdbcutil execute queries in diferent databases such as mysql, oracle, postgresql and etc.
Query with resultset output over stdout in CSV format.

usage: jdbcutil [OPTION]... SQL
 -f,--csv-format <FORMAT>     Output CSV format with possibale values:
                              Default, Excel, InformixUnload,
                              InformixUnloadCsv, MongoDBCsv, MongoDBTsv,
                              MySQL, Oracle, PostgreSQLCsv,
                              PostgreSQLText, RFC4180 and TDF. Default
                              format is "Default".
 -h,--help                    show this help, then exit
 -H,--hide-headers            hide headers on output
 -u,--jdbc-url <URL STRING>   JDBC driver connection URL string

```

## 2. Postgresql connection example (postgresql-9.3-1102-jdbc4.jar required)

```shell
$ java -cp postgresql-9.3-1102-jdbc4.jar:jdbcutil-1.0.0.jar com.cxzq.jdbcutil.Main \
    -f PostgreSQLText \
    -u 'jdbc:postgresql://host:port/dbname?user=postgres&password=secretkey' \
    'select version()'

```


## 3. Oracle connection example (ojdbc8-12.2.0.1.jar required)

```shell
$ java -cp ojdbc8-12.2.0.1.jar:jdbcutil-1.0.0.jar com.cxzq.jdbcutil.Main \
    -u 'jdbc:oracle:thin:<user>/<password>@host:port:dbname' \
    'select * from V$VERSION'

```


## 4. mysql connection example (mysql-connector-java-8.0.18.jar required)

```shell
$ java -cp mysql-connector-java-8.0.18.jar:jdbcutil-1.0.0.jar com.cxzq.jdbcutil.Main \
    -u 'jdbc:mysql://user:password@host:port/dbname' \
    'SHOW VARIABLES LIKE "%version%"'

```

## 5. Copy table from a database to another database 

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
        "mode": "overwrite"
    }
}
```
then, run the following command 

```shell script
java -cp ojdbc8-12.2.0.1.jar:ojdbc8-12.2.0.1.jar:jdbcutil-1.0.0.jar \
  com.cxzq.jdbcutil.TableCopy \
  ./sample.json
```