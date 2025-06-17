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

First, create a JSON file `sample.json` , like the following:

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

## hiveSchema subcommand

the `hiveSchema` subcommand allow you to generate a hive schema from a table in a database.

```shell
java -jar jdbcutils-<version>-jar-with-dependencies.jar hiveSchema \
  -U jdbc:mysql://localhost -u username -p password  \
  -d link_scrm  -t qw_customer \
  -D odsc2 -T qw_customer \
  --partition-name=dt --partition-type=int
  --hdfs-path=/ods/odscs2  
```

it will print the hive table DDL SQL, like the following:

```sql
CREATE EXTERNAL TABLE odsc2.qw_customer (
  id STRING,
  qw_customer_id INT,
  external_userid STRING,
  name STRING,
  avatar STRING,
  type STRING,
  gender STRING,
  unionid STRING,
  position STRING,
  corp_name STRING,
  follow_user_userid STRING,
  follow_user_remark STRING,
  follow_user_description STRING,
  follow_user_createtime STRING,
  follow_user_add_way STRING,
  follow_user_state STRING,
  create_time STRING,
  corpid STRING,
  del_follow_user INT,
  del_external_contact INT,
  channel_id INT
)
partitioned by ( dt int)
stored as ORC
location '/ods/odscs2/qw_customer'
```