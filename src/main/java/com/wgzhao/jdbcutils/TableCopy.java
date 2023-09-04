package com.wgzhao.jdbcutils;

import cn.hutool.json.JSON;
import picocli.CommandLine;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.Callable;

import static cn.hutool.json.JSONUtil.readJSON;

@CommandLine.Command(
        name = "tableCopy",
        mixinStandardHelpOptions = true,
        version = "jdbcutil 1.0",
        description = "copy data between different databases"
)
public class TableCopy implements Callable<Integer>
{
    private Properties srcConnectProps;
    private Properties destConnectProps;
    private String srcJdbc;
    private String destJdbc;
    private String destTable;
    private String mode;
    private String preSql;
    private String postSql;

    @CommandLine.Parameters(index = "0", description = "json file, here is a example: \n " + "{\n" +
            "    \"src\": {\n" +
            "        \"jdbc\": \"jdbc:oracle:thin:@127.0.0.1:1521/orcl\",\n" +
            "        \"user\": \"oracle\",\n" +
            "        \"password\": \"password\",\n" +
            "        \"sql\": \"select * from mytable\"\n" +
            "    },\n" +
            "    \"dest\": {\n" +
            "        \"jdbc\": \"jdbc:mysql://127.0.0.1:3306/test\",\n" +
            "        \"user\": \"mysql\",\n" +
            "        \"password\": \"password\",\n" +
            "        \"dbtable\": \"tbl\",\n" +
            "        \"mode\": \"overwrite\",\n" +
            "        \"preSql\": \"\",\n" +
            "        \"postSql\": \"\"\n" +
            "    }\n" +
            "}")
    private File jsonFile;

    /**
     * 提取所有的字段名称，并按照逗号拼接
     */
    private static String getColumns(ResultSetMetaData rsmd)
    {
        StringJoiner joiner = new StringJoiner(",");
        try {
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                joiner.add(rsmd.getColumnLabel(i));
            }
            return joiner.toString();
        }
        catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void processConn(JSON json)
    {

        srcConnectProps = new Properties();
        destConnectProps = new Properties();

        srcJdbc = json.getByPath("src.jdbc", String.class);
        destJdbc = json.getByPath("dest.jdbc", String.class);
        destTable = json.getByPath("dest.dbtable", String.class);
        mode = json.getByPath("dest.mode", String.class);
        //presql
        preSql = json.getByPath("dest.presql", String.class);
        postSql = json.getByPath("dest.postsql", String.class);
        destConnectProps.put("user", json.getByPath("dest.user", String.class));
        destConnectProps.put("password", json.getByPath("dest.password", String.class));

        srcConnectProps.put("user", json.getByPath("src.user", String.class));
        srcConnectProps.put("password", json.getByPath("src.password", String.class));
    }

    private void copyRecords(JSON job)
            throws SQLException
    {
        // source database
        System.out.print("Connect source db with: " + srcJdbc);
        if (srcJdbc.contains("clickhouse")) {
            try {
                Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            }
            catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(2);
            }
        }
        Connection srcConn = DriverManager.getConnection(srcJdbc, srcConnectProps);
        System.out.println(" OK");
        Statement srcStmt = srcConn.createStatement();

        // destination database
        System.out.print("Connect destination db with: " + destJdbc);
        System.out.println(" OK");
        Connection destConn = DriverManager.getConnection(destJdbc, destConnectProps);
        destConn.setAutoCommit(false);
        Statement destStmt = destConn.createStatement();


        if ("overwrite".equals(mode)) {
            destStmt.execute("truncate table " + job.getByPath("dest.dbtable", String.class));
            destConn.commit();
        }

        if (preSql != null && ! Objects.requireNonNull(preSql).trim().isEmpty()) {
            System.out.print("execute pre-sql on dest db: " + preSql);
            destStmt.execute(preSql);
            System.out.println(" OK");
        }

        String insertSql = "insert into " + destTable;
        StringJoiner joinerv = new StringJoiner(",");
        StringJoiner joinerc = new StringJoiner(",");

        String query;

        if (!Objects.equals(null, job.getByPath("src.dbtable", String.class)) && !"".equals(job.getByPath("src.dbtable"))) {
            query = "select * from " + job.getByPath("src.dbtable", String.class);
        }
        else {
            query = job.getByPath("src.sql", String.class);
        }

        System.out.print("Retrives source records");
        srcStmt.setFetchSize(256);
        srcStmt.execute(query);
        System.out.println(" OK");
        ResultSet resSet = srcStmt.getResultSet();
        ResultSetMetaData resMd = resSet.getMetaData();

        // 获得目标表的结构
        Statement stmt = destConn.createStatement();
        String destSql = "select " + getColumns(resMd) + " from " + destTable + " where 1=2";
        System.out.printf("query destination table with SQL: %s%n", destSql);
        stmt.execute(destSql);
        ResultSet destSchema = stmt.getResultSet();
        ResultSetMetaData destMd = destSchema.getMetaData();

        int colNum = resMd.getColumnCount();
        for (int i = 1; i <= colNum; i++) {
            joinerc.add(destMd.getColumnName(i));
            joinerv.add("?");
        }

        insertSql = insertSql + "(" + joinerc + ")values(" + joinerv + ")";
        PreparedStatement preparedStmt = destConn.prepareStatement(insertSql);

        int batchSize = 0;
        System.out.print("Begin insert records");
        while (resSet.next()) {
            for (int i = 1; i <= colNum; i++) {
                if ("unknown".equals(resMd.getColumnTypeName(i))) {
                    preparedStmt.setObject(i, resSet.getObject(i), Types.VARCHAR);
                }
                else {
                    preparedStmt.setObject(i, resSet.getObject(i), resMd.getColumnType(i));
                }
            }
            preparedStmt.addBatch();
            batchSize++;
            if (batchSize % 256 == 0) {
                preparedStmt.executeBatch();
                destConn.commit();
                preparedStmt.clearBatch();
            }
        }
        preparedStmt.executeBatch();
        destConn.commit();
        System.out.println(" OK ");
        if (postSql != null && ! Objects.requireNonNull(postSql).trim().isEmpty()) {
            System.out.print("Execute post-sql on dest db: " + postSql);
            destStmt.execute(postSql);
            destConn.commit();
            System.out.println(" OK");
        }
        destConn.close();
        srcConn.close();
    }

    @Override
    public Integer call() throws Exception
    {
        JSON job = readJSON(jsonFile, StandardCharsets.UTF_8);
        TableCopy tc = new TableCopy();
        tc.processConn(job);
        tc.copyRecords(job);
        return 0;
    }
    public static void main(String[] args) {
        int exitCode = new CommandLine(new TableCopy()).execute(args);
        System.exit(exitCode);
    }
}
