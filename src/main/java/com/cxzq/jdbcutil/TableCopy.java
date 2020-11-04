package com.cxzq.jdbcutil;

import cn.hutool.json.JSON;

import java.io.File;
import java.nio.charset.Charset;
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

import static cn.hutool.json.JSONUtil.readJSON;

public class TableCopy
{
    private Properties srcConnectProps;
    private Properties destConnectProps;
    private String srcJdbc;
    private String destJdbc;
    private String destTable;
    private String mode;

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
        destConnectProps.put("user", json.getByPath("dest.user", String.class));
        destConnectProps.put("password", json.getByPath("dest.password", String.class));

        srcConnectProps.put("user", json.getByPath("src.user", String.class));
        srcConnectProps.put("password", json.getByPath("src.password", String.class));
    }

    private void copyRecords(JSON job) throws SQLException
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
        Statement destStmt = destConn.createStatement();

        if ("overwrite".equals(mode)) {
            destStmt.execute("truncate table " + job.getByPath("dest.dbtable", String.class));
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

        insertSql = insertSql + "(" + joinerc.toString() + ")values(" + joinerv.toString() + ")";
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
        destConn.close();
        srcConn.close();

    }

    public static void main(String[] args)
    {
        if (args.length < 1) {
            System.out.println("json file missing");
            System.exit(1);
        }
        Charset charset = StandardCharsets.UTF_8;
        JSON job = readJSON(new File(args[0]), charset);
        TableCopy tc = new TableCopy();
        tc.processConn(job);
        try {
            tc.copyRecords(job);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
