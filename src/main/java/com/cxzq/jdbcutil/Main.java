package com.cxzq.jdbcutil;

import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Main
{

    public static final int EXIT_STATUS_ERR = 1;
    private static CSVFormat csvFormat;

    /**
     * 原始格式输出，用于tuna工具
     *
     * @param resultSet 结果集
     * @throws SQLException SQL 异常
     */
    private static void rawOutput(ResultSet resultSet)
            throws SQLException
    {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (resultSet.getString(i) != null) {
                    System.out.print(resultSet.getString(i)); // NOSONAR
                }
            }
            System.out.println(); // NOSONAR
        }
    }

    /**
     * 删除换行输出，用于风险控制需要的结果集
     *
     * @param resultSet 结果集
     * @throws SQLException SQL 异常
     */
    private static void trimOutput(ResultSet resultSet)
            throws SQLException
    {
        String sep = ",";
        int columnCount = resultSet.getMetaData().getColumnCount();
        StringBuilder sb = new StringBuilder();
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; ++i) {
                Object object = resultSet.getObject(i);
                if (object != null) {
                    String res;
                    if (object instanceof Clob) {
                        res = ((Clob) object).getCharacterStream().toString();
                    }
                    else {
                        res = object.toString();
                    }
                    sb.append(res.replace("\r", "").replace("\n", "").replace(",", ":"));
                }
                if (i < columnCount) {
                    sb.append(sep);
                }
            }
            System.out.println(sb.toString()); // NOSONAR
            sb.setLength(0);
        }
    }

    /**
     * 标签系统需要的输出
     * 特定分隔符，^ 符号以及换行替换
     *
     * @param resultSet 结果集
     * @throws SQLException SQL 异常
     */
    private static void bqOutput(ResultSet resultSet)
            throws SQLException
    {
        String sep = "^"; // 分隔符
        int columnCount = resultSet.getMetaData().getColumnCount();
        StringBuilder sb = new StringBuilder();
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                Object object = resultSet.getObject(i);
                if (object != null) {
                    String res;
                    if (object instanceof Clob) {
                        res = ((Clob) object).getCharacterStream().toString();
                    }
                    else {
                        res = object.toString();
                    }
                    sb.append(res.replace("\r", "").replace("\n", "").replace("^",
                            ""));
                }
                if (i < columnCount) {
                    sb.append(sep);
                }
            }
            System.out.println(sb); // NOSONAR
            sb.setLength(0);
        }
    }

    /**
     * 标准的CSV结果集输出
     *
     * @param resultSet 结果集
     * @throws IOException 文件无法找到异常
     * @throws SQLException SQL查询异常
     */
    private static void stdCsvOutput(ResultSet resultSet)
            throws IOException, SQLException
    {
        CSVPrinter csvPrint = new CSVPrinter(System.out, csvFormat); // NOSONAR
        csvPrint.printRecords(resultSet);
        csvPrint.flush();
        csvPrint.close();
    }

    public static void main(String[] args)
            throws ParseException, IOException
    {
        Cli cli = new Cli(args);
        Properties connectProps = new Properties();
        connectProps.put("user", cli.getUser());
        connectProps.put("password", cli.getPassword());
        try (Connection conn = DriverManager.getConnection(cli.getJdbcUrl(), connectProps);
                Statement stmt = conn.createStatement()) {
            ResultSet resSet;
            if (stmt.execute(cli.getQuery())) {
                resSet = stmt.getResultSet();
                // raw 模式下，不调用CSV格式，直接原始内容输出
                if (cli.isRaw()) {
                    rawOutput(resSet);
                    return ;
                }
                csvFormat = cli.getCsvFormat();
                if (!cli.isHideHeaders()) {
                    csvFormat.withHeader(resSet).print(System.out); // NOSONAR
                }
                if (cli.isBq()) {
                    bqOutput(resSet);
                }
                else if (cli.isTrim()) {
                    trimOutput(resSet);
                }
                else {
                    stdCsvOutput(resSet);
                }
            }
            System.exit(0);
        }
        catch (SQLException ex) {
            System.err.println(ex.getMessage()); // NOSONAR
            System.exit(EXIT_STATUS_ERR);
        }
    }
}
