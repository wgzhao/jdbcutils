package com.wgzhao.jdbcutils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Callable;

@Command(
        name = "sql",
        mixinStandardHelpOptions = true,
        version = "1.0",
        description = "execute queries in different databases such as mysql, oracle, postgresql and etc.\n"
                + "Query with resultSet output over stdout in CSV format.\n"
)
public class Sql implements Callable<Integer> {
    @Option(names = {"-U", "--jdbc-url"}, required = true, description = "JDBC driver connection URL string")
    private String jdbcUrl;

    @Option(names = {"-u", "--user"}, required = false, description = "user to connect")
    private String user;

    @Option(names = {"-p", "--password"}, arity = "0..1", description = "password for user")
    private String password;

    @Option(names = {"-f", "--csv-format"}, description = "Output CSV format with possible values: Default, Excel, InformixUnload, InformixUnloadCsv, " +
            "MongoDBCsv, MongoDBTsv, MySQL, Oracle, PostgreSQLCsv, PostgreSQLText, RFC4180 and TDF. " +
            "Default format is Default. For more info visit link " +
            "'https://javadoc.io/doc/org.apache.commons/commons-csv/latest/org/apache/commons/csv/CSVFormat.html'", defaultValue = "Default")
    private String csvFormatName;

    @Option(names = {"-H", "--print-headers"}, description = "print headers on output", defaultValue = "false")
    private boolean printHeader;

    // print directly instead of csv
    @Option(names = {"-r", "--raw"}, description = "print directly instead of csv", defaultValue = "false")
    private boolean isRaw;

    // trim special symbol
    @Option(names = {"-T", "--trim"}, description = "trim new line and comma symbol, " +
            "the \\r|\\n will be trimed  and comma (,) will be replaced by :, ONLY for jhxt", defaultValue = "false")
    private boolean isTrim;

    // 标签系统需要的格式
    @Option(names = {"-B", "--bq"}, description = "trim new line and symbol ^ ONLY for " +
            "biaoqian", defaultValue = "false")
    private boolean isBq;

    @Option(names = {"-n", "--fetch-size"}, description = "fetch size each time", defaultValue = "1024")
    private int fetchSize;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help and exit")
    boolean usageHelpRequested;

    @Option(names = {"-V", "--version"}, versionHelp = true, description = "display version info and exit")
    boolean versionInfoRequested;

    @Parameters(index = "0", description = "SQL query")
    private String query;


    private CSVFormat.Builder csvBuilder;

    /**
     * 原始格式输出，用于tuna工具
     *
     * @param resultSet 结果集
     * @throws SQLException SQL 异常
     */
    private void rawOutput(ResultSet resultSet)
            throws SQLException {
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
    private void trimOutput(ResultSet resultSet) throws SQLException {
        char sep = ',';
        int columnCount = resultSet.getMetaData().getColumnCount();
        StringBuilder sb = new StringBuilder();
        if (printHeader) {
            csvBuilder.setHeader(resultSet).setDelimiter(sep);
        }
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; ++i) {
                Object object = resultSet.getObject(i);
                if (object != null) {
                    String res;
                    if (object instanceof Clob) {
                        res = ((Clob) object).getCharacterStream().toString();
                    } else {
                        res = object.toString();
                    }
                    sb.append(res.replace("\r", "").replace("\n", "").replace(",", ":"));
                }
                if (i < columnCount) {
                    sb.append(sep);
                }
            }
            System.out.println(sb);
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
    private void bqOutput(ResultSet resultSet)
            throws SQLException, IOException {
        char sep = '^'; // 分隔符
        int columnCount = resultSet.getMetaData().getColumnCount();
        StringBuilder sb = new StringBuilder();
        if (printHeader) {
            csvBuilder.setHeader(resultSet).setDelimiter(sep);
        }
        // 首先，把需要替换的字段提取出来，后续循环就不再需要逐一判断了
        HashMap<Integer, Boolean> needConvert = new HashMap<>(columnCount);

        for (int i = 1; i <= columnCount; i++) {
            int type = resultSet.getMetaData().getColumnType(i);
            needConvert.put(i, type == Types.VARCHAR || type == Types.CLOB);
        }
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                Object object = resultSet.getObject(i);
                // only varchar and clob need trim
                if (object != null && needConvert.get(i)) {
                    sb.append(object.toString()
                            .replace("\r", "")
                            .replace("\n", "")
                            .replace("^", "")
                    );
                } else {
                    sb.append(object);
                }
                if (i < columnCount) {
                    sb.append(sep);
                }
            }
            System.out.println(sb);
            sb.setLength(0);
        }
    }

    /**
     * 标准的CSV结果集输出
     *
     * @param resultSet 结果集
     * @throws IOException  文件无法找到异常
     * @throws SQLException SQL查询异常
     */
    private void stdCsvOutput(ResultSet resultSet)
            throws IOException, SQLException {
        if (printHeader) {
            csvBuilder.setHeader(resultSet);
        }
        CSVPrinter csvPrint = new CSVPrinter(System.out, csvBuilder.build());
        csvPrint.printRecords(resultSet);
        csvPrint.flush();
        csvPrint.close();
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Sql()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        Properties properties = new Properties();
        if (user != null) {
            properties.put("user", user);
        }
        if (password != null) {
            properties.put("password", password);
        }
        Connection conn = DriverManager.getConnection(jdbcUrl, properties);
        Statement stmt = conn.createStatement();
        ResultSet resSet;
        stmt.setFetchSize(fetchSize);
        csvBuilder = CSVFormat.Builder.create(CSVFormat.Predefined.valueOf(csvFormatName).getFormat());
        if (stmt.execute(query)) {
            resSet = stmt.getResultSet();
            // raw 模式下，不调用CSV格式，直接原始内容输出
            if (isRaw) {
                rawOutput(resSet);
                return 0;
            }
            if (isBq) {
                bqOutput(resSet);
                return 0;
            }
            if (isTrim) {
                trimOutput(resSet);
                return 0;
            } else {
                stdCsvOutput(resSet);
            }
        }
        return 0;
    }
}
