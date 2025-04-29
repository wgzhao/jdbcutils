package com.wgzhao.jdbcutils;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.Callable;

@Command(
        name = "hive-schema",
        mixinStandardHelpOptions = true,
        version = "1.0",
        description = "Generate Hive table creation SQL from an RDBMS table."
)
public class HiveSchema implements Callable<Integer>
{
    @Option(names = {"-U", "--jdbc-url"}, required = true, description = "JDBC driver connection URL string")
    private String jdbcUrl;

    @Option(names = {"-u", "--user"}, required = true, description = "User to connect")
    private String user;

    @Option(names = {"-p", "--password"}, required = true, description = "Password for user")
    private String password;

    @Option(names = {"-d", "--database"}, required = true, description = "RDBMS database name")
    private String database;

    @Option(names = {"-t", "--table"}, required = true, description = "RDBMS table name")
    private String tableName;

    @Option(names = {"-D", "--hive-database"}, description = "Hive database name. Defaults to the same as the RDBMS database.")
    private String hiveDatabaseOption;

    @Option(names = {"-T", "--hive-table"}, description = "Hive table name. Defaults to the same as the RDBMS table name.")
    private String hiveTable;

    @Option(names = {"-S", "--storage-format"}, description = "Hive storage format. Defaults to 'ORC'.", defaultValue = "ORC")
    private String storage;

    @Option(names = {"--partition-name"}, description = "The partition name of hive table")
    private String partitionName;

    @Option(names = {"--partition-type"}, description = "The data type of partition in hive table. Defaults to 'string'", defaultValue = "string")
    private String partitionType;

    @Option(names = {"--hdfs-path"}, required = true, description = "The hdfs path for the hive table")
    private String hdfsPath;

    @Option(names = {"--addax-column-json"}, required = false, description = "create column definition json for addax", defaultValue="false")
    private boolean createColumnJson;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Display this help and exit")
    boolean usageHelpRequested;

    @Option(names = {"-V", "--version"}, versionHelp = true, description = "Display version info and exit")
    boolean versionInfoRequested;

    @Override
    public Integer call() throws Exception
    {
        Properties properties = new Properties();
        if (user != null) {
            properties.put("user", user);
        }
        if (password != null) {
            properties.put("password", password);
        }

        if (hiveDatabaseOption == null) {
            hiveDatabaseOption = database;
        }
        if (hiveTable == null) {
            hiveTable = tableName; // Default to the same as the RDBMS table name
        }

        try (Connection conn = DriverManager.getConnection(jdbcUrl, properties)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
                StringBuilder createTableSQL = new StringBuilder();
                createTableSQL.append("CREATE TABLE ").append(hiveDatabaseOption).append(".").append(hiveTable).append(" (\n");
                StringBuilder addaxColumn = new StringBuilder();
                boolean firstColumn = true;
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String columnType = columns.getString("TYPE_NAME");
                    int columnSize = columns.getInt("COLUMN_SIZE");
                    String hiveType = convertToHiveType(columnType, columnSize);
                    String column = String.format("{" +
                            "\"name\": \"%s\"," +
                            "\"type\": \"%s\"" +
                            "}", columnName, hiveType);
                    if (firstColumn) {
                        addaxColumn.append(column);
                        firstColumn = false;
                    } else {
                        addaxColumn.append(",").append(column);
                    }
                    createTableSQL.append("  ").append(columnName).append(" ").append(hiveType).append(",\n");
                }

                // Remove the trailing comma and newline
                if (createTableSQL.charAt(createTableSQL.length() - 2) == ',') {
                    createTableSQL.setLength(createTableSQL.length() - 2);
                }
                createTableSQL.append("\n)\n");
                // Add partition if present
                if (partitionName != null && partitionType != null) {
                    createTableSQL.append("partitioned by ( ").append(partitionName).append(" ").append(partitionType).append(")\n");
                }
                // Add storage format
                createTableSQL.append("stored as ").append(storage).append("\n");
                // Add hdfs path
                if (hdfsPath != null) {
                    createTableSQL.append("location '").append(hdfsPath).append("'\n");
                }
                System.out.println(createTableSQL);

                if (createColumnJson) {
                    System.out.println("[" + addaxColumn + "]");
                }
            }
        }
        return 0;
    }

    private String convertToHiveType(String rdbmsType, int size)
    {
        // Basic RDBMS to Hive type mapping
        switch (rdbmsType.toUpperCase()) {
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
                return "STRING";
            case "INT":
            case "INTEGER":
                return "INT";
            case "BIGINT":
                return "BIGINT";
            case "FLOAT":
            case "REAL":
                return "FLOAT";
            case "DOUBLE":
            case "NUMERIC":
            case "DECIMAL":
                return "DOUBLE";
            case "DATE":
                return "DATE";
            case "TIMESTAMP":
                return "TIMESTAMP";
            default:
                return "STRING"; // Default to STRING for unsupported types
        }
    }

    public static void main(String[] args)
    {
        int exitCode = new CommandLine(new HiveSchema()).execute(args);
        System.exit(exitCode);
    }
}
