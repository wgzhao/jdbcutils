package com.cxzq.jdbcutil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class Cli
{
    private String jdbcUrl;

    private String query;

    private String user = null;

    private String password = null;

    private CSVFormat csvFormat = null;

    private boolean hideHeaders = false;

    // print directly instead of csv
    private boolean isRaw = true;

    // trim special symbol
    private boolean isTrim = false;

    // 标签系统需要的格式
    private boolean isBq = false;

    private int fetchSize = 1024;

    private final Options options = new Options();

    private final Option optionJdbcUrl = Option.builder("U")
            .longOpt("jdbc-url")
            .hasArg()
            .argName("URL STRING")
            .desc("JDBC driver connection URL string")
            .build();

    private final Option optionCsvFormat = Option.builder("f")
            .longOpt("csv-format")
            .hasArg()
            .argName("FORMAT")
            .desc("Output CSV format with possibale values: Default, Excel, InformixUnload, InformixUnloadCsv, " +
                    "MongoDBCsv, MongoDBTsv, MySQL, Oracle, PostgreSQLCsv, PostgreSQLText, RFC4180 and TDF. " +
                    "Default format is \"Default\". For more info visit link " +
                    "'https://javadoc.io/doc/org.apache.commons/commons-csv/latest/org/apache/commons/csv/CSVFormat.html'")
            .build();

    private final Option optionUser = Option.builder("u")
            .longOpt("user")
            .hasArg()
            .argName("User Name")
            .desc("user to connect")
            .build();

    private final Option optionPassword = Option.builder("p")
            .longOpt("password")
            .hasArg()
            .argName("Password")
            .desc("password for user")
            .build();

    private final Option optionStrip = new Option("T", "trim", false, "trim new line and comma symbol, " +
            "the \\r|\\n will be trimed  and comma (,) will be replaced by :, ONLY for jhxt");

    private final Option optionBq = new Option("B", "bq", false, "trim new line and symbol ^ ONLY for " +
            "biaoqian");

    private final Option optionFetchSize = new Option("n", "fetch-size", true, "fetch size each " +
            "time");

    public Cli(String[] args)
            throws ParseException
    {
        Option optionHelp = new Option("h", "help", false, "show this help, then exit");
        options.addOption(optionHelp);
        Option optionHideHeaders = new Option("H", "hide-headers", false, "hide headers on output");
        options.addOption(optionHideHeaders);
        options.addOption(optionJdbcUrl);
        options.addOption(optionCsvFormat);
        options.addOption(optionUser);
        options.addOption(optionPassword);
        options.addOption(optionStrip);
        options.addOption(optionBq);
        options.addOption(optionFetchSize);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h")) {
            usage();
            System.exit(1);
        }
        else {
            if (cmd.hasOption(optionCsvFormat.getOpt())) {
                isRaw = false;
                csvFormat = CSVFormat.Predefined.valueOf(cmd.getOptionValue(optionCsvFormat.getOpt())).getFormat();
            }

            if (cmd.hasOption(optionHideHeaders.getOpt())) {
                hideHeaders = true;
            }

            if (cmd.hasOption(optionUser.getOpt())) {
                user = cmd.getOptionValue(optionUser.getOpt());
            }
            else {
                System.err.println(String.format("Option --%s required", optionUser.getLongOpt()));
                System.exit(Main.EXIT_STATUS_ERR);
            }

            if (cmd.hasOption(optionPassword.getOpt())) {
                password = cmd.getOptionValue(optionPassword.getOpt());
            }
            else {
                password = "";
            }

            if (cmd.hasOption(optionJdbcUrl.getOpt())) {
                jdbcUrl = cmd.getOptionValue(optionJdbcUrl.getOpt());

                if (cmd.getArgs().length == 0) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                    query = br.lines().collect(Collectors.joining(System.getProperty("line.separator")));
                }
                else if (cmd.getArgs().length == 1) {
                    query = cmd.getArgs()[0];
                }
                else {
                    System.err.println("Too many SQL");
                    System.exit(Main.EXIT_STATUS_ERR);
                }
            }
            else {
                System.err.println(String.format("Option --%s required", optionJdbcUrl.getLongOpt()));
                System.exit(Main.EXIT_STATUS_ERR);
            }

            if (cmd.hasOption(optionStrip.getOpt())) {
                isTrim = true;
            }
            if (cmd.hasOption(optionBq.getOpt())) {
                isBq = true;
            }
            if (cmd.hasOption(optionFetchSize.getOpt())) {
                fetchSize = Integer.parseInt(cmd.getOptionValue(optionFetchSize.getOpt()));
            }
        } // end else
    }

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    public String getQuery()
    {
        return query;
    }

    public CSVFormat getCsvFormat()
    {
        return csvFormat;
    }

    public boolean isHideHeaders()
    {
        return hideHeaders;
    }

    public String getUser()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
    }

    public boolean isRaw()
    {
        return isRaw;
    }

    public boolean isTrim() { return isTrim; }

    public boolean isBq() {return isBq; }

    public int getFetchSize()
    {
        return fetchSize;
    }

    public void usage()
    {
        System.out.println(
                "jdbc2csv execute queries in diferent databases such as mysql, oracle, postgresql and etc.\n"
                        + "Query with resultset output over stdout in CSV format.\n");

        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("jdbc2csv [OPTION]... SQL", options);
        System.out.println();
    }
}
