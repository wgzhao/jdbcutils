package com.azsoftware.jdbc2csv;

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

public class Cli {
  private String jdbcUrl;

  private String query;

  private String user = null;

  private String password = null;

  private CSVFormat csvFormat = null;

  private boolean hideHeaders = false;

  // print directly instead of csv
  private boolean isRaw = true;

  private Options options = new Options();

  private Option optionHelp = new Option("h", "help", false, "show this help, then exit");

  private Option optionHideHeaders = new Option("H", "hide-headers", false, "hide headers on output");

  private Option optionJdbcUrl = Option.builder("U")
      .longOpt("jdbc-url")
      .hasArg()
      .argName("URL STRING")
      .desc("JDBC driver connection URL string")
      .build();

  private Option optionCsvFormat = Option.builder("f")
      .longOpt("csv-format")
      .hasArg()
      .argName("FORMAT")
      .desc("Output CSV format with possibale values: Default, Excel, InformixUnload, InformixUnloadCsv, " +
              "MongoDBCsv, MongoDBTsv, MySQL, Oracle, PostgreSQLCsv, PostgreSQLText, RFC4180 and TDF. " +
              "Default format is \"Default\". For more info visit link " +
              "'https://javadoc.io/doc/org.apache.commons/commons-csv/latest/org/apache/commons/csv/CSVFormat.html'")
      .build();

  private Option optionUser = Option.builder("u")
    .longOpt("user")
    .hasArg()
    .argName("User Name")
    .desc("user to connect")
    .build();

  private Option optionPassword = Option.builder("p")
    .longOpt("password")
    .hasArg()
    .argName("Password")
    .desc("password for user")
    .build(); 

  public Cli(String[] args) throws ParseException {
    options.addOption( optionHelp );
    options.addOption( optionHideHeaders );
    options.addOption( optionJdbcUrl );
    options.addOption( optionCsvFormat );
    options.addOption( optionUser);
    options.addOption( optionPassword);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse( options, args);

    if (cmd.hasOption("h")) {
      usage();
    } else {
      if (cmd.hasOption(optionCsvFormat.getOpt())) {
        isRaw = false;
        csvFormat = CSVFormat.Predefined.valueOf(cmd.getOptionValue(optionCsvFormat.getOpt())).getFormat();
      }

      if (cmd.hasOption(optionHideHeaders.getOpt())) {
        hideHeaders = true;
      }

      if (cmd.hasOption(optionUser.getOpt())) {
        user = cmd.getOptionValue(optionUser.getOpt());
      }else {
        System.err.println( String.format("Option --%s required", optionUser.getLongOpt()) );
        System.exit(Main.exit_status_err);
      }

      if (cmd.hasOption(optionPassword.getOpt())) {
        password = cmd.getOptionValue(optionPassword.getOpt());
      } else {
        System.err.println( String.format("Option --%s required", optionPassword.getLongOpt()) );
        System.exit(Main.exit_status_err);
      }

      if (cmd.hasOption(optionJdbcUrl.getOpt())) {
        jdbcUrl = cmd.getOptionValue(optionJdbcUrl.getOpt());

        if ( cmd.getArgs().length == 0 ) {
          BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
          query = br.lines().collect(Collectors.joining( System.getProperty("line.separator") ));
        } else if ( cmd.getArgs().length == 1 ) {
          query = cmd.getArgs()[0];
        } else {
          System.err.println( "Too many SQL" );
          System.exit(Main.exit_status_err);
        }
      } else {
        System.err.println( String.format("Option --%s required", optionJdbcUrl.getLongOpt()) );
        System.exit(Main.exit_status_err);
      }
    } // end else
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public String getQuery() {
    return query;
  }


  public CSVFormat getCsvFormat() {
    return csvFormat;
  }

  public boolean isHideHeaders() {
    return hideHeaders;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public boolean isRaw() {
    return isRaw;
  }

  public void usage() {
    System.out.println(
      "jdbc2csv execute queries in diferent databases such as mysql, oracle, postgresql and etc.\n"
      + "Query with resultset output over stdout in CSV format.\n");

    // automatically generate the help statement
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "jdbc2csv [OPTION]... SQL", options );
    System.out.println();
  }
}
