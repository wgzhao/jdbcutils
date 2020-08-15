package com.cxzq.jdbcutil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class Main {

  public final static int exit_status_err = 1;

  public static void main(String[] args) throws ParseException, IOException {

    Cli cli = new Cli(args);
    try {
      Properties connectProps = new Properties();
      connectProps.put("user", cli.getUser());
      connectProps.put("password", cli.getPassword());
      Connection conn = DriverManager.getConnection(cli.getJdbcUrl(), connectProps);
      Statement stmt = conn.createStatement();
      ResultSet resSet = null;
      if (stmt.execute(cli.getQuery())) {
        resSet = stmt.getResultSet();

        boolean isRaw = cli.isRaw();
        if (isRaw) {
          ResultSetMetaData rsmd = resSet.getMetaData();
          int columnsNumber = rsmd.getColumnCount();
            while (resSet.next()) {
              for(int i=1; i<= columnsNumber; i++) {
                System.out.print(resSet.getString(i));
              }
              System.out.println("");
            }
        } else {
          CSVFormat csvFormat = cli.getCsvFormat();
          if (!cli.isHideHeaders())
            csvFormat.withHeader(resSet).print(System.out);

          CSVPrinter csvPrint = new CSVPrinter(System.out, csvFormat);
          csvPrint.printRecords(resSet);
          csvPrint.flush();
          csvPrint.close();
        }
      }
      conn.close();
      System.exit(0);
    } catch (SQLException ex) {
        System.err.println( ex.getMessage() );
        System.exit(exit_status_err);
    }
  }
}
