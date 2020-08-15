package com.cxzq.jdbcutil;

import cn.hutool.json.JSON;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;

import static cn.hutool.json.JSONUtil.readJSON;

public class TableCopy {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("json file missing");
            System.exit(1);
        }
        Charset charset = StandardCharsets.UTF_8;
        JSON job = readJSON(new File(args[0]), charset);

        Properties srcConnectProps = new Properties();
        Properties destConnectProps = new Properties();

        String srcJdbc = job.getByPath("src.jdbc", String.class);
        String destJdbc = job.getByPath("dest.jdbc", String.class);

        destConnectProps.put("user", job.getByPath("dest.user", String.class));
        destConnectProps.put("password", job.getByPath("dest.password", String.class));

        srcConnectProps.put("user", job.getByPath("src.user", String.class));
        srcConnectProps.put("password", job.getByPath("src.password", String.class));

        try {
            // source database
            Connection srcConn = DriverManager.getConnection(srcJdbc, srcConnectProps);
            Statement srcStmt = srcConn.createStatement();

            // destination database
            Connection destConn = DriverManager.getConnection(destJdbc, destConnectProps);
            Statement destStmt = destConn.createStatement();

            if ("overwrite".equals(job.getByPath("dest.mode", String.class))) {
                destStmt.execute("truncate table " + job.getByPath("dest.dbtable", String.class));
            }

            String insertSql = "insert into " + job.getByPath("dest.dbtable", String.class) + " values(";
            StringJoiner joiner = new StringJoiner(",");

            String query;

            if (!Objects.equals(null, job.getByPath("src.dbtable", String.class)) & !"".equals(job.getByPath("src.dbtable"))) {
                query = "select * from " + job.getByPath("src.dbtable", String.class);
            } else {
                query = job.getByPath("src.sql", String.class);
            }

            srcStmt.execute(query);
            ResultSet resSet = srcStmt.getResultSet();
            ResultSetMetaData md = resSet.getMetaData();
            int colNum = md.getColumnCount();
            for (int i = 0; i < colNum; i++) {
                joiner.add("?");
            }

            insertSql = insertSql + joiner.toString() + ")";

            PreparedStatement preparedStmt = destConn.prepareStatement(insertSql);
            int batchSize = 0;
            while (resSet.next()) {
                for (int i = 1; i <= colNum; i++) {
                    preparedStmt.setObject(i, resSet.getObject(i));
                }
                preparedStmt.addBatch();
                batchSize++;
                if (batchSize % 1024 == 0) {
                    preparedStmt.executeBatch();
                    destConn.commit();
                }
            }
            preparedStmt.executeBatch();
            destConn.commit();
            destConn.close();
            srcConn.close();

        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
            System.exit(1);
        }

    }
}
