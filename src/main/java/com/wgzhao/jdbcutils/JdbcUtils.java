package com.wgzhao.jdbcutils;


import cn.hutool.core.lang.JarClassLoader;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.File;
import java.util.Objects;


@Command(name = "jdbcutil", subcommands = {Sql.class, TableCopy.class}, description = "A util to connect and manage popular RDBMS")
public class JdbcUtils
{
    public static void main(String[] args)
    {
        CommandLine cmd = new CommandLine(new JdbcUtils());
        if (args.length == 0 ) {
            cmd.usage(System.out);
        } else {
            cmd.execute(args);
        }
    }
}
