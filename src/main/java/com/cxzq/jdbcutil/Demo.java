package com.azsoftware.jdbc2csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.List;

public class Demo {
    public static void main(String[] args) throws IOException {
        CSVFormat csvFormat = CSVFormat.RFC4180.withFirstRecordAsHeader()
                .withIgnoreEmptyLines(true)
                .withTrim();
        List<String> lines = Files.readAllLines(Paths.get("/tmp/test.json"));
        for(String line: lines) {
            System.out.println(line);
        }
//        try (CSVPrinter printer = new CSVPrinter(System.out, csvFormat)) {
//            printer.printRecord("id", "\"userName\"", "firstName", "lastName", "birthday");
//            printer.printRecord(1, "john73", "John", "Doe", LocalDate.of(1973, 9, 15));
//            printer.println();
//            printer.printRecord(2, "mary", "Mary", "Meyer", LocalDate.of(1985, 3, 29));
//        } catch (IOException ex) {
//            ex.printStackTrace();
//        }
    }
}
