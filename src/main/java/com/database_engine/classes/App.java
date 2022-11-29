package com.database_engine.classes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.database_engine.common.Helper.DataReader;
import com.database_engine.common.Helper.DataWriter;

public class App {
    public static void main(String[] args) {
        // Database database = new Database(null, null, null, null, null)
        Database database3 = new Database("dev2", null, null, null, null);
        // List<String> columnNames = new ArrayList<String>();
        // columnNames.add("Name");
        // columnNames.add("Age");
        // columnNames.add("Gender");
        // columnNames.add("Points");
        // Column column = new Column(columnNames);
        // Table table = new Table("Players", "prod", null, column);
        // table.insertRow("insert into Players (Name, Age,Gender,Points)Values(Mohamed,
        // 24, Male, 2200)");
        // table.deleteRow("Delete from Players where Age > 29");
        // table.updateRow("Update Players set Points = 222;");
        // table.searchRow("select Name from Players where Age > 20");
        // DataWriter dataWriter = new DataWriter();
        // DataWriter dataReader = new DataWriter();
        // dataWriter.updateRows("dev2", "update Players set Points = 340 where Age >
        // 2");
        // System.out.println(table.deleteRow("Delete From Players").isSucceeded());
        // table.deleteRow("Delete from players where Age > 29");
        // DataWriter dataWriter = new DataWriter();
        // dataWriter.dropDatabaseDirectory("production");
        // DataReader dataReader = new DataReader();
        // System.out.println(Arrays.toString(dataReader.getDatabasesName().toArray()));
        // List<Row> rows = table.searchRow("select Name from players where Age = 30 or
        // Points = 0");
        // table.insertRow("insert into Players (Name, Age, Gender,
        // Points)Values(Mohamed, 24, Male, 2200)");
        // table.insertRow("insert into Players (Name, Age, Gender, Points)
        // Values(Amr,30, Male, 4000)");
        // table.deleteRow("Delete from Players where Name < 4000");
        // for (int row = 0; row < rows.size(); row++) {
        // System.out.println(Arrays.toString(rows.get(row).value));
        // }
    }
}
