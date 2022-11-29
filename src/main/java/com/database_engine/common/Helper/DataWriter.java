package com.database_engine.common.Helper;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.database_engine.classes.Column;
import com.database_engine.classes.Row;
import com.database_engine.common.Parser.SqlParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

/**
 * DataWriter is a class that utilizes csvWriter functions under the hood to
 * write data to csv files.
 */

public class DataWriter implements IDataWriter {
    final String PATH = "src/main/java/com/database_engine/data";

    @Override
    public boolean createDatabaseDirectory(String databaseName) {
        DataReader dataReader = new DataReader();
        boolean isDatabaseCreated = false;
        if (dataReader.getDatabaseName(databaseName) == null) {
            try {
                File databaseDirectory = new File(PATH + "/" + databaseName);
                databaseDirectory.mkdir();
                FileWriter fileWriter = new FileWriter(PATH + "/" + "databases.csv", true);
                CSVWriter csvWriter = new CSVWriter(fileWriter);
                String[] database = { databaseName };
                csvWriter.writeNext(database);
                File tablesDirectory = new File(PATH + "/" + databaseName + "/" + "Tables.csv");
                isDatabaseCreated = tablesDirectory.createNewFile();
                if (isDatabaseCreated) {
                    fileWriter = new FileWriter(PATH + "/" + databaseName + "/" + "Tables.csv", true);
                    String[] header = { "Name" };
                    csvWriter = new CSVWriter(fileWriter);
                    csvWriter.writeNext(header);
                }
                csvWriter.close();
                fileWriter.close();

            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
        return isDatabaseCreated;
    }

    @Override
    public boolean createTableFile(String databaseName, String tableName, Column column) {
        boolean isTableCreated = false;
        try {
            File tableDirectory = new File(PATH + "/" + databaseName + "/" + tableName + ".csv");
            FileWriter fileWriter = new FileWriter(tableDirectory.getPath(), true);
            CSVWriter csvWriter = new CSVWriter(fileWriter);
            String[] columns = column.names.toArray(new String[column.names.size()]);
            csvWriter.writeNext(columns);
            isTableCreated = tableDirectory.createNewFile() & addToTablesCSV(databaseName, tableName);
            csvWriter.close();
            fileWriter.close();

        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return isTableCreated;
    }

    private boolean addToTablesCSV(String databaseName, String tableName) {
        boolean isAdded = false;
        FileWriter fileWriter;
        try {

            fileWriter = new FileWriter(PATH + "/" + databaseName + "/" + "Tables.csv", true);
            String[] table = { tableName };
            CSVWriter csvWriter = new CSVWriter(fileWriter);
            csvWriter.writeNext(table);
            isAdded = true;
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isAdded;
    }

    private boolean removeFromTablesCSV(String databaseName, String tableName) {
        File file = new File(PATH + "/" + databaseName + "/" + "Tables.csv");
        boolean isRemoved = false;
        try {
            FileWriter fileWriter = new FileWriter(PATH + "/" + databaseName + "/" + "Tables.csv");
            DataReader dataReader = new DataReader();
            Column columns = new Column(null);
            columns.getNames().add("Name");
            List<String> tableNames = dataReader.getTablesName(databaseName);
            String[] newTableNames = new String[tableNames.size() - 1];
            PrintWriter pw = new PrintWriter(file);
            CSVWriter csvWriter = new CSVWriter(pw);
            isRemoved = this.createTableFile(databaseName, tableName, columns);
            for (int name = 0; name < tableNames.size(); name++) {
                if (!tableNames.get(name).equals(tableName)) {
                    newTableNames[name] = tableName;
                }
            }
            csvWriter.writeNext(newTableNames);
            pw.close();
            csvWriter.close();
            fileWriter.close();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return isRemoved;
    }

    private boolean removeFromDatabasesCSV(String databaseName) {
        File file = new File(PATH + "/" + "databases.csv");
        boolean isRemoved = false;
        try {
            FileWriter fileWriter = new FileWriter(PATH + "/" + "databases.csv");
            DataReader dataReader = new DataReader();
            Column columns = new Column(null);
            columns.getNames().add("Name");
            List<String> databasesNames = dataReader.getDatabasesName();
            String[] newdatabasesNames = new String[databasesNames.size() - 1];
            PrintWriter pw = new PrintWriter(file);
            CSVWriter csvWriter = new CSVWriter(pw);
            for (int name = 0; name < databasesNames.size(); name++) {
                if (!databasesNames.get(name).equals(databaseName)) {
                    newdatabasesNames[name] = databaseName;
                }
            }
            csvWriter.writeNext(newdatabasesNames);
            pw.close();
            csvWriter.close();
            fileWriter.close();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return isRemoved;
    }

    @Override
    public boolean insertRow(String databaseName, String query) {
        FileWriter fileWriter;
        boolean isInserted = false;
        try {
            DataReader dataReader = new DataReader();
            SqlParser sqlParser = new SqlParser();
            String tableName = sqlParser.parseInsertTableName(query);
            if (!dataReader.getTablesName(databaseName).contains(tableName)) {
                return false;
            }
            fileWriter = new FileWriter(PATH + "/" + databaseName + "/" + tableName + ".csv", true);
            CSVWriter writer = new CSVWriter(fileWriter);
            Column column = dataReader.getColumnsName(databaseName, tableName);
            List<String> columns = column.getNames();
            List<List<String>> insert_column = sqlParser.parseInsertStatement(query);
            String[] row = new String[columns.size()];
            String columnName;
            String columnValue;
            for (int column_value = 0; column_value < insert_column.get(0).size(); column_value++) {
                columnName = insert_column.get(0).get(column_value);
                columnValue = insert_column.get(1).get(column_value);
                if (columns.contains(columnName)) {
                    row[columns.indexOf(columnName)] = columnValue;
                } else {
                    isInserted = false;
                    break;
                }
            }
            writer.writeNext((String[]) row);
            writer.close();
            isInserted = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isInserted;
    }

    @Override
    public boolean updateRows(String databaseName, String query) {
        boolean isUpdated = false;
        DataReader dataReader = new DataReader();
        List<Row> updatedRows = new ArrayList<Row>();
        SqlParser sqlParser = new SqlParser();
        String columnName;
        String operator;
        String columnValue;
        String logicalOperator;
        boolean is_operand;
        boolean condition;
        try {
            String tableName = sqlParser.parseUpdateTableName(query);
            if (!dataReader.getTablesName(databaseName).contains(tableName)) {
                return false;
            }
            List<List<String>> updateElements = sqlParser.parseUpdateStatement(query);
            List<String> updatedColumnNames = updateElements.get(0);
            List<String> updatedColumnValues = updateElements.get(1);
            FileReader fileReader = new FileReader(PATH + "/" + databaseName + "/" + tableName + ".csv");
            CSVReader csvReader = new CSVReader(fileReader);
            String[] row = csvReader.readNext();
            Column columns = dataReader.getColumnsName(databaseName, tableName);
            while ((row = csvReader.readNext()) != null) {
                System.out.println(Arrays.toString(row));
                is_operand = false;
                condition = false;
                logicalOperator = "";
                // This condition holds to be true in case there is no where condition(bulk
                // update)
                if (updateElements.size() == 2) {
                    for (int cell = 0; cell < updatedColumnNames.size(); cell++) {
                        String updatedColumnName = updatedColumnNames.get(cell);
                        String updatedColumnValue = updatedColumnValues.get(cell);
                        updatedColumnName = updatedColumnName.substring(1, updatedColumnName.length() - 1);
                        updatedColumnValue = updatedColumnValue.substring(1, updatedColumnValue.length() - 1);
                        row[columns.getNames().indexOf(updatedColumnName)] = updatedColumnValue;
                    }
                    updatedRows.add(new Row(row));
                } else {
                    List<String> columnNames = updateElements.get(2);
                    List<String> operators = updateElements.get(3);
                    List<String> columnValues = updateElements.get(4);
                    List<String> logicalOperators = updateElements.get(5);
                    for (int value = 0; value < columnNames.size(); value++) {
                        columnName = columnNames.get(value);
                        operator = operators.get(value);
                        columnValue = columnValues.get(value);
                        if (is_operand & logicalOperators.size() > 0) {
                            logicalOperator = logicalOperators.get(value - 1);
                            if (logicalOperator.equalsIgnoreCase("and")) {
                                logicalOperator = "AND";
                            } else {
                                logicalOperator = "OR";
                            }
                        }
                        if (logicalOperator.equals("AND")) {
                            condition = checkOperatorCondition(operator,
                                    row[columns.getNames().indexOf(columnName)],
                                    columnValue) & condition;
                        } else {
                            condition = checkOperatorCondition(operator,
                                    row[columns.getNames().indexOf(columnName)],
                                    columnValue) | condition;
                        }
                        is_operand = !is_operand;
                    }
                    if (condition) {
                        for (int cell = 0; cell < updatedColumnNames.size(); cell++) {
                            String updatedColumnName = updatedColumnNames.get(cell);
                            String updatedColumnValue = updatedColumnValues.get(cell);
                            updatedColumnName = updatedColumnName.substring(1, updatedColumnName.length() - 1);
                            updatedColumnValue = updatedColumnValue.substring(1, updatedColumnValue.length() - 1);
                            row[columns.getNames().indexOf(updatedColumnName)] = updatedColumnValue;
                        }
                    }
                }
            }
            File file = new File(PATH + "/" + databaseName + "/" + tableName + ".csv");
            FileWriter fileWriter = new FileWriter(PATH + "/" + databaseName + "/" + tableName + ".csv",
                    true);
            PrintWriter pw = new PrintWriter(file);
            CSVWriter csvWriter = new CSVWriter(fileWriter);
            isUpdated = this.createTableFile(databaseName, tableName, columns);
            for (int cell = 0; cell < updatedRows.size(); cell++) {
                csvWriter.writeNext((String[]) updatedRows.get(cell).value);
            }
            pw.close();
            csvWriter.close();
            fileWriter.close();
            csvReader.close();
        } catch (Exception exception) {
        }
        System.out.println(isUpdated);
        return isUpdated;
    }

    @Override
    public boolean deleteRows(String databaseName, String query) {
        boolean isDeleted = false;
        DataReader dataReader = new DataReader();
        SqlParser sqlParser = new SqlParser();
        String tableName = sqlParser.parseDeleteTableName(query);
        if (!dataReader.getTablesName(databaseName).contains(tableName)) {
            return false;
        }
        List<List<String>> deletedElements = sqlParser.parseDeleteStatement(query);
        Column columns = dataReader.getColumnsName(databaseName, tableName);
        List<Row> rows = new ArrayList<Row>();
        try {
            FileWriter fileWriter = new FileWriter(PATH + "/" + databaseName + "/" + tableName + ".csv", true);
            CSVWriter csvWriter = new CSVWriter(fileWriter);
            if (deletedElements.size() != 0) {
                FileReader fileReader = new FileReader(PATH + "/" + databaseName + "/" + tableName + ".csv");
                CSVReader csvReader = new CSVReader(fileReader);
                String columnName;
                String operator;
                String columnValue;
                String logicalOperator;
                boolean is_operand;
                boolean condition;
                String[] row = csvReader.readNext();
                List<String> columnNames = deletedElements.get(0);
                List<String> operators = deletedElements.get(1);
                List<String> columnValues = deletedElements.get(2);
                List<String> logicalOperators = deletedElements.get(3);
                while ((row = csvReader.readNext()) != null) {
                    condition = false;
                    is_operand = false;
                    logicalOperator = "";
                    for (int value = 0; value < columnNames.size(); value++) {
                        columnName = columnNames.get(value);
                        operator = operators.get(value);
                        columnValue = columnValues.get(value);
                        if (is_operand & logicalOperators.size() > 0) {
                            logicalOperator = logicalOperators.get(value - 1);
                            if (logicalOperator.equalsIgnoreCase("and")) {
                                logicalOperator = "AND";
                            } else {
                                logicalOperator = "OR";
                            }
                        }
                        if (logicalOperator.equals("AND")) {
                            condition = checkOperatorCondition(operator,
                                    row[columns.getNames().indexOf(columnName)],
                                    columnValue) & condition;
                        } else {
                            condition = checkOperatorCondition(operator,
                                    row[columns.getNames().indexOf(columnName)],
                                    columnValue) | condition;
                        }
                        is_operand = !is_operand;
                    }
                    if (!condition) {
                        rows.add(new Row(row));
                    }
                }
                csvReader.close();
            }
            File file = new File(PATH + "/" + databaseName + "/" + tableName + ".csv");
            PrintWriter pw = new PrintWriter(file);
            isDeleted = this.createTableFile(databaseName, tableName, columns);
            if (rows.size() > 0) {
                for (int row = 0; row < rows.size(); row++) {
                    csvWriter.writeNext((String[]) rows.get(row).value);
                }
            }
            pw.close();
            csvWriter.close();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isDeleted;
    }

    public boolean dropTableFile(String databseName, String tableName) {
        boolean isDroppedSuccesfully = false;
        DataReader dataReader = new DataReader();
        if (!dataReader.getTablesName(databseName).contains(tableName)) {
            return isDroppedSuccesfully;
        }
        File f = new File(PATH + "/" + databseName + "/" + tableName + ".csv");
        isDroppedSuccesfully = f.delete() & this.removeFromTablesCSV(databseName, tableName);
        return isDroppedSuccesfully;
    }

    public boolean dropDatabaseDirectory(String databaseName) {
        boolean isDroppedSuccesfully = false;
        DataReader dataReader = new DataReader();
        if (dataReader.getDatabaseName(databaseName) == null) {
            return isDroppedSuccesfully;
        }
        File databaseDirectory = new File(PATH + "/" + databaseName);
        String[] entries = databaseDirectory.list();
        for (String s : entries) {
            File currentFile = new File(databaseDirectory.getPath(), s);
            currentFile.delete();
        }
        isDroppedSuccesfully = databaseDirectory.delete() & removeFromDatabasesCSV(databaseName);
        return isDroppedSuccesfully;
    }

    private boolean checkOperatorCondition(String operator, String actualvalue, String searchValue) {
        boolean condition = false;
        if (operator.equals("=")) {
            condition = actualvalue.equals(searchValue);
        } else {
            if (operator.equals("!=")) {
                condition = !(actualvalue.equals(searchValue));
            } else {
                if (operator.equals(">")) {
                    condition = Integer
                            .parseInt(actualvalue) > Integer.parseInt(searchValue);
                } else {
                    if (operator.equals("<")) {
                        condition = Integer
                                .parseInt(actualvalue) < Integer.parseInt(searchValue);
                    } else {
                        if (operator.equals("<=")) {
                            condition = Integer
                                    .parseInt(actualvalue) <= Integer.parseInt(searchValue);
                        } else {
                            if (operator.equals(">=")) {
                                condition = Integer
                                        .parseInt(actualvalue) >= Integer.parseInt(searchValue);
                            }
                        }
                    }
                }
            }
        }
        return condition;
    }
}
