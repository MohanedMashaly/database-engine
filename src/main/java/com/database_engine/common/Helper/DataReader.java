package com.database_engine.common.Helper;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import com.database_engine.classes.Column;
import com.database_engine.classes.Row;
import com.database_engine.common.Parser.SqlParser;
import com.opencsv.CSVReader;

/**
 * DataReader is a class that utilizes csvReader functions under the hood to
 * read data from csv files.
 */

public class DataReader implements IDataReader {
    final String PATH = "src/main/java/com/database_engine/data";

    @Override
    public List<String> getDatabasesName() {
        List<String> databasesNames = new ArrayList<String>();
        try {
            FileReader fileReader = new FileReader(PATH + "/databases.csv");
            CSVReader csvReader = new CSVReader(fileReader);
            String[] database = null;
            database = csvReader.readNext();
            while ((database = csvReader.readNext()) != null) {
                databasesNames.add(database[0]);
            }
            csvReader.close();
            fileReader.close();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return databasesNames;
    }

    @Override
    public String getDatabaseName(String name) {
        String databaseName = null;
        try {
            FileReader fileReader = new FileReader(PATH + "/databases.csv");
            CSVReader csvReader = new CSVReader(fileReader);
            String[] database = null;
            database = csvReader.readNext();
            while ((database = csvReader.readNext()) != null) {
                if (database[0].equals(name)) {
                    databaseName = name;
                    break;
                }
            }
            csvReader.close();
            fileReader.close();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return databaseName;
    }

    @Override
    public List<String> getTablesName(String databaseName) {
        List<String> tablesNames = new ArrayList<>();
        try {
            FileReader fileReader = new FileReader(PATH + "/" + databaseName + "/" + "Tables.csv");
            CSVReader csvReader = new CSVReader(fileReader);
            String[] table = null;
            table = csvReader.readNext();
            while ((table = csvReader.readNext()) != null) {
                tablesNames.add(table[0]);
            }
            csvReader.close();
        } catch (Exception exception) {
            exception.getStackTrace();
        }
        return tablesNames;
    }

    @Override
    public Column getColumnsName(String databaseName, String tableName) {
        List<String> columnsNames = new ArrayList<>();
        try {
            FileReader fileReader = new FileReader(PATH + "/" + databaseName + "/" + tableName + ".csv");
            CSVReader csvReader = new CSVReader(fileReader);
            String[] column = null;
            column = csvReader.readNext();
            for (int header = 0; header < column.length; header++) {
                columnsNames.add(column[header]);
            }
            csvReader.close();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        Column column = new Column(columnsNames);
        return column;
    }

    @Override
    public List<Row> searchforRows(String databaseName, String query) {
        List<Row> rows = new ArrayList<>();
        SqlParser sqlParser = new SqlParser();
        try {
            String tableName = sqlParser.parseSelectTableName(query);
            if (!this.getTablesName(databaseName).contains(tableName)) {
                return rows;
            }
            FileReader fileReader = new FileReader(PATH + "/" + databaseName + "/" + tableName + ".csv");
            CSVReader csvReader = new CSVReader(fileReader);
            String[] row = csvReader.readNext();
            List<List<String>> selectElements = sqlParser.parseSelectStatement(query);
            List<String> column_names = sqlParser.parseSelectColumnName(query);
            List<String> columns_names_csv = this.getColumnsName(databaseName, tableName).getNames();
            boolean is_column_names_valid = isColumnNamesValid(columns_names_csv, column_names);
            if (selectElements == null & is_column_names_valid) {
                rows = this.searchforAllRows(databaseName, tableName);
            } else {
                if (!is_column_names_valid) {
                    csvReader.close();
                    return null;
                } else {
                    String columnName;
                    String operator;
                    String columnValue;
                    String logicalOperator;
                    boolean is_operand;
                    boolean condition;
                    List<String> columnNames = selectElements.get(0);
                    List<String> operators = selectElements.get(1);
                    List<String> columnValues = selectElements.get(2);
                    List<String> logicalOperators = selectElements.get(3);
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
                                        row[columns_names_csv.indexOf(columnName)],
                                        columnValue) & condition;
                            } else {
                                condition = checkOperatorCondition(operator,
                                        row[columns_names_csv.indexOf(columnName)],
                                        columnValue) | condition;
                            }
                            is_operand = !is_operand;
                        }
                        if (condition) {
                            rows.add(new Row(row));
                        }
                    }
                }
            }
            csvReader.close();
        } catch (

        Exception exception) {
            exception.printStackTrace();
        }
        return rows;
    }

    @Override
    public List<Row> searchforAllRows(String databaseName, String tableName) {
        List<Row> rows = new ArrayList<>();
        try {
            FileReader fileReader = new FileReader(PATH + "/" + databaseName + "/" + tableName + ".csv");
            CSVReader csvReader = new CSVReader(fileReader);
            csvReader.readNext();
            List<String[]> csvRows = csvReader.readAll();
            for (int row = 0; row < csvRows.size(); row++) {
                rows.add(new Row(csvRows.get(row)));
            }
            csvReader.close();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return rows;
    }

    public boolean isColumnNamesValid(List<String> columns_names_csv, List<String> column_names) {
        for (int column = 0; column < column_names.size(); column++) {
            if (!columns_names_csv.contains(column_names.get(column))) {
                return false;
            }
        }
        return true;
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