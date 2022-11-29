package com.database_engine.classes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.database_engine.Validation.DatabaseValidator;
import com.database_engine.common.Helper.DataReader;
import com.database_engine.common.Helper.DataWriter;
import com.database_engine.common.ProcessResult.ProcessResult;
import com.database_engine.entities.interfaces.ITable;

/**
 * Table class represents table which stores rows and columns
 */
public class Table implements ITable {
    public String name;
    public String databaseName;
    public String primaryKey;
    public List<Row> rows;
    public Column column;

    public Table(String name, String databaseName, String primaryKey, Column column) {
        this.name = name;
        this.databaseName = databaseName;
        this.primaryKey = primaryKey;
        this.rows = new ArrayList<Row>();
        this.column = column;
        this.saveTableFile(column);
    }

    @Override
    public ProcessResult insertRow(String query) {
        DataWriter dataWriter = new DataWriter();
        ProcessResult processResult = new ProcessResult(false, "", null, "insert row");
        try {
            boolean isInserted = dataWriter.insertRow(this.getDatabaseName(), query);
            processResult.setSucceeded(isInserted);
            processResult.setMessage("row could not be inserted");
            if (isInserted) {
                processResult.setMessage("row inserted successfully");
            }
        } catch (Exception exception) {
            processResult.setMessage(exception.getMessage());
        }
        return processResult;
    }

    @Override
    public ProcessResult deleteRow(String query) {
        DataWriter dataWriter = new DataWriter();
        ProcessResult processResult = new ProcessResult(false, "", null, "delete row");
        try {
            boolean isDeleted = dataWriter.deleteRows(this.getDatabaseName(), query);
            processResult.setSucceeded(isDeleted);
            processResult.setMessage("row could not be deleted");
            if (isDeleted) {
                processResult.setMessage("row(s) deleted successfully");
            }
        } catch (Exception exception) {
            processResult.setMessage(exception.getMessage());
        }
        return processResult;
    }

    @Override
    public ProcessResult updateRow(String query) {
        DataWriter dataWriter = new DataWriter();
        ProcessResult processResult = new ProcessResult(false, "", null, "update row");
        try {
            boolean isUpdated = dataWriter.updateRows(this.getDatabaseName(), query);
            processResult.setSucceeded(isUpdated);
            processResult.setMessage("row could not be updated");
            if (isUpdated) {
                processResult.setMessage("row(s) updated successfully");
            }
        } catch (Exception exception) {
            processResult.setMessage(exception.getMessage());
        }
        return processResult;
    }

    @Override
    public ProcessResult searchRow(String query) {
        DataReader dataReader = new DataReader();
        ProcessResult processResult = new ProcessResult(false, "", null, "update row");
        try {
            List<Row> rows = dataReader.searchforRows(this.getDatabaseName(), query);
            processResult.setMessage("no row(s) returned");
            if (rows.size() > 0) {
                for (int row = 0; row < rows.size(); row++) {
                    System.out.println(Arrays.toString(rows.get(row).value));
                }
                processResult.setMessage("row(s) returned");
                processResult.setData(rows);
            }
        } catch (Exception exception) {
            processResult.setMessage(exception.getMessage());
        }
        return processResult;
    }

    @Override
    public String getPrimaryKey() {
        return this.primaryKey;
    }

    @Override
    public String getTableName() {
        return this.name;
    }

    @Override
    public Column getTableColumns() {
        DataReader dataReader = new DataReader();
        Column columns = dataReader.getColumnsName(this.databaseName, this.name);
        return columns;
    }

    @Override
    public List<Row> getTableRows() {
        DataReader dataReader = new DataReader();
        List<Row> rows = dataReader.searchforRows(this.getDatabaseName(), this.getTableName());
        return rows;
    }

    @Override
    public String getDatabaseName() {
        return this.databaseName;
    }

    @Override
    public void setPrimaryKey(String primayKey) {
        this.primaryKey = primayKey;
    }

    @Override
    public void setTableName(String name) {
        this.name = name;

    }

    @Override
    public void setTableColumns(Column column) {
        this.column = column;

    }

    @Override
    public void setDatabaseName(String name) {
        this.databaseName = name;

    }

    public void saveTableFile(Column column) {
        DataWriter dataWriter = new DataWriter();
        DatabaseValidator databaseValidator = new DatabaseValidator();
        if (!databaseValidator.isTableNameExists(this.getDatabaseName(), this.getTableName())) {
            dataWriter.createTableFile(this.getDatabaseName(), this.getTableName(), column);
        }
    }
}
