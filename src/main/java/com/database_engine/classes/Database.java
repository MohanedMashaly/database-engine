package com.database_engine.classes;

import java.util.List;
import com.database_engine.Validation.DatabaseValidator;
import com.database_engine.common.Helper.DataWriter;
import com.database_engine.common.ProcessResult.ProcessResult;
import com.database_engine.entities.interfaces.IDatabase;

/**
 * Database class represents actual database.
 */
public class Database implements IDatabase {
    public String name;
    public List<String> tableNames;
    public String userName;
    public String password;
    public String port;
    public String host;

    public Database(String name, String userName, String password, String port, String host) {
        this.name = name;
        this.userName = userName;
        this.password = password;
        this.port = port;
        this.host = host;
        this.createDatabaseDirectory(name);
    }

    @Override
    public ProcessResult dropDatabase(String name) {
        DataWriter dataWriter = new DataWriter();
        ProcessResult processResult = new ProcessResult(false, "", null, name);
        try {
            boolean isDropped = dataWriter.dropDatabaseDirectory(name);
            String message = "Database is dropped successfully";
            processResult.setSucceeded(isDropped);
            processResult.setMessage(message);
            processResult.setData(null);
            processResult.setMessage("drop database");
            if (!isDropped) {
                processResult.setMessage("Database is not dropped");
            }
        } catch (Exception exception) {
            processResult.setMessage(exception.getMessage());
        }
        return processResult;
    }

    @Override
    public ProcessResult updateDatabase(String oldDatabaseName, String newDatabaseName) {
        return null;
    }

    @Override
    public ProcessResult createTable(String tableName, Column column) {
        ProcessResult processResult = new ProcessResult(false, null, null, "createTable");
        try {
            Table table = new Table(this.name, tableName, null, column);
            processResult.setMessage("table created successfully");
            processResult.setSucceeded(true);
            processResult.setData(table);
        } catch (Exception exception) {
            processResult.setMessage("table could not be created");
        }
        return processResult;
    }

    @Override
    public ProcessResult dropTable(String name) {
        DataWriter dataWriter = new DataWriter();
        ProcessResult processResult = new ProcessResult(false, "", null, name);
        try {
            boolean isDropped = dataWriter.dropTableFile(this.name, name);
            String message = "table is dropped successfully";
            processResult.setSucceeded(isDropped);
            processResult.setMessage(message);
            processResult.setData(null);
            processResult.setMessage("drop table");
            if (!isDropped) {
                processResult.setMessage("table is not dropped");
            }
        } catch (Exception exception) {
            processResult.setMessage(exception.getMessage());
        }
        return processResult;
    }

    private void createDatabaseDirectory(String name) {
        DataWriter dataWriter = new DataWriter();
        DatabaseValidator databaseValidator = new DatabaseValidator();
        if (databaseValidator.isDatabaseNameValid(name)) {
            if (databaseValidator.isDatabaseNameValid(name)) {
                dataWriter.createDatabaseDirectory(name);
            }
        }
    }
}
