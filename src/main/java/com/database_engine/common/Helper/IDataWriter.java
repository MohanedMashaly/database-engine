package com.database_engine.common.Helper;

import com.database_engine.classes.Column;

public interface IDataWriter {
    public boolean createDatabaseDirectory(String databaseName);

    public boolean createTableFile(String databaseName, String tableName, Column column);

    public boolean insertRow(String databaseName, String query);

    public boolean updateRows(String databaseName, String query);

    public boolean deleteRows(String databaseName, String query);

}
