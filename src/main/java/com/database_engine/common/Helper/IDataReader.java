package com.database_engine.common.Helper;

import java.util.List;
import com.database_engine.classes.Column;
import com.database_engine.classes.Row;;

public interface IDataReader {

    public List<Row> searchforRows(String databaseName, String query);

    public List<Row> searchforAllRows(String databaseName, String tableName);

    public String getDatabaseName(String databaseName);

    public List<String> getDatabasesName();

    public List<String> getTablesName(String databaseName);

    public Column getColumnsName(String databaseName, String tableName);

}
